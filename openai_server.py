#!/usr/bin/env python3
"""
Server unificado y ligero de Byobot enfocado exclusivamente en la API Realtime de OpenAI.
Combina la lógica de configuración, servidor WebSocket, parsing binario y streaming pautado.
"""

import asyncio
import base64
import json
import logging
import os
import struct
import sys
import time
import argparse
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Dict, Any, Optional

import websockets
from websockets.server import WebSocketServerProtocol
from websockets.http import Headers
from websockets import Response
import jwt
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError

# Configuración de Logging
logger = logging.getLogger("byobot_openai")

# Constantes del Protocolo Binario Compacto (16 bytes de cabecera)
FLAG_LAST_FRAME_COMPACT = 0x0001
FLAG_EXTENSION = 0x0002
SOURCE_MAP = {"none": 0, "tx": 1, "rx": 2}
SOURCE_REVERSE_MAP = {0: "none", 1: "tx", 2: "rx"}

# =====================================================================
# FUNCIONES DE UTILIDAD PROTOCOLO BINARIO
# =====================================================================

def parse_compact_binary_frame(frame_data: bytes) -> Optional[Dict[str, Any]]:
    """Parsea una trama binaria compacta entrante de 16 bytes."""
    if len(frame_data) < 16:
        return None
    try:
        flags = struct.unpack('>H', frame_data[0:2])[0]
        bid = frame_data[2]
        source_enum = frame_data[3]
        source = SOURCE_REVERSE_MAP.get(source_enum, "none")
        sequence_num = struct.unpack('>I', frame_data[4:8])[0]
        timestamp_micros = struct.unpack('>Q', frame_data[8:16])[0]
        
        offset = 16
        if (flags & FLAG_EXTENSION) != 0:
            if len(frame_data) < offset + 4:
                return None
            ext_len = struct.unpack('>I', frame_data[offset:offset+4])[0]
            offset += 4 + ext_len

        return {
            'bid': bid,
            'source': source,
            'sequenceNum': sequence_num,
            'timestamp': timestamp_micros,
            'flags': flags,
            'payload': frame_data[offset:]
        }
    except Exception as e:
        logger.error(f"Error parseando trama binaria: {e}")
        return None

def build_compact_binary_frame(bid: int, source: str, sequence_num: int, timestamp_micros: int, 
                                flags: int, media_data: bytes) -> bytes:
    """Construye una trama binaria compacta saliente de 16 bytes."""
    source_enum = SOURCE_MAP.get(source, 0)
    header = struct.pack('>H', flags)
    header += bytes([bid, source_enum])
    header += struct.pack('>I', sequence_num)
    header += struct.pack('>Q', timestamp_micros)
    return header + media_data

# =====================================================================
# CLASE PRINCIPAL DEL SERVIDOR
# =====================================================================

class OpenAIBotServer:
    def __init__(self, host: str, port: int, enable_auth: bool, jwt_secret: str):
        self.host = host
        self.port = port
        self.auth_enabled = enable_auth
        self.jwt_secret_key = jwt_secret
        self.openai_api_key = os.environ.get("OPENAI_API_KEY")
        
        # Estado de conexiones y sesiones activas
        self.connections: Dict[str, WebSocketServerProtocol] = {}
        self.sequence_numbers: Dict[str, int] = {}
        
        # Datos mapeados por sessionId
        self.sessions: Dict[str, Dict[str, Any]] = {}

        if not self.openai_api_key:
            logger.warning("¡ATENCIÓN! La variable de entorno OPENAI_API_KEY no está configurada.")

    def get_next_sequence(self, client_id: str) -> int:
        self.sequence_numbers[client_id] = self.sequence_numbers.get(client_id, 0) + 1
        return self.sequence_numbers[client_id]

    async def send_session_error(self, websocket: WebSocketServerProtocol, client_id: str, 
                                 session_id: str, code: int, reason: str, description: str):
        """Envía un mensaje estructurado de error al cliente."""
        response = {
            "version": "1.0.0",
            "type": "session.error",
            "sessionId": session_id,
            "sequenceNum": self.get_next_sequence(client_id),
            "timestamp": datetime.now(UTC).isoformat(),
            "payload": {
                "status": {"code": code, "reason": reason, "description": description}
            }
        }
        logger.warning(f"[{client_id}] OUTBOUND Error enviado: {reason} - {description}")
        await websocket.send(json.dumps(response))

    # =====================================================================
    # CONTROLADOR DE CONEXIONES Y ENRUTAMIENTO
    # =====================================================================
    
    async def handle_connection(self, websocket: WebSocketServerProtocol):
        """Manejador principal de la conexión WebSocket del cliente."""
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        self.connections[client_id] = websocket
        logger.info(f"Nueva conexión establecida desde {client_id}")
        
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    await self._process_binary_frame(client_id, message)
                elif isinstance(message, str):
                    await self._process_json_message(websocket, client_id, message)
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Conexión cerrada por el cliente: {client_id}")
        finally:
            await self._cleanup_client_sessions(client_id)

    async def _process_json_message(self, websocket: WebSocketServerProtocol, client_id: str, message: str):
        """Procesa mensajes de texto JSON."""
        try:
            data = json.loads(message)
            msg_type = data.get("type", "").strip()
            session_id = data.get("sessionId", "unknown")
            
            logger.info(f"[{client_id}] INBOUND JSON ({msg_type})")

            if msg_type == "session.start":
                await self._handle_session_start(websocket, client_id, data)
            elif msg_type == "bot.start":
                await self._handle_bot_start(websocket, client_id, data)
            elif msg_type == "media":
                await self._handle_media_json(client_id, data)
            elif msg_type in ("bot.end", "session.end", "session.stop"):
                await self._handle_session_end(websocket, client_id, session_id)
            elif msg_type == "session.ping":
                pong = {
                    "version": "1.0.0", "type": "session.pong", "sessionId": session_id,
                    "sequenceNum": self.get_next_sequence(client_id), "timestamp": datetime.now(UTC).isoformat()
                }
                await websocket.send(json.dumps(pong))
        except json.JSONDecodeError:
            logger.error(f"[{client_id}] JSON inválido recibido en el canal de texto.")
        except Exception as e:
            logger.error(f"[{client_id}] Error procesando mensaje JSON: {e}", exc_info=True)

    async def _process_binary_frame(self, client_id: str, message: bytes):
        """Procesa tramas de audio binarias crudas desde el cliente."""
        parsed = parse_compact_binary_frame(message)
        if not parsed:
            return
        
        # Buscar la sesión asociada a este cliente
        session_id = next((sid for sid, s in self.sessions.items() if s["client_id"] == client_id), None)
        if not session_id or not self.sessions[session_id].get("openai_ws"):
            return
            
        await self._forward_audio_to_openai(session_id, parsed['payload'])

    # =====================================================================
    # LÓGICA DE NEGOCIACIÓN DE SESIÓN Y CONEXIÓN A OPENAI
    # =====================================================================

    async def _handle_session_start(self, websocket: WebSocketServerProtocol, client_id: str, data: Dict[str, Any]):
        session_id = data.get("sessionId", "unknown")
        payload = data.get("payload", {})
        
        # Guardar configuración básica de códec solicitada por el cliente
        media_transports = payload.get("mediaTransports", [{}])
        media_codecs = media_transports[0].get("mediaCodecs", [["audio", "L16", 8000, 1]])
        selected_codec = media_codecs[0] # Usar la primera opción disponible (Ej: L16, 8000Hz)
        
        # Registrar sesión interna en el servidor
        self.sessions[session_id] = {
            "client_id": client_id,
            "websocket": websocket,
            "codec_name": selected_codec[1],
            "sample_rate": selected_codec[2],
            "openai_ws": None,
            "openai_task": None,
            "outbound_queue": asyncio.Queue(),
            "pacer_task": None,
            "ingress_bid": 1, # ID de flujo asignado para enviar audio al cliente
            "outbound_seq": 0
        }

        # Responder session.started
        response = {
            "version": "1.0.0", "type": "session.started", "sessionId": session_id,
            "sequenceNum": self.get_next_sequence(client_id), "timestamp": datetime.now(UTC).isoformat(),
            "payload": {
                "mediaTransport": {
                    "type": "avaya-wss",
                    "transportEncoding": "binary" if "binary" in media_transports[0].get("transportEncodings", []) else "base64",
                    "mediaCodecs": [selected_codec]
                },
                "services": ["bot"]
            }
        }
        logger.info(f"[{client_id}] OUTBOUND JSON (session.started) negociado a {selected_codec[1]} {selected_codec[2]}Hz")
        await websocket.send(json.dumps(response))

    async def _handle_bot_start(self, websocket: WebSocketServerProtocol, client_id: str, data: Dict[str, Any]):
        session_id = data.get("sessionId", "unknown")
        payload = data.get("payload", {})
        endpoint_id = payload.get("endpointId", "")
        
        if session_id not in self.sessions:
            await self.send_session_error(websocket, client_id, session_id, 404, "SESSION_NOT_FOUND", "Sesión no inicializada")
            return

        if not self.openai_api_key:
            await self.send_session_error(websocket, client_id, session_id, 500, "MISSING_API_KEY", "OPENAI_API_KEY no configurada en entorno")
            return

        logger.info(f"[{client_id}] Conectando sesión {session_id} al API Realtime de OpenAI...")
        
        url = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"
        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "OpenAI-Beta": "realtime=v1"
        }

        try:
            # Conexión Cliente WebSocket hacia OpenAI
            openai_ws = await websockets.connect(url, additional_headers=headers)
            
            # Envío de configuración de audio a OpenAI (Por defecto espera PCM 24kHz, adaptaremos en el Pacer si es necesario)
            setup_event = {
                "type": "session.update",
                "session": {
                    "turn_detection": {"type": "server_vad"}, # VAD activado en OpenAI para Barge-In automático
                    "input_audio_format": "pcm16",
                    "output_audio_format": "pcm16"
                }
            }
            await openai_ws.send(json.dumps(setup_event))
            
            # Guardar sockets y lanzar tareas de procesamiento asíncronas
            self.sessions[session_id]["openai_ws"] = openai_ws
            self.sessions[session_id]["openai_task"] = asyncio.create_task(
                self._listen_to_openai_loop(session_id, openai_ws, websocket, client_id, endpoint_id)
            )
            self.sessions[session_id]["pacer_task"] = asyncio.create_task(
                self._realtime_audio_pacer_loop(session_id, websocket)
            )

            # Confirmar al cliente que el bot está listo
            started_msg = {
                "version": "1.0.0", "type": "bot.started", "sessionId": session_id,
                "sequenceNum": self.get_next_sequence(client_id), "timestamp": datetime.now(UTC).isoformat(),
                "payload": {"endpointId": endpoint_id}
            }
            await websocket.send(json.dumps(started_msg))
            logger.info(f"[{client_id}] ¡Bot de OpenAI conectado y listo para la sesión {session_id}!")

        except Exception as e:
            logger.error(f"[{client_id}] Fallo crítico al conectar con OpenAI: {e}", exc_info=True)
            await self.send_session_error(websocket, client_id, session_id, 502, "OPENAI_CONNECTION_FAILED", str(e))

    async def _handle_media_json(self, client_id: str, data: Dict[str, Any]):
        """Procesa audio recibido en formato JSON Base64."""
        audio_base64 = data.get("audio", "")
        if not audio_base64:
            return
        
        session_id = next((sid for sid, s in self.sessions.items() if s["client_id"] == client_id), None)
        if not session_id or not self.sessions[session_id].get("openai_ws"):
            return

        audio_bytes = base64.b64decode(audio_base64)
        await self._forward_audio_to_openai(session_id, audio_bytes)

    async def _forward_audio_to_openai(self, session_id: str, audio_bytes: bytes):
        """Codifica y envía bloques de audio hacia OpenAI."""
        openai_ws = self.sessions[session_id]["openai_ws"]
        if openai_ws and not openai_ws.closed:
            try:
                base64_data = base64.b64encode(audio_bytes).decode('utf-8')
                append_event = {
                    "type": "input_audio_buffer.append",
                    "audio": base64_data
                }
                await openai_ws.send(json.dumps(append_event))
            except Exception as e:
                logger.error(f"Error reenviando audio a OpenAI: {e}")

    # =====================================================================
    # ESCUCHA DE OPENAI Y STREAMING PACED (PACER)
    # =====================================================================

    async def _listen_to_openai_loop(self, session_id: str, openai_ws, client_ws, client_id: str, endpoint_id: str):
        """Escucha las respuestas provenientes de OpenAI de manera reactiva."""
        try:
            async for message in openai_ws:
                event = json.loads(message)
                event_type = event.get("type")

                if event_type == "response.audio.delta":
                    delta_b64 = event.get("delta", "")
                    if delta_b64:
                        raw_pcm = base64.b64decode(delta_b64)
                        # Encolar en el distribuidor de audio con ritmo (Pacer)
                        await self.sessions[session_id]["outbound_queue"].put((raw_pcm, False))

                elif event_type == "response.audio.done":
                    # Indicar al pacer el fin de la ráfaga de habla actual
                    await self.sessions[session_id]["outbound_queue"].put((b"", True))
                    logger.info(f"[{client_id}] OpenAI terminó de transmitir el bloque de audio actual.")

                elif event_type == "input_audio_buffer.speech_started":
                    # ¡Barge-In detectado! El usuario interrumpió al bot hablando encima.
                    logger.info(f"[{client_id}] ¡Interrupción detectada (Barge-In)! Limpiando colas de salida.")
                    await self._barge_in_clear(session_id, client_ws)

                elif event_type == "error":
                    logger.error(f"[{client_id}] Error de OpenAI API: {event.get('error')}")
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error en bucle de escucha OpenAI de la sesión {session_id}: {e}")

    async def _realtime_audio_pacer_loop(self, session_id: str, client_ws: WebSocketServerProtocol):
        """
        Modela el comportamiento del IngressStreamer original.
        Agrupa el audio en paquetes de 100ms y los envía con retrasos calculados
        en tiempo real para evitar que el reproductor del cliente sufra jitter.
        """
        session = self.sessions.get(session_id)
        if not session:
            return

        queue = session["outbound_queue"]
        sample_rate = session["sample_rate"] # Ej: 8000 u 16000 Hz
        
        # 100ms chunk size calculation (PCM 16-bit = 2 bytes por muestra)
        chunk_size = int(sample_rate * 0.1 * 2) 
        chunk_interval = 0.100 # 100 milisegundos exactos
        
        buffer = b""
        pacing_start_time = None
        chunks_sent = 0

        try:
            while True:
                audio_chunk, is_last = await queue.get()
                
                if audio_chunk:
                    buffer += audio_chunk

                # Procesar y enviar el buffer acumulado en porciones de 100ms
                while len(buffer) >= chunk_size or (is_last and len(buffer) > 0):
                    if is_last and len(buffer) < chunk_size:
                        # Rellenar con ceros el último fragmento incompleto para mantener la alineación
                        current_chunk = buffer + b"\x00" * (chunk_size - len(buffer))
                        buffer = b""
                    else:
                        current_chunk = buffer[:chunk_size]
                        buffer = buffer[chunk_size:]

                    # Inicializar temporizador de ritmo absoluto en el primer fragmento de la oración
                    if pacing_start_time is None:
                        pacing_start_time = time.monotonic()
                        chunks_sent = 0

                    # Enviar trama binaria compacta
                    session["outbound_seq"] += 1
                    flags = FLAG_LAST_FRAME_COMPACT if (is_last and len(buffer) == 0) else 0
                    timestamp_micros = int(time.time() * 1000000)
                    
                    frame = build_compact_binary_frame(
                        bid=session["ingress_bid"],
                        source="none",
                        sequence_num=session["outbound_seq"],
                        timestamp_micros=timestamp_micros,
                        flags=flags,
                        media_data=current_chunk
                    )
                    
                    await client_ws.send(frame)
                    chunks_sent += 1

                    # Control de ritmo absoluto (Pacing) para evitar acumulación de latencia
                    target_time = pacing_start_time + (chunks_sent * chunk_interval)
                    sleep_time = target_time - time.monotonic()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                if is_last:
                    # Enviar marcador de final vacío obligatorio si es necesario
                    if chunks_sent > 0:
                        session["outbound_seq"] += 1
                        empty_frame = build_compact_binary_frame(
                            bid=session["ingress_bid"], source="none",
                            sequence_num=session["outbound_seq"],
                            timestamp_micros=int(time.time() * 1000000),
                            flags=FLAG_LAST_FRAME_COMPACT, media_data=b""
                        )
                        await client_ws.send(empty_frame)
                    
                    # Reiniciar estados para el siguiente turno de habla de la IA
                    pacing_start_time = None
                    buffer = b""

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Excepción en bucle de pautado de audio de la sesión {session_id}: {e}")

    async def _barge_in_clear(self, session_id: str, client_ws: WebSocketServerProtocol):
        """Interrumpe la reproducción actual vaciando los buffers y notificando al cliente."""
        session = self.sessions.get(session_id)
        if not session:
            return

        # Cancelar el pacer para detener cualquier envío en progreso de inmediato
        if session["pacer_task"] and not session["pacer_task"].done():
            session["pacer_task"].cancel()
            try:
                await session["pacer_task"]
            except asyncio.CancelledError:
                pass

        # Vaciar la cola de reproducción asíncrona
        queue = session["outbound_queue"]
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break

        # Enviar de inmediato un flag de corte (`lastf` o `FLAG_LAST_FRAME_COMPACT`) con payload vacío
        session["outbound_seq"] += 1
        barge_in_frame = build_compact_binary_frame(
            bid=session["ingress_bid"],
            source="none",
            sequence_num=session["outbound_seq"],
            timestamp_micros=int(time.time() * 1000000),
            flags=FLAG_LAST_FRAME_COMPACT,
            media_data=b""
        )
        await client_ws.send(barge_in_frame)
        
        # Relanzar el pacer limpio para recibir nuevas respuestas
        session["pacer_task"] = asyncio.create_task(self._realtime_audio_pacer_loop(session_id, client_ws))

    # =====================================================================
    # LIMPIEZA DE SESIONES
    # =====================================================================

    async def _handle_session_end(self, websocket: WebSocketServerProtocol, client_id: str, session_id: str):
        await self._cleanup_session_resources(session_id)
        
        ended_msg = {
            "version": "1.0.0", "type": "session.ended", "sessionId": session_id,
            "sequenceNum": self.get_next_sequence(client_id), "timestamp": datetime.now(UTC).isoformat()
        }
        await websocket.send(json.dumps(ended_msg))

    async def _cleanup_session_resources(self, session_id: str):
        if session_id in self.sessions:
            session = self.sessions.pop(session_id)
            
            if session["openai_task"] and not session["openai_task"].done():
                session["openai_task"].cancel()
            if session["pacer_task"] and not session["pacer_task"].done():
                session["pacer_task"].cancel()
                
            openai_ws = session["openai_ws"]
            if openai_ws and not openai_ws.closed:
                await openai_ws.close()
            logger.info(f"Todos los recursos de la sesión {session_id} han sido liberados.")

    async def _cleanup_client_sessions(self, client_id: str):
        sids = [sid for sid, s in self.sessions.items() if s["client_id"] == client_id]
        for sid in sids:
            await self._cleanup_session_resources(sid)
        if client_id in self.connections:
            del self.connections[client_id]
        if client_id in self.sequence_numbers:
            del self.sequence_numbers[client_id]

# =====================================================================
# HANDSHAKE Y PROCESO DE AUTENTICACIÓN JWT
# =====================================================================

async def make_process_request_handler(server: OpenAIBotServer):
    """Genera la función interceptora del HTTP Handshake para validar JWT."""
    async def process_request(connection, request):
        if not server.auth_enabled:
            return None # Proceder directo si la seguridad está desactivada
            
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            logger.warning("Intento de conexión rechazado: Encabezado Authorization ausente o inválido.")
            return Response(status_code=HTTPStatus.UNAUTHORIZED, headers=Headers([('Content-Type', 'text/plain')]), body=b"Unauthorized: Missing Bearer Token\n")
            
        token = auth_header[7:]
        try:
            # Soportar llave secreta tanto en formato string como en bytes UTF-8
            jwt.decode(token, server.jwt_secret_key, algorithms=['HS256'])
            return None # Éxito, continuar con la elevación del WebSocket
        except (ExpiredSignatureError, InvalidTokenError) as e:
            logger.warning(f"Fallo de firma JWT: {e}")
            return Response(status_code=HTTPStatus.UNAUTHORIZED, headers=Headers([('Content-Type', 'text/plain')]), body=b"Unauthorized: Invalid or Expired Token\n")
    return process_request

# =====================================================================
# ENTRADA DE LÍNEA DE COMANDOS Y ARRANQUE
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Byobot Lite Server - Solo OpenAI Realtime API")
    parser.add_argument("--host", default="0.0.0.0", help="Host de escucha (Por defecto: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8443, help="Puerto de escucha (Por defecto: 8443)")
    parser.add_argument("--enable-auth", action="store_true", help="Activar validación obligatoria de Token JWT Bearer")
    parser.add_argument("--jwt-secret", default="a37be135-3cea456e8b645f640cb1db4e", help="Clave secreta para JWT")
    args = parser.parse_args()

    # Configuración de logs limpia apuntando a Consola estándar
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    server_instance = OpenAIBotServer(
        host=args.host, port=args.port, 
        enable_auth=args.enable_auth, jwt_secret=args.jwt_secret
    )

    async def start():
        process_request_func = await make_process_request_handler(server_instance)
        import socket
        
        async with websockets.serve(
            server_instance.handle_connection,
            server_instance.host,
            server_instance.port,
            family=socket.AF_INET, # Forzar uso estricto de IPv4 para entornos de nube como Render
            process_request=process_request_func,
            ping_interval=30,
            ping_timeout=10
        ):
            logger.info(f"Servidor arrancado en ws://{server_instance.host}:{server_instance.port}")
            logger.info(f"Autenticación JWT: {'ACTIVA' if server_instance.auth_enabled else 'DESACTIVADA'}")
            logger.info("Presiona Ctrl+C para detener el servidor de manera segura.")
            await asyncio.Future() # Mantener corriendo indefinidamente

    try:
        asyncio.run(start())
    except KeyboardInterrupt:
        logger.info("Servidor detenido de manera ordenada.")

if __name__ == "__main__":
    main()