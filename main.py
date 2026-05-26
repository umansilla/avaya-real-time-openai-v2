# main.py
import json
import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from rcms_handler import handle_session_start, handle_bot_start
from openai_handler import OpenAIBridge
from auth import authenticate_avaya_ws

app = FastAPI()

# Diccionario global para controlar los números de secuencia independientes de cada llamada/sesión
sequence_counters = {}

def get_utc_timestamp():
    """
    Retorna el timestamp exacto que exige Avaya: formato ISO-8601 UTC,
    con granularidad de milisegundos y el sufijo estricto 'Z'[cite: 155].
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

@app.websocket("/avaya-rcms")
async def websocket_endpoint(websocket: WebSocket):
    # Aceptar la conexión de red inicial (Handshake de WebSocket)
    await websocket.accept()
    
    # 1. Seguridad: Intercepta y valida el token JWT Bearer enviado por Avaya [cite: 235]
    account_id = await authenticate_avaya_ws(websocket)
    if not account_id:
        # Si no es válido, auth.py ya cerró la conexión con un código de política HTTP 401 [cite: 3327]
        return

    session_id = None
    bridge = None  # Se inicializa aquí para que persista durante toda la vida del socket

    try:
        while True:
            # Escuchar de forma asíncrona los mensajes en tiempo real de Avaya
            raw_msg = await websocket.receive_text()
            msg = json.loads(raw_msg)
            
            msg_type = msg.get("type")
            session_id = msg.get("sessionId")
            
            # Si es el primer mensaje de la sesión, inicializamos su contador en 1 [cite: 149]
            if session_id not in sequence_counters:
                sequence_counters[session_id] = 1

            # --- ENRUTADOR DE MENSAJES DEL PROTOCOLO AVAYA RCMS ---
            
            if msg_type == "session.start":
                # Primer paso del protocolo: Avaya propone los parámetros de la sesión [cite: 278, 281]
                response = handle_session_start(msg, sequence_counters[session_id])
                await websocket.send_text(json.dumps(response))
                sequence_counters[session_id] += 1
                
            elif msg_type == "bot.start":
                # Segundo paso: Avaya solicita levantar el agente conversacional inteligente [cite: 1476, 1477]
                response = handle_bot_start(msg, sequence_counters[session_id])
                await websocket.send_text(json.dumps(response))
                sequence_counters[session_id] += 1
                
                # Inicializamos el cliente hacia OpenAI Realtime usando este mismo socket para retransmitir
                print(f"Iniciando flujo de Inteligencia Artificial para la sesión {session_id}...")
                bridge = OpenAIBridge(websocket)
                await bridge.start()
                
            elif msg_type == "media":
                # Flujo continuo de audio: El usuario final está hablando por teléfono [cite: 1403]
                audio_b64 = msg.get("audio") # Avaya empaqueta trozos de audio en formato Base64 [cite: 1407]
                
                if audio_b64 and bridge:
                    # Inyectamos el audio directamente en la API de OpenAI (formato G.711 u-law emparejado)
                    await bridge.send_audio_to_openai(audio_b64)
                    
            elif msg_type == "session.ping":
                # Avaya envía pings cada 15 segundos si el canal está en silencio; exige un pong de vuelta [cite: 1157, 1168]
                pong = {
                    "version": "1.0.0",
                    "type": "session.pong",
                    "sessionId": session_id,
                    "sequenceNum": sequence_counters[session_id],
                    "timestamp": get_utc_timestamp()
                }
                await websocket.send_text(json.dumps(pong))
                sequence_counters[session_id] += 1

    except WebSocketDisconnect:
        print(f"Avaya cerró abruptamente la conexión para la sesión: {session_id} [cite: 3298]")
    except Exception as e:
        print(f"Error crítico en el bucle principal de control: {e}")
    finally:
        # Bloque de seguridad preventivo: si la llamada se cae o termina en Avaya,
        # matamos la sesión paralela que abrimos en los servidores de OpenAI para no desperdiciar recursos ni tokens.
        if bridge and bridge.openai_ws:
            try:
                await bridge.openai_ws.close()
                print("Conexión con OpenAI finalizada y liberada con éxito.")
            except Exception:
                pass