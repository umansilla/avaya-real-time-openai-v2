import json
import datetime
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from rcms_handler import handle_session_start, handle_bot_start
from openai_handler import OpenAIBridge
from auth import authenticate_avaya_ws

# Configuración de logging para visualización inmediata en Render
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("main_app")

app = FastAPI()

# Diccionario para rastrear números de secuencia por sesión [cite: 3295]
sequence_counters = {}

def get_utc_timestamp():
    """Genera el timestamp ISO-8601 UTC requerido por Avaya[cite: 155]."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

@app.websocket("/avaya-rcms")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("Nueva solicitud de conexión WebSocket recibida.")
    await websocket.accept()
    
    # 1. Validación de Seguridad JWT [cite: 235, 2951]
    account_id = await authenticate_avaya_ws(websocket)
    if not account_id:
        logger.warning("Fallo en la autenticación. Conexión abortada.")
        return 

    session_id = None
    bridge = None  # Instancia única del puente hacia OpenAI por sesión
    decoder = json.JSONDecoder()

    try:
        while True:
            # Recibir mensaje de texto de Avaya [cite: 77]
            raw_msg = await websocket.receive_text()
            
            # --- MANEJO DE BATCHING (Múltiples objetos JSON)  ---
            idx = 0
            length = len(raw_msg)
            
            while idx < length:
                # Ignorar espacios en blanco entre objetos
                while idx < length and raw_msg[idx].isspace():
                    idx += 1
                if idx >= length:
                    break
                
                # Extraer el siguiente objeto JSON del flujo [cite: 3110]
                try:
                    msg, offset = decoder.raw_decode(raw_msg[idx:])
                    idx += offset
                except json.JSONDecodeError as e:
                    logger.error(f"Error al decodificar mensaje agrupado (Batch): {e}")
                    break

                # --- PROCESAMIENTO DE MENSAJES INDIVIDUALES ---
                msg_type = msg.get("type")
                session_id = msg.get("sessionId")
                
                if msg_type != "media":
                    logger.info(f"Mensaje RCMS recibido: {msg_type} (Session: {session_id})")
                
                # Inicializar contador de secuencia si es una sesión nueva [cite: 149]
                if session_id not in sequence_counters:
                    sequence_counters[session_id] = 1

                # Lógica de Respuesta según el Tipo de Mensaje [cite: 207]
                if msg_type == "session.start":
                    # Iniciar sesión y negociar parámetros de audio [cite: 278, 281]
                    response = handle_session_start(msg, sequence_counters[session_id])
                    await websocket.send_text(json.dumps(response))
                    logger.info("Respuesta 'session.started' enviada exitosamente.")
                    sequence_counters[session_id] += 1
                    
                elif msg_type == "bot.start":
                    # Confirmar inicio del servicio de Bot de IA [cite: 1514, 1515]
                    response = handle_bot_start(msg, sequence_counters[session_id])
                    await websocket.send_text(json.dumps(response))
                    logger.info("Respuesta 'bot.started' enviada. Estableciendo puente con OpenAI...")
                    sequence_counters[session_id] += 1
                    
                    # Conectar con OpenAI Realtime API
                    bridge = OpenAIBridge(websocket)
                    await bridge.start()
                    
                elif msg_type == "media":
                    # Streaming de audio entrante en formato Base64 [cite: 1403, 1404]
                    audio_b64 = msg.get("audio")
                    if audio_b64 and bridge:
                        await bridge.send_audio_to_openai(audio_b64)
                        
                elif msg_type == "session.ping":
                    # Responder al Keep-alive para evitar desconexiones [cite: 1157, 1158]
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
        logger.warning(f"Desconexión de Avaya para la sesión: {session_id}")
    except Exception as e:
        logger.error(f"Falla inesperada en el hilo principal: {e}", exc_info=True)
    finally:
        # Limpieza de recursos: cerrar socket de OpenAI si existe
        if bridge and bridge.openai_ws:
            try:
                await bridge.openai_ws.close()
                logger.info("Puente hacia OpenAI cerrado correctamente.")
            except Exception as e:
                logger.error(f"Error al cerrar OpenAI: {e}")