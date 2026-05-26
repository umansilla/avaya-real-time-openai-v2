import json
import datetime
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from rcms_handler import handle_session_start, handle_bot_start
from openai_handler import OpenAIBridge
from auth import authenticate_avaya_ws

# Configuración global de logging para que no use búfer
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger("main_app")

app = FastAPI()
sequence_counters = {}

def get_utc_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

@app.websocket("/avaya-rcms")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("Recibiendo nueva solicitud de conexión WebSocket...")
    await websocket.accept()
    
    # 1. Seguridad: Intercepta y valida el token JWT
    account_id = await authenticate_avaya_ws(websocket)
    if not account_id:
        logger.warning("La autenticación falló. Cerrando hilo del WebSocket.")
        return 

    session_id = None
    bridge = None  

    try:
        while True:
            raw_msg = await websocket.receive_text()
            msg = json.loads(raw_msg)
            
            msg_type = msg.get("type")
            session_id = msg.get("sessionId")
            logger.info(f"Mensaje recibido de Avaya: tipo='{msg_type}', sessionId='{session_id}'")
            
            if session_id not in sequence_counters:
                sequence_counters[session_id] = 1

            if msg_type == "session.start":
                logger.info("Procesando session.start y negociando parámetros...")
                response = handle_session_start(msg, sequence_counters[session_id])
                await websocket.send_text(json.dumps(response))
                logger.info("Respuesta session.started enviada.")
                sequence_counters[session_id] += 1
                
            elif msg_type == "bot.start":
                logger.info("Procesando bot.start...")
                response = handle_bot_start(msg, sequence_counters[session_id])
                await websocket.send_text(json.dumps(response))
                logger.info("Respuesta bot.started enviada.")
                sequence_counters[session_id] += 1
                
                logger.info(f"Iniciando flujo hacia OpenAI para la sesión {session_id}...")
                bridge = OpenAIBridge(websocket)
                await bridge.start()
                
            elif msg_type == "media":
                # Demasiados logs aquí saturarían la consola, lo mantenemos en silencio
                audio_b64 = msg.get("audio")
                if audio_b64 and bridge:
                    await bridge.send_audio_to_openai(audio_b64)
                    
            elif msg_type == "session.ping":
                logger.info("Ping recibido, enviando pong...")
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
        logger.warning(f"Avaya cerró la conexión (WebSocketDisconnect) para la sesión: {session_id}")
    except Exception as e:
        logger.error(f"Error crítico en el bucle principal de control: {e}", exc_info=True)
    finally:
        if bridge and bridge.openai_ws:
            try:
                await bridge.openai_ws.close()
                logger.info("Conexión con OpenAI finalizada y liberada con éxito.")
            except Exception as e:
                logger.error(f"Error al cerrar la conexión de OpenAI: {e}")