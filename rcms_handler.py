import datetime

def get_utc_timestamp():
    # Avaya requiere milisegundos en formato ISO-8601 UTC
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='milliseconds')

def handle_session_start(msg: dict, seq_num: int) -> dict:
    """
    Procesa el mensaje session.start y genera la respuesta session.started.
    """
    session_id = msg.get("sessionId")
    
    # Para este ejemplo usaremos PCMU a 8000Hz (G.711 u-law) y Base64.
    # Aunque Avaya recomienda Binary para producción por eficiencia de ancho de banda,
    # Base64 es más fácil para iterar rápido con JSON.
    response = {
        "version": "1.0.0",
        "type": "session.started",
        "sessionId": session_id,
        "sequenceNum": seq_num,
        "timestamp": get_utc_timestamp(),
        "payload": {
            "services": ["bot"],
            "mediaTransport": {
                "type": "avaya-wss",
                "preferredPTimeMs": 20,
                "dtx": "auto",
                "maskDTMF": False,
                "mediaCodecs": [
                    ["audio", "PCMU", 8000, 1]
                ],
                "transportEncoding": "base64" 
            }
        }
    }
    return response

def handle_bot_start(msg: dict, seq_num: int) -> dict:
    """
    Procesa el mensaje bot.start y genera la respuesta bot.started.
    """
    session_id = msg.get("sessionId")
    
    # Necesitamos extraer el endpointId del cliente que originó la llamada
    # para confirmarle a Avaya a qué endpoint nos estamos enlazando.
    endpoint_id = msg.get("payload", {}).get("endpointId", "")
    
    response = {
        "version": "1.0.0",
        "type": "bot.started",
        "sessionId": session_id,
        "sequenceNum": seq_num,
        "timestamp": get_utc_timestamp(),
        "payload": {
            "endpointId": endpoint_id
        }
    }
    return response