import os
import jwt
import logging
from fastapi import WebSocket, status

# Configurar el logger para este módulo
logger = logging.getLogger("auth_module")

async def authenticate_avaya_ws(websocket: WebSocket) -> str:
    """
    Valida el token JWT enviado por Avaya en la cabecera Authorization.
    Retorna el Account ID si es válido. Si no lo es, cierra la conexión.
    """
    # Imprimimos todos los headers para depuración visual
    logger.info(f"Nuevos Headers recibidos: {websocket.headers}")
    
    auth_header = websocket.headers.get("Authorization")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        logger.error("Conexión rechazada: Header Authorization ausente o no empieza con 'Bearer '.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing or Invalid Token Format")
        return None

    token = auth_header.split(" ")[1]
    logger.info(f"Token extraído (primeros 20 caracteres): {token[:20]}...")
    
    avaya_secret = os.getenv("AVAYA_PRIMARY_KEY") 

    if not avaya_secret:
        logger.error("Error del servidor: Variable de entorno AVAYA_PRIMARY_KEY no configurada en Render.")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Server Config Error")
        return None

    try:
        # Intentamos decodificar asumiendo Fase 1 (HS256)
        payload = jwt.decode(token, avaya_secret, algorithms=["HS256"])
        account_id = payload.get("sub")
        logger.info(f"Autenticación exitosa. Account ID: {account_id}")
        return account_id

    except jwt.ExpiredSignatureError:
        logger.warning("Conexión rechazada: El token JWT ha expirado.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token Expired")
    except jwt.InvalidTokenError as e:
        logger.error(f"Conexión rechazada: Token inválido. Motivo: {e}")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid Token")
    except Exception as e:
        logger.error(f"Error inesperado en la validación JWT: {e}")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Internal Error")
        
    return None