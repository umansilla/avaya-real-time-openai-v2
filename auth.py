import os
import jwt
from fastapi import WebSocket, status

async def authenticate_avaya_ws(websocket: WebSocket) -> str:
    """
    Valida el token JWT enviado por Avaya en la cabecera Authorization.
    Retorna el Account ID si es válido. Si no lo es, cierra la conexión.
    """
    # Extraemos el header de autorización
    auth_header = websocket.headers.get("Authorization")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        print("Conexión rechazada: Header Authorization ausente o inválido.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing or Invalid Token Format")
        return None

    # Extraer solo el string del token
    token = auth_header.split(" ")[1]
    
    # Llave primaria simétrica de tu entorno (Fase 1 - HS256)
    avaya_secret = os.getenv("AVAYA_PRIMARY_KEY") 

    if not avaya_secret:
        print("Error del servidor: AVAYA_PRIMARY_KEY no configurada.")
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR, reason="Server Config Error")
        return None

    try:
        # PyJWT valida automáticamente la firma y la fecha de expiración ('exp')
        payload = jwt.decode(token, avaya_secret, algorithms=["HS256"])
        
        # El claim 'sub' contiene el Account ID de Avaya
        account_id = payload.get("sub")
        print(f"Autenticación exitosa para la cuenta: {account_id}")
        return account_id

    except jwt.ExpiredSignatureError:
        print("Conexión rechazada: El token JWT ha expirado.")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Token Expired")
    except jwt.InvalidTokenError as e:
        print(f"Conexión rechazada: Token inválido ({e}).")
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid Token")
    
    return None