import os
import json
import asyncio
import websockets
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("openai_bridge")

class OpenAIBridge:
    def __init__(self, avaya_ws):
        self.avaya_ws = avaya_ws
        self.openai_ws = None
        self.avaya_asn = 1

    async def start(self):
        """Inicia la conexión con OpenAI y configura el formato de audio."""
        url = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview"
        
        # En las versiones más recientes de websockets, se usa 'additional_headers'
        # o simplemente se pasan los headers directamente dependiendo de la versión.
        headers = {
            "Authorization": f"Bearer {os.getenv('OPENAI_API_KEY')}",
            "OpenAI-Beta": "realtime=v1"
        }
        
        try:
            # Intentamos conectar con la sintaxis universalmente aceptada en entornos modernos
            self.openai_ws = await websockets.connect(url, additional_headers=headers)
            
            # Configurar la sesión para usar G.711 u-law (PCMU)
            session_update = {
                "type": "session.update",
                "session": {
                    "input_audio_format": "g711_ulaw",
                    "output_audio_format": "g711_ulaw",
                    "turn_detection": {"type": "server_vad"}
                }
            }
            await self.openai_ws.send(json.dumps(session_update))
            
            # Lanzar la tarea para escuchar a OpenAI
            asyncio.create_task(self.receive_from_openai())
            logger.info("Conexión establecida exitosamente con OpenAI Realtime API")
            
        except Exception as e:
            logger.error(f"Error al conectar con OpenAI: {e}")
            raise e

    async def send_audio_to_openai(self, base64_audio: str):
        """Envía audio de Avaya a OpenAI."""
        if self.openai_ws:
            event = {
                "type": "input_audio_buffer.append",
                "audio": base64_audio
            }
            await self.openai_ws.send(json.dumps(event))

    async def receive_from_openai(self):
        """Recibe audio de OpenAI y lo envía a Avaya."""
        try:
            async for message in self.openai_ws:
                event = json.loads(message)
                
                if event.get("type") == "response.audio.delta":
                    avaya_media_msg = {
                        "type": "media",
                        "bid": 0,
                        "asn": self.avaya_asn,
                        "audio": event["delta"]
                    }
                    await self.avaya_ws.send_text(json.dumps(avaya_media_msg))
                    self.avaya_asn += 1
                    
        except Exception as e:
            logger.error(f"Conexión con OpenAI terminada: {e}")