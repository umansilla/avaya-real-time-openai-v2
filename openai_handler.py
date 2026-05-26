import os
import json
import asyncio
import websockets
from dotenv import load_dotenv

load_dotenv()

class OpenAIBridge:
    def __init__(self, avaya_ws):
        self.avaya_ws = avaya_ws
        self.openai_ws = None
        # Avaya requiere un Audio Sequence Number (asn) que inicie en 1 y se incremente 
        self.avaya_asn = 1

    async def start(self):
        """Inicia la conexión con OpenAI y configura el formato de audio."""
        url = "wss://api.openai.com/v1/realtime?model=gpt-realtime-mini"
        headers = {
            "Authorization": f"Bearer {os.getenv('OPENAI_API_KEY')}"
        }
        
        self.openai_ws = await websockets.connect(url, extra_headers=headers)
        
        # Configurar la sesión para usar G.711 u-law, coincidiendo con el PCMU de Avaya
        session_update = {
            "type": "session.update",
            "session": {
                "input_audio_format": "g711_ulaw",
                "output_audio_format": "g711_ulaw",
                "turn_detection": {"type": "server_vad"} # Detección automática de voz
            }
        }
        await self.openai_ws.send(json.dumps(session_update))
        
        # Lanzar la tarea para escuchar las respuestas de OpenAI en segundo plano
        asyncio.create_task(self.receive_from_openai())
        print("Conectado a OpenAI Realtime API")

    async def send_audio_to_openai(self, base64_audio: str):
        """Recibe audio de Avaya y lo inyecta al buffer de OpenAI."""
        if self.openai_ws:
            event = {
                "type": "input_audio_buffer.append",
                "audio": base64_audio
            }
            await self.openai_ws.send(json.dumps(event))

    async def receive_from_openai(self):
        """Escucha el audio generado por OpenAI y lo envía a Avaya."""
        try:
            async for message in self.openai_ws:
                event = json.loads(message)
                
                # Cuando OpenAI genera audio hablado
                if event.get("type") == "response.audio.delta":
                    avaya_media_msg = {
                        "type": "media",
                        "bid": 0, # Usamos el Bearer ID 0 del endpoint principal 
                        "asn": self.avaya_asn,
                        "audio": event["delta"]
                    }
                    await self.avaya_ws.send_text(json.dumps(avaya_media_msg))
                    self.avaya_asn += 1
                    
        except websockets.exceptions.ConnectionClosed:
            print("Conexión con OpenAI cerrada.")
        except Exception as e:
            print(f"Error en OpenAIBridge: {e}")