from channels.generic.websocket import AsyncWebsocketConsumer
import json

class FrameConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()
        await self.send(text_data=json.dumps({
            "message": "WebSocket 连接成功！"
        }))

    async def disconnect(self, close_code):
        pass

    async def receive(self, text_data):
        # 处理前端发来的数据
        await self.send(text_data=json.dumps({
            "message": f"收到: {text_data}"
        }))
