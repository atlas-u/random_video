from django.urls import re_path
from video import consumers

websocket_urlpatterns = [
    re_path(r'ws/frames/$', consumers.FrameConsumer.as_asgi()),
]