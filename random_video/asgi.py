"""
ASGI config for random_video project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/howto/deployment/asgi/
"""

import os
import django
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.layers import get_channel_layer
from video.routing import websocket_urlpatterns

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'liveboard.settings')
django.setup()

application = ProtocolTypeRouter({
    "websocket": URLRouter(websocket_urlpatterns),
})
