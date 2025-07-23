import json

from django.http import StreamingHttpResponse
import time

from django.shortcuts import render
from .ots_client import get_latest_frames

def index(request):
    tags = {"channel": "c2"}
    data = get_latest_frames(tags)
    print(data)
    return render(request, 'video/index.html', {'frames': data})

def sse_frames(request):
    def event_stream():
        while True:
            tags = {"channel": "c2"}
            data = get_latest_frames(tags)  # 应返回一个帧对象列表
            json_data = json.dumps(data)
            yield f"data: {json_data}\n\n"
            time.sleep(1)

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    return response