import json

from django.http import StreamingHttpResponse
import time

from django.shortcuts import render
from .ots_client import get_latest_frames


def index(request):
    try:
        data = get_latest_frames()
        print(data)
    except Exception as e:
        print(f"OSError 捕获: {e}")
        data = []
    # return render(request, 'video/index.html', {'frames': data})
    response = StreamingHttpResponse(data, content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    return response



def sse_frames(request):
    def event_stream():
        while True:
            data = get_latest_frames()  # 应返回一个帧对象列表
            json_data = json.dumps(data)
            yield f"data: {json_data}\n\n"
            time.sleep(2)

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    return response
