from django.shortcuts import render
from .ots_client import get_latest_frames

def index(request):
    tags = {"channel": "c2"}
    data = get_latest_frames(tags)
    print(data)
    return render(request, 'video/index.html', {'frames': data})