from django.shortcuts import render
from .ots_client import get_latest_frames

def index(request):
    data = get_latest_frames()
    return render(request, 'video/index.html', {'frames': data})