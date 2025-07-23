from django.urls import path
from video import views
from .views import sse_frames
urlpatterns = [
    path('', views.index, name='index'),
    path('sse/frames/', sse_frames, name='sse_frames'),
]
