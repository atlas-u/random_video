from django.apps import AppConfig
from random_video.process import random_main
import os
import threading

class VideoConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'video'

    # def ready(self):
    #     if os.environ.get('RUN_MAIN') == 'true':  # 只在主进程启动子线程
    #         t = threading.Thread(target=random_main, daemon=True)
    #         t.start()
