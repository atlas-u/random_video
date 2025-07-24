from gevent import monkey
monkey.patch_all()

import json
from flask import Flask, render_template, Response, stream_with_context
from video import ots_client, process
from gevent import spawn, sleep
from gevent.pywsgi import WSGIServer

app = Flask(__name__)

@app.route('/')
def index():
    try:
        data = ots_client.get_latest_frames()
    except Exception as e:
        print(f"OSError 捕获: {e}")
        data = []
    return render_template('index.html', frames=data)

@app.route('/sse/frames/')
def stream():
    def event_stream():
        while True:
            data = ots_client.get_latest_frames()
            json_data = json.dumps(data)
            yield f"data: {json_data}\n\n"
            sleep(2)
    return Response(stream_with_context(event_stream()), content_type='text/event-stream')

def rrrr():
    while True:
        print("------")
        sleep(1)

if __name__ == '__main__':
    spawn(process.random_main)
    # spawn(rrrr)
    http_server = WSGIServer(('', 8999), app)
    print("✅ Flask SSE 服务已启动：http://localhost:8999")
    http_server.serve_forever()
