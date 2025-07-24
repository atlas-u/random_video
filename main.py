import traceback
from multiprocessing import Process

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

from flask import request, jsonify


@app.route('/verify')
def verify():
    try:
        time = request.args.get('time')
        hex_val = request.args.get('hex')
        rand_val = request.args.get('rand')
        if not all([time, hex_val, rand_val]):
            return jsonify({'error': '缺少必要参数(time/hex/rand)'}), 400
        filter_cond = {
            "hex": hex_val,
            "rand": rand_val
        }
        result = ots_client.get_one(time, filter_cond=filter_cond)
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"接口异常: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'error': '服务器内部错误'}), 500


if __name__ == '__main__':
    # spawn(process.random_main)
    p = Process(target=process.random_main)
    p.start()
    # spawn(rrrr)
    http_server = WSGIServer(('', 8999), app)
    print("✅ Flask SSE 服务已启动：http://localhost:8999")
    http_server.serve_forever()
