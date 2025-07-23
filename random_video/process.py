import random

import cv2
import streamlink
from datetime import datetime
import numpy as np
import redis
import math
import time
import base64
import threading
import multiprocessing as mp
import os
import signal
import json
import asyncio
from channels.layers import get_channel_layer
from snowflake import Snowflake, SnowflakeGenerator

from video.ots_client import put_frame_data

# 使用代码嵌入的RAM用户的访问密钥配置访问凭证。
db_host = "192.168.3.80"
db_password = "Pl@1221view"
db_port = 6379

sf = Snowflake.parse(856165981072306191, 1288834974657)
gen = SnowflakeGenerator.from_snowflake(sf)

redis_client = redis.Redis(
    host=db_host,
    port=db_port,
    # username=db_user,
    password=db_password,
)


class CapWork(mp.Process):
    def __init__(self, que, url, channelName, quality):
        super().__init__()
        self.url = url
        self.que = que
        self.channelName = channelName
        self.quality = quality
        self.errCount = 0


    def run(self):
        streams = streamlink.streams(self.url)

        # print("可用清晰度列表：")
        # for quality in streams.keys():
        #     print(quality)

        mapped_quality = self.quality
        if mapped_quality not in streams:
            print(f"清晰度映射失败或不存在 {mapped_quality}，尝试其他清晰度")
            for fallback in ["al_2000k", "md", "origin", "best"]:
                if fallback in streams:
                    mapped_quality = fallback
                    print(f"使用备用清晰度 {mapped_quality}")
                    break
            else:
                print("没有合适的清晰度，无法播放")
                return

        s_url = streams[mapped_quality].to_url()
        cap = cv2.VideoCapture(s_url)
        frame_time = 1.0 / 40.0
        latest = 0
        print("======run=====", self.channelName, self.url)
        while True:
            now = datetime.now()
            if now.second != latest:
                latest = now.second
                self.getData(cap)
            time.sleep(frame_time)

    def getData(self, cap):
        try:
            size = 64
            now = datetime.now()
            t1 = time.time_ns() // 1_000_000
            ret, frame = cap.read()
            if not ret:
                self.errCount += 1
                return
            msec = cap.get(cv2.CAP_PROP_POS_MSEC)
            arr = np.asarray(frame)
            h, w = arr.shape[:2]
            h = math.ceil(h / size)
            w = math.ceil(w / size)
            barr = np.zeros(w * h, dtype=np.uint8)
            for y in range(h):
                for x in range(w):
                    block = arr[y * size: (y + 1) * size, x * size: (x + 1) * size]
                    he = np.sum(block) % 0xFF
                    b = 1 if he > 127 else 0
                    barr[y * w + x] = b
            has_bytes = np.packbits(barr).tobytes()
            tid = int(now.strftime("%Y%m%d%H%M%S"))
            vw = cap.get(cv2.CAP_PROP_FRAME_WIDTH)
            vh = cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
            t2 = time.time_ns() // 1_000_000
            fileData = cv2.imencode(".png", frame)[1].tobytes()
            data = {
                "tid": tid,
                "code": 0,
                "hash_bytes": has_bytes,
                "msec": int(msec),
                "t1": t1,
                "t2": t2,
                "width": vw,
                "height": vh,
                "channelName": self.channelName,
                "url": self.url,
                "img": fileData,
                "stid": tid
            }
            self.que.put(data)
        except Exception as e:
            self.errCount += 1
            self.que.put({
                "pid": os.getpid(),
                "code": 1,
                "channelName": self.channelName,
                "errCount": self.errCount
            })


upload_que = mp.Queue()
que = mp.Queue()
quality = '720p'

channel_url = dict()
change_channel = list()


def updateChannel(channelName):
    url = "https://live.douyin.com/870887192950?activity_name=&anchor_id=96582746791&banner_type=recommend&category_name=all&page_type=live_main_page"
    print("updateChannel:", url)
    if channelName in change_channel:
        return
    elif channelName in channel_url and channel_url[channelName] != url:
        change_channel[channelName] = url
    else:
        channel_url[channelName] = url
    work = CapWork(que, url, channelName, quality)
    work.start()


def subMsg(msg):
    if msg[0] == b'subscribe':
        print("订阅成功", msg[1])
    elif msg[0] == b'message':
        print("message:", msg[2])
        updateChannel(msg[2].decode())

def redis_subscriber():
    pub = redis_client.pubsub()
    pub.subscribe("channel:update")
    while True:
        msg = pub.parse_response()
        subMsg(msg)


def upload_data():
    while True:
        data = upload_que.get()
        tid = data["tid"]
        tid_str = str(tid)

        if 'type' in data and data["type"] == 'frame':
            del data["type"]
            jsonStr = json.dumps(data)
            print(f"[FRAME] tid={tid_str} json={jsonStr}")
            print(f"[FRAME] tid={tid_str} hex={data['hex']}")
        else:
            img = data["img"]
            hash_bytes = data["hash_bytes"]
            code = data["code"]
            msec = data["msec"]
            t1 = data["t1"]
            t2 = data["t2"]
            width = data["width"]
            height = data["height"]
            channelName = data["channelName"]
            url = data["url"]
            stid = data["stid"]
            merge = tid.to_bytes(6, 'big') + hash_bytes
            mhex = merge.hex()
            b64 = str(base64.b64encode(merge), 'utf-8')
            rand = prng_with_seed(mhex)
            putdata = {
                "id": next(gen),
                "tid": tid,
                "channel": channelName,
                "url": url,
                "stid": stid,
                "t1": t1,
                "t2": t2,
                "code": code,
                "msec": msec,
                "hex": mhex,
                "b64": b64,
                "rand": rand
            }
            jsonStr = json.dumps(putdata)
            print(f"[DATA] tid={tid_str} json={jsonStr}")
            print(f"[DATA] tid={tid_str} hex={mhex}")
            print(f"[DATA] img size: {len(img)} bytes")
            put_frame_data(putdata)

frame_cache = dict()
frame_data = dict()
check_list = list()

# def hex_to_random_by_hash(hex_str):
#     b = bytes.fromhex(hex_str)
#     digest = hashlib.sha256(b).digest()
#     rand = int.from_bytes(digest[:6], 'big')
#     return rand



def prng_with_seed(seed):
    rng = random.Random(seed)
    return rng.randint(0, 2 ** 32 - 1)

def frameHanld(tid, isFull):
    if isFull:
        frame_cache["c1"] = frame_data["c1"]
        frame_cache["c2"] = frame_data["c2"]
        frame_cache["c3"] = frame_data["c3"]
        frame_cache["c4"] = frame_data["c4"]
    else:
        if "c1" not in frame_data:
            frame_data["c1"] = frame_cache["c1"]
        else:
            frame_cache["c1"] = frame_data["c1"]

        if "c2" not in frame_data:
            frame_data["c2"] = frame_cache["c2"]
        else:
            frame_data["c2"] = frame_data["c2"]

        if "c3" not in frame_data:
            frame_data["c3"] = frame_cache["c3"]
        else:
            frame_data["c3"] = frame_data["c3"]

        if "c4" not in frame_data:
            frame_data["c4"] = frame_cache["c4"]
        else:
            frame_data["c4"] = frame_data["c4"]

    c1 = frame_data["c1"]
    c1["tid"] = tid

    c2 = frame_data["c2"]
    c2["tid"] = tid

    c3 = frame_data["c3"]
    c3["tid"] = tid

    c4 = frame_data["c4"]
    c4["tid"] = tid

    c1_np = np.frombuffer(c1["hash_bytes"], dtype=np.uint16)
    c2_np = np.frombuffer(c2["hash_bytes"], dtype=np.uint16)
    c3_np = np.frombuffer(c3["hash_bytes"], dtype=np.uint16)
    c4_np = np.frombuffer(c4["hash_bytes"], dtype=np.uint16)

    merge = c1_np + c2_np + c3_np + c4_np

    thex = tid.to_bytes(6, 'big').hex()
    mhex = merge.tobytes().hex()
    out_hex = thex + mhex

    upload_que.put({
        "tid": tid,
        "type": "frame",
        "hex": out_hex,
        "c1": c1["url"],
        "c2": c2["url"],
        "c3": c3["url"],
        "c4": c4["url"],
    })

    redis_client.publish("channel:out", out_hex)

    upload_que.put(c1)
    upload_que.put(c2)
    upload_que.put(c3)
    upload_que.put(c4)

    global check_list
    check_list = []


def random_main():
    updateChannel("c2")
    subscriber_thread = threading.Thread(target=redis_subscriber)
    subscriber_thread.start()
    upload_thread = threading.Thread(target=upload_data)
    upload_thread.start()

    latest_tid = 0
    while True:
        data = que.get()
        channelName = data["channelName"]
        tid = data["tid"]

        if latest_tid == 0:
            latest_tid = tid
        print("=========data:", tid, channelName, data["code"])

        if channelName in change_channel:
            channel_url[channelName] = change_channel[channelName]
            del change_channel[channelName]
            os.kill(data["pid"], signal.SIGTERM)
            frame_data[channelName] = data

        if "c1" not in frame_cache:
            frame_cache["c1"] = data
        if "c2" not in frame_cache:
            frame_cache["c2"] = data
        if "c3" not in frame_cache:
            frame_cache["c3"] = data
        if "c4" not in frame_cache:
            frame_cache["c4"] = data

        if data["code"] != 0:
            frame_data[channelName] = frame_cache[channelName]
            frame_data[channelName]["code"] = data["code"]
        else:
            frame_data[channelName] = data

        check_list.append(channelName)

        if "c1" in check_list and "c2" in check_list and "c3" in check_list and "c4" in check_list:
            frameHanld(data["tid"], True)
        elif latest_tid != data["tid"]:
            frameHanld(latest_tid, False)

        latest_tid = tid


# if __name__ == "__main__":
#     random_main()
