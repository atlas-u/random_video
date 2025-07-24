import random

import cv2
import oss2
import streamlink
from datetime import datetime
import numpy as np
import redis
import math
import time
from gevent import spawn, sleep


import multiprocessing as mp
import os
# from snowflake import Snowflake, SnowflakeGenerator

from video.ots_client import put_frame_data

# 使用代码嵌入的RAM用户的访问密钥配置访问凭证。
db_host = "192.168.3.80"
db_password = "Pl@1221view"
db_port = 6379

access_key_id = os.getenv("ACCESS_KEY_ID")
access_key_secret = os.getenv("ACCESS_KEY_SECRET")
oss_endpoint = os.getenv("OSS_ENDPOINT")
bucket_name = 'live-video-img'

# sf = Snowflake.parse(856165981072306191, 1288834974657)
# gen = SnowflakeGenerator.from_snowflake(sf)

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
            # print(f"清晰度映射失败或不存在 {mapped_quality}，尝试其他清晰度")
            for fallback in ["al_2000k", "md", "origin", "best"]:
                if fallback in streams:
                    mapped_quality = fallback
                    # print(f"使用备用清晰度 {mapped_quality}")
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
            sleep(frame_time)

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
channel_worker = None  # 通道名 => CapWork 实例


def updateChannel(channelName):
    global channel_worker
    # channel = redis_client.get('channel:config:' + channelName)
    # url = channel[b'url'].decode()
    URL_MAP = {
        "c1": "https://live.douyin.com/547977714661?column_type=single&from_search=true&is_aweme_tied=0&search_id=202507231620366A568B5E4A07237687BA&search_result_id=7529148369990585634",
        "c2": "https://live.douyin.com/870887192950?activity_name=&anchor_id=96582746791&banner_type=recommend&category_name=all&page_type=live_main_page",
        "c3": "https://live.douyin.com/296728101980?column_type=single&from_search=true&is_aweme_tied=0&search_id=202507241440238DF4EF59502A1634E9E2&search_result_id=7529680026967575835",
        "c4": "https://live.douyin.com/296728101980?column_type=single&from_search=true&is_aweme_tied=0&search_id=202507241440238DF4EF59502A1634E9E2&search_result_id=7529680026967575835"
    }
    url = URL_MAP[channelName]

    # url = channel.decode('utf-8')

    if channelName in change_channel:
        return
    elif channelName in channel_url and channel_url[channelName] != url:
        change_channel[channelName] = url
    else:
        channel_url[channelName] = url
    # 如果旧线程存在，先关闭它
    if channel_worker is not None:
        old_work = channel_worker
        if old_work.is_alive():
            # print(f"正在停止通道 {channelName} 的旧线程")
            old_work.terminate()  # 正确终止进程
            old_work.join(timeout=2)  # 等待最多 2 秒退出

    # 启动新线程并保存
    new_work = CapWork(que, url, channelName, quality)
    new_work.start()
    channel_worker = new_work

def upload_data():
    # while True:
    data = upload_que.get()
    if 'type' in data and data["type"] == 'frame':
        del data["type"]
        rand = prng_with_seed(data['hex'])
        data["rand"] = rand
        # data["id"] = str(next(gen))
        data["time"] = str(time.time_ns() // 1_000_000)
        print("want upload data === ",data)
        put_frame_data(data)


frame_cache = dict()
frame_data = dict()
check_list = list()

def prng_with_seed(seed):
    rng = random.Random(seed)
    return rng.randint(0, 2 ** 32 - 1)

def upload_img_and_update(data, suffix):
    img = data["img"]
    tid = data["tid"]
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, oss_endpoint, bucket_name)
    key = f"video/{tid}_{suffix}.jpg"
    bucket.put_object(key, img)
    data["img"] = f"https://{bucket_name}.{oss_endpoint}/{key}"


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

    upload_img_and_update(c1, "c1")
    upload_img_and_update(c2, "c2")
    upload_img_and_update(c3, "c3")
    upload_img_and_update(c4, "c4")

    merge = c1_np + c2_np + c3_np + c4_np

    thex = tid.to_bytes(6, 'big').hex()
    mhex = merge.tobytes().hex()
    out_hex = thex + mhex

    upload_que.put({
        "tid": tid,
        "type": "frame",
        "hex": out_hex,
        "c1": c1["url"],
        "img1":c1["img"],
        "c2": c2["url"],
        "img2": c2["img"],
        "c3": c3["url"],
        "img3": c3["img"],
        "c4": c4["url"],
        "img4": c4["img"],
        "time": time.time(),
        "rand": 0,
        # "id": "0",
    })

    redis_client.publish("channel:out", out_hex)

    global check_list
    check_list = []


def random_main():
    print("开始运行 process")
    ordered_frame_buffer = {"c1": None, "c2": None, "c3": None, "c4": None}
    latest_tid = 0
    while True:
        # 热切换通道
        for ch in ordered_frame_buffer:
            if ordered_frame_buffer[ch] is None:
                # 使用这个空位
                updateChannel(ch)
                break
        data = que.get()
        channelName = data["channelName"]
        tid = data["tid"]


        if latest_tid == 0:
            latest_tid = tid
        # 保证有缓存
        if channelName not in frame_cache:
            frame_cache[channelName] = data
        # 更新 frame_data，处理异常帧回滚
        if data["code"] != 0:
            frame_data[channelName] = frame_cache[channelName]
            frame_data[channelName]["code"] = data["code"]
        else:
            frame_data[channelName] = data
            frame_cache[channelName] = data  # 正确帧才更新缓存
        # 更新按顺序等待的 buffer
        ordered_frame_buffer[channelName] = data

        # 如果四个通道都有了，就处理一次
        if all(ordered_frame_buffer.values()):
            # 取最新 tid
            tid = max(frame["tid"] for frame in ordered_frame_buffer.values())
            frameHanld(tid, isFull=True)
            print("保存数据", tid)
            upload_data()
            # 清空等待缓存，准备下一轮
            ordered_frame_buffer = {"c1": None, "c2": None, "c3": None, "c4": None}

        latest_tid = tid

if __name__ == "__main__":
    random_main()
