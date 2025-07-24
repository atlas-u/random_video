import os
import time
from tkinter.constants import INSERT
from dotenv import load_dotenv

from tablestore import (
    OTSClient, Row, TableMeta, TableOptions,
    RowExistenceExpectation, Condition, INF_MIN, INF_MAX, Direction,
    CreateTimeseriesTableRequest, ReservedThroughput, CapacityUnit, TimeseriesTableOptions, TimeseriesMetaOptions,
    TimeseriesTableMeta, TimeseriesKey, TimeseriesRow, GetTimeseriesDataRequest
)

load_dotenv()

# 阿里云配置
access_key_id = os.getenv("ACCESS_KEY_ID")
access_key_secret = os.getenv("ACCESS_KEY_SECRET")
endpoint = os.getenv("ENDPOINT")
instance_name = os.getenv("INSTANCE_NAME")
table_name = os.getenv("TABLE_NAME")

# 打印测试
print("access_key_id:", access_key_id)
print("endpoint:", endpoint)

# 创建客户端
client = OTSClient(endpoint, access_key_id, access_key_secret, instance_name)


# def create_table_if_not_exists():
#     try:
#         tables = client.list_table()
#         if table_name in tables:
#             print(f"表 {table_name} 已存在")
#             return
#         schema_of_primary_key = [('id', 'STRING')]  # 注意这里使用字符串类型
#         schema = TableMeta(table_name, schema_of_primary_key)
#         table_option = TableOptions(time_to_live=-1, max_version=1)
#         reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
#         client.create_table(schema, table_option, reserved_throughput)
#         print(f"表 {table_name} 创建成功")
#         time.sleep(5)
#     except Exception as e:
#         print("表创建失败:", e)

def create_table_if_not_exists():
    try:
        # 获取已存在的时序表列表
        tables = client.list_timeseries_table()
        table_names = [t.timeseries_table_name for t in tables]
        if table_name in table_names:
            print(f"时序表 {table_name} 已存在")
            return

        # 创建时序表配置
        table_option = TimeseriesTableOptions(172800)  # 数据保留2天
        meta_option = TimeseriesMetaOptions(-1, True)# 元数据永久保存
        table_meta = TimeseriesTableMeta(table_name, table_option, meta_option)

        # 创建请求并发送
        request = CreateTimeseriesTableRequest(table_meta)
        client.create_timeseries_table(request)
        print(f"时序表 {table_name} 创建成功")
    except Exception as e:
        print("时序表创建失败:", e)


create_table_if_not_exists()

# def put_frame_data(data):
#     primary_key = [('id', data["id"])]
#     attr_columns = [
#         ('tid', data["tid"]),
#         ('channel', data['channel']),
#         ('url', data['url']),
#         ('hex', data['hex']),
#         ('b64', data['b64']),
#         ('t1', data['t1']),
#         ('t2', data['t2']),
#         ('msec', data['msec']),
#         ('code', data['code']),
#     ]
#     row = Row(primary_key, attr_columns)
#     cond = Condition(RowExistenceExpectation.IGNORE)
#     client.put_row(table_name, row, cond)

def put_frame_data(data):
    tags = {
        "video": "random",
    }

    key = TimeseriesKey("frame_measure", "video_source", tags)

    fields = {
        "c1": data["c1"],
        "c2": data["c2"],
        "c3": data["c3"],
        "c4": data["c4"],
        "hex": data["hex"],
        "img1": data["img1"],
        "img2": data["img2"],
        "img3": data["img3"],
        "img4": data["img4"],
        "tid":  data["tid"],
        "time": data["time"],
        "rand": data["rand"],
    }
    timestamp = int(time.time() * 1000000)
    row = TimeseriesRow(key, fields, timestamp)
    try:
        client.put_timeseries_data(table_name, [row])
        print("Timeseries data written successfully.")
    except Exception as e:
        print("Failed to write timeseries data:", e)


# def get_latest_frames(limit=10):
#     query = client.get_range(
#         table_name,
#         direction=Direction.BACKWARD,
#         inclusive_start_primary_key=[('id', INF_MIN)],
#         exclusive_end_primary_key=[(' id', INF_MAX)],
#         limit=limit,
#     )
#     return [row.primary_key + row.attribute_columns for row in query.rows]

def get_latest_frames(limit = 10, measure_name ="frame_measure", datasource ="video_source"):
    try:
        tags = {
            "video": "random",
        }
        key = TimeseriesKey(measure_name, datasource, tags)

        request = GetTimeseriesDataRequest(table_name)
        request.timeseriesKey = key
        request.endTimeInUs = int(time.time() * 1_000_000)
        request.limit = limit
        request.fieldsToGet = {}  # 留空表示取所有字段

        request.backward = True
        response = client.get_timeseries_data(request)
        return format_timeseries_rows(response.rows)  # 已是结构化的 Point 数据
    except Exception as e:
        print(f"获取时序数据失败: {e}")
        return []

def format_timeseries_rows(rows):
    result = []
    for row in rows:
        data = row.fields
        item = {
            "c1": data.get("c1"),
            "c2": data.get("c2"),
            "c3": data.get("c3"),
            "c4": data.get("c4"),
            "hex": data.get("hex"),
            "img1": data.get("img1"),
            "img2": data.get("img2"),
            "img3": data.get("img3"),
            "img4": data.get("img4"),
            "tid": data.get("tid"),
            "time": data.get("time"),
            "rand": data.get("rand"),
        }
        result.append(item)
    return result
