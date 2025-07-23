import time
from tkinter.constants import INSERT

from tablestore import (
    OTSClient, Row, TableMeta, TableOptions,
    RowExistenceExpectation, Condition, INF_MIN, INF_MAX, Direction,
    CreateTimeseriesTableRequest, ReservedThroughput, CapacityUnit, TimeseriesTableOptions, TimeseriesMetaOptions,
    TimeseriesTableMeta, TimeseriesKey, TimeseriesRow, GetTimeseriesDataRequest
)


# 阿里云配置
access_key_id = "***"
access_key_secret = "***"
endpoint = "https://***"

instance_name = "t01w1pc2nz0p"
table_name = "live_data"

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
        "channel": "c2",
    }

    key = TimeseriesKey("frame_measure", "video_source", tags)

    fields = {
        "tid": data["tid"],
        "channel": data["channel"],
        "url": data["url"],
        "hex": data["hex"],
        "b64": data["b64"],
        "t1": data["t1"],
        "t2": data["t2"],
        "msec": data["msec"],
        "code": data["code"],
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

def get_latest_frames(tags,measure_name = "frame_measure", datasource = "video_source", limit=10):
    key = TimeseriesKey(measure_name, datasource, tags)

    request = GetTimeseriesDataRequest(table_name)
    request.timeseriesKey = key
    request.endTimeInUs = int(time.time() * 1_000_000)  # 当前时间戳（微秒）
    request.limit = limit
    request.fieldsToGet = {}  # 留空表示取所有字段

    try:
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
            "tid": data.get("tid"),
            "channel": data.get("channel"),
            "url": data.get("url"),
            "hex": data.get("hex"),
            "b64": data.get("b64"),
            "t1": data.get("t1"),
            "t2": data.get("t2"),
            "msec": data.get("msec"),
            "code": data.get("code"),
            "rand": data.get("rand"),
        }
        result.append(item)
    return result
