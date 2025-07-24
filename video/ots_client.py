import math
import os
import time
from dotenv import load_dotenv

from tablestore import (
    OTSClient, Row, TableMeta, TableOptions,
    RowExistenceExpectation, Condition, INF_MIN, INF_MAX, Direction,
    ReservedThroughput, CapacityUnit, BatchGetRowRequest, TableInBatchGetRowItem, CompositeColumnCondition,
    LogicalOperator, SingleColumnCondition, ComparatorType
)

load_dotenv()

# 阿里云配置
access_key_id = os.getenv("ACCESS_KEY_ID")
access_key_secret = os.getenv("ACCESS_KEY_SECRET")
endpoint = os.getenv("ENDPOINT")
instance_name = os.getenv("INSTANCE_NAME")
table_name = os.getenv("TABLE_NAME")

table_name_time = "live_time_data"

# 打印测试
# print("access_key_id:", access_key_id)
# print("endpoint:", endpoint)

# 创建客户端
client = OTSClient(endpoint, access_key_id, access_key_secret, instance_name)


def create_table_if_not_exists():
    try:
        tables = client.list_table()
        if table_name in tables:
            return
        schema_of_primary_key = [('time', 'STRING')]  # 注意这里使用字符串类型
        schema = TableMeta(table_name, schema_of_primary_key)
        table_option = TableOptions(time_to_live=-1, max_version=1)
        reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
        client.create_table(schema, table_option, reserved_throughput)
        print(f"表 {table_name} 创建成功")
        time.sleep(5)
    except Exception as e:
        print("表创建失败:", e)

# def create_table_if_not_exists():
#     try:
#         # 获取已存在的时序表列表
#         tables = client.list_timeseries_table()
#         table_names = [t.timeseries_table_name for t in tables]
#         if table_name_time in table_names:
#             # print(f"时序表 {table_name} 已存在")
#             return
#         # 创建时序表配置
#         table_option = TimeseriesTableOptions(172800)
#         meta_option = TimeseriesMetaOptions(-1, True)
#         table_meta = TimeseriesTableMeta(table_name_time, table_option, meta_option)
#
#         # 创建请求并发送
#         request = CreateTimeseriesTableRequest(table_meta)
#         client.create_timeseries_table(request)
#         print(f"时序表 {table_name_time} 创建成功")
#     except Exception as e:
#         print("时序表创建失败:", e)


create_table_if_not_exists()

def put_frame_data(data):
    primary_key = [('time', data["time"])]
    attr_columns = [
        ('c1', data["c1"]),
        ('c2', data['c2']),
        ('c3', data['c3']),
        ('c4', data['c4']),
        ('hex', data['hex']),
        ('img1', data['img1']),
        ('img2', data['img2']),
        ('img3', data['img3']),
        ('img4', data['img4']),
        ('tid', data['tid']),
        # ('time', data['time']),
        ('rand', data['rand']),
    ]
    row = Row(primary_key, attr_columns)
    cond = Condition(RowExistenceExpectation.IGNORE)
    try:
        client.put_row(table_name, row, cond)
        print("data written successfully.")
    except Exception as e:
        print("Failed to write data:", e)

# def put_frame_data(data):
#     tags = {
#         "video": "random",
#     }
#
#     key = TimeseriesKey("frame_measure", "video_source", tags)
#
#     fields = {
#         "c1": data["c1"],
#         "c2": data["c2"],
#         "c3": data["c3"],
#         "c4": data["c4"],
#         "hex": data["hex"],
#         "img1": data["img1"],
#         "img2": data["img2"],
#         "img3": data["img3"],
#         "img4": data["img4"],
#         "tid":  data["tid"],
#         "time": data["time"],
#         "rand": data["rand"],
#     }
#     timestamp = int(time.time() * 1000000)
#     row = TimeseriesRow(key, fields, timestamp)
#     try:
#         client.put_timeseries_data(table_name_time, [row])
#         print("Timeseries data written successfully.")
#     except Exception as e:
#         print("Failed to write timeseries data:", e)

"""
获取最新的帧数据（按时间倒序）

:param limit: 返回的最大帧数量
:param columns: 需要返回的列（None表示所有列）
:param filter_cond: 列过滤条件
:return: 包含帧数据的列表
"""
def get_latest_frames(limit=10, columns=None, filter_cond=None, time_range=None):
    cond = None
    # 设置主键范围（时间倒序）
    if time_range is not None:
        start_time = int(time_range["start_time"])
        end_time = int(time_range["end_time"])

        inclusive_start = max(start_time, end_time) + 1
        exclusive_end = min(start_time, end_time) - 1

        inclusive_start_primary_key = [('time', str(inclusive_start))]
        exclusive_end_primary_key = [('time', str(exclusive_end))]
    else:
        inclusive_start_primary_key = [('time', INF_MAX)]  # 从最新时间开始
        exclusive_end_primary_key = [('time', INF_MIN)]  # 到最旧时间结束

    if filter_cond is not None:
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("hex", filter_cond["hex"], ComparatorType.EQUAL, pass_if_missing=False))
        cond.add_sub_condition(SingleColumnCondition("rand", int(filter_cond["rand"]), ComparatorType.EQUAL, pass_if_missing=False))

    # 执行范围查询（倒序）
    consumed, next_start_pk, rows, _ = client.get_range(
        table_name,
        direction=Direction.BACKWARD,  # 倒序
        inclusive_start_primary_key=inclusive_start_primary_key,
        exclusive_end_primary_key=exclusive_end_primary_key,
        columns_to_get=columns,
        limit=limit,
        column_filter=cond,
        max_version=1,
    )

    # 打印调试信息
    print(f"Retrieved {len(rows)} frames. Consumed read units: {consumed.read}")
    data = transform_frame_data(rows)
    return data



"""
获取指定的一条数据

:param primary_key: 时间戳
"""
def get_one(primary_key, filter_cond=None):
    cond = None
    id_key = [('time', primary_key)]

    if filter_cond is not None:
        cond = CompositeColumnCondition(LogicalOperator.AND)
        cond.add_sub_condition(SingleColumnCondition("hex", filter_cond["hex"], ComparatorType.EQUAL, pass_if_missing=False))
        cond.add_sub_condition(SingleColumnCondition("rand", int(filter_cond["rand"]), ComparatorType.EQUAL, pass_if_missing=False))

    consumed, row, next_token = client.get_row(
        table_name,
        primary_key=id_key,
        column_filter=cond,
        max_version=1
    )
    # 打印调试信息
    print(f"Retrieved 1 frames. Consumed read units: {consumed.read}")
    if row is not None:
        data = transform_frame_data([row])
        return data[0]
    else:
        return {}

# def get_latest_frames(limit = 10, measure_name ="frame_measure", datasource ="video_source", queryTags = None):
#     try:
#         tags = {
#             "video": "random",
#         }
#         if queryTags:
#             tags.update(queryTags)
#
#         key = TimeseriesKey(measure_name, datasource, tags)
#
#         request = GetTimeseriesDataRequest(table_name_time)
#         request.timeseriesKey = key
#         request.endTimeInUs = int(time.time() * 1_000_000)
#         request.limit = limit
#         request.fieldsToGet = {}  # 留空表示取所有字段
#
#         request.backward = True
#         response = client.get_timeseries_data(request)
#         return format_timeseries_rows(response.rows)  # 已是结构化的 Point 数据
#     except Exception as e:
#         print(f"获取时序数据失败: {e}")
#         return []
"""
    将原始帧数据转换为标准格式
    :param raw_frames: 从Tablestore获取的原始帧数据列表
    :return: 转换后的帧数据列表
"""
def transform_frame_data(raw_frames):
    formatted_frames = []
    for frame in raw_frames:
        formatted = {
        }
        # 提取主键中的时间戳
        for pk in frame.primary_key:
            if pk[0] == 'time':
                formatted['time'] = int(pk[1])
                break
        attrs = {}
        for col in frame.attribute_columns:
            attrs[col[0]] = col[1]
        formatted.update({
            "tid": attrs.get('tid', 0),
            "hex": attrs.get('hex', ''),
            "rand": attrs.get('rand', 0),
            "c1": attrs.get('c1', ''),
            "img1": attrs.get('img1', ''),
            "c2": attrs.get('c2', ''),
            "img2": attrs.get('img2', ''),
            "c3": attrs.get('c3', ''),
            "img3": attrs.get('img3', ''),
            "c4": attrs.get('c4', ''),
            "img4": attrs.get('img4', '')
        })
        formatted_frames.append(formatted)
    return formatted_frames

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


if __name__ == "__main__":
    time_range = {
        "start_time": "1753353509201",
        "end_time": "1753353553734",
    }

    filter_cond = {
        "hex":"126afd38af5dfad200c49c53deb1",
        "rand":"845565712"
    }

    list = get_one("1753353553733",filter_cond=filter_cond)
    print(list)
