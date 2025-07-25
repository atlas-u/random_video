from typing import Any, Dict, Optional
from enum import Enum


class ResponseCode(Enum):
    """响应状态码枚举类"""
    SUCCESS = (200, "成功")
    FAIL = (400, "失败")
    UNAUTHORIZED = (401, "未授权")
    FORBIDDEN = (403, "禁止访问")
    NOT_FOUND = (404, "资源不存在")
    SERVER_ERROR = (500, "服务器错误")

    @property
    def code(self):
        return self.value[0]

    @property
    def msg(self):
        return self.value[1]


class ResponseUtil:
    """响应工具类"""

    @staticmethod
    def success(data: Any = None, message: Optional[str] = None) -> Dict[str, Any]:
        """成功响应

        Args:
            data: 返回数据
            message: 自定义消息

        Returns:
            格式化的响应字典
        """
        return {
            "code": ResponseCode.SUCCESS.code,
            "message": message or ResponseCode.SUCCESS.msg,
            "data": data
        }

    @staticmethod
    def fail(message: Optional[str] = None, code: Optional[int] = None) -> Dict[str, Any]:
        """失败响应

        Args:
            message: 错误消息
            code: 自定义错误码

        Returns:
            格式化的响应字典
        """
        return {
            "code": code or ResponseCode.FAIL.code,
            "message": message or ResponseCode.FAIL.msg,
            "data": {}
        }

    @staticmethod
    def response(code_enum: ResponseCode, data: Any = None, message: Optional[str] = None) -> Dict[str, Any]:
        """通用响应

        Args:
            code_enum: 响应状态码枚举
            data: 返回数据
            message: 自定义消息

        Returns:
            格式化的响应字典
        """
        return {
            "code": code_enum.code,
            "message": message or code_enum.msg,
            "data": data
        }


# 使用示例
if __name__ == "__main__":
    # 成功响应
    success_response = ResponseUtil.success(data={"user_id": 1, "username": "test"})
    print(success_response)

    # 失败响应
    fail_response = ResponseUtil.fail(message="用户名或密码错误")
    print(fail_response)

    # 自定义响应
    custom_response = ResponseUtil.response(
        code_enum=ResponseCode.NOT_FOUND,
        message="用户不存在"
    )
    print(custom_response)