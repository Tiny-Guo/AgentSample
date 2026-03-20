"""
LangGraph 状态定义
"""
from typing import Annotated, TypedDict
from langgraph.graph import add_messages


class AgentState(TypedDict):
    """Agent 状态定义"""
    messages: Annotated[list, add_messages]  # 对话历史
    intent: str                               # 用户意图
    data_source: str                          # 数据来源: database / file
    query_params: dict                        # 查询参数
    report_data: any                          # 报表数据
    report_path: str                           # 生成的报表文件路径
    error: str                                # 错误信息
    action: str                               # 当前执行的动作
