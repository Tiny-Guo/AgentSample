"""
LangGraph Agent 主程序
"""
from langgraph.graph import StateGraph, START, END
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_ollama import ChatOllama
from langchain_community.chat_models import ChatZhipuAI
from langchain_core.messages import SystemMessage
from langgraph.prebuilt import ToolNode
from .state import AgentState
from .nodes import SYSTEM_PROMPT
from .tools import TOOLS
from ..config import llm_config
from langchain_core.messages import HumanMessage


def create_llm():
    """根据配置创建LLM实例"""
    if llm_config.provider == "zhipu":
        import os
        os.environ["ZHIPUAI_API_KEY"] = llm_config.zhipu_api_key
        return ChatZhipuAI(
            model="glm-4",
            temperature=0
        )
    elif llm_config.provider == "openai":
        return ChatOpenAI(
            model="gpt-4o",
            api_key=llm_config.openai_api_key,
            temperature=0
        )
    elif llm_config.provider == "anthropic":
        return ChatAnthropic(
            model="claude-sonnet-4-20250514",
            api_key=llm_config.anthropic_api_key,
            temperature=0
        )
    elif llm_config.provider == "ollama":
        return ChatOllama(
            base_url=llm_config.ollama_base_url,
            model=llm_config.ollama_model,
            temperature=0
        )
    else:
        raise ValueError(f"不支持的LLM provider: {llm_config.provider}")


def create_agent():
    """创建Agent"""
    # 创建LLM
    llm = create_llm()

    # 绑定工具
    llm_with_tools = llm.bind_tools(TOOLS)

    # 定义节点
    def llm_node(state: AgentState):
        """LLM处理节点"""
        messages = state.get("messages", [])

        # 添加系统提示
        response = llm_with_tools.invoke(
            [SystemMessage(content=SYSTEM_PROMPT)] + messages
        )

        return {"messages": [response]}

    # 创建工具节点
    tool_node = ToolNode(TOOLS)

    # 构建图
    graph = StateGraph(AgentState)

    graph.add_node("llm", llm_node)
    graph.add_node("tool", tool_node)

    # 设置边
    graph.add_edge(START, "llm")

    # LLM -> 工具 或 LLM -> 结束
    graph.add_conditional_edges(
        "llm",
        lambda state: "tool" if (
            state.get("messages", []) and
            hasattr(state["messages"][-1], "tool_calls") and
            state["messages"][-1].tool_calls
        ) else END
    )

    # 工具 -> LLM
    graph.add_edge("tool", "llm")

    return graph.compile()


# 全局Agent实例
_agent = None


def get_agent():
    """获取Agent实例（单例）"""
    global _agent
    if _agent is None:
        _agent = create_agent()
    return _agent


def run_agent(user_message: str) -> str:
    """
    运行Agent处理用户消息
    Args:
        user_message: 用户输入的消息
    Returns:
        Agent的响应
    """

    agent = get_agent()

    # 构建输入
    input_data = {
        "messages": [HumanMessage(content=user_message)],
        "intent": "",
        "data_source": "",
        "query_params": {},
        "report_data": None,
        "report_path": "",
        "error": "",
        "action": ""
    }

    # 运行Agent
    result = agent.invoke(input_data)

    # 获取最后一条AI消息
    messages = result.get("messages", [])
    for msg in reversed(messages):
        if hasattr(msg, "content") and msg.content:
            if isinstance(msg, type(messages[0])):  # HumanMessage
                continue
            return msg.content

    return "处理完成，没有返回结果"
