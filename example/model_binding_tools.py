from langchain.tools import tool
from langchain_community.chat_models import ChatZhipuAI
import os
os.environ["ZHIPUAI_API_KEY"] = "8ec5f04880274934988dde0d34fa37ee.NU6GuPWl5DJ48hfT"

model = ChatZhipuAI(
    model="glm-4",
    temperature=0.5
)


# Define tools
@tool
def multiply(a: int, b: int) -> int:
    """Multiply `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a * b


@tool
def add(a: int, b: int) -> int:
    """Adds `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a + b


@tool
def divide(a: int, b: int) -> float:
    """Divide `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a / b


# Augment the LLM with tools
tools = [add, multiply, divide]
#   - 键：工具的名称（如 "add", "multiply", "divide"）
#   - 值：对应的工具对象
tools_by_name = {tool.name: tool for tool in tools}
model_with_tools = model.bind_tools(tools)