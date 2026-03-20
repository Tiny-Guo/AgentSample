# Build workflow
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from IPython.display import Image, display
from node import llm_call, tool_node, should_continue
from state import MessagesState
from langchain.messages import HumanMessage


agent_builder = StateGraph(MessagesState)

# Add nodes
agent_builder.add_node("llm_call", llm_call)
agent_builder.add_node("tool_node", tool_node)

# Add edges to connect nodes
agent_builder.add_edge(START, "llm_call")
agent_builder.add_conditional_edges(
    "llm_call",
    should_continue,
    ["tool_node", END]
)
agent_builder.add_edge("tool_node", "llm_call")

# Compile the agent
agent = agent_builder.compile()

# Show the agent
graph = agent.get_graph(xray=True)
png_data = graph.draw_mermaid_png()

# Save to file
with open('agent_graph.png', 'wb') as f:
    f.write(png_data)

print("Graph saved to agent_graph.png")


# Invoke
messages = [HumanMessage(content="Add 3 and 4.")]
messages = agent.invoke({"messages": messages})
for m in messages["messages"]:
    m.pretty_print()