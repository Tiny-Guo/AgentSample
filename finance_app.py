"""
Chainlit Web 界面入口 - 从根目录启动
"""
import chainlit as cl
import pandas as pd
from finance_agent.src.graph.agent import run_agent, get_agent
from finance_agent.src.graph.tools import TOOLS, generate_full_financial_report, generate_monthly_summary, calculate_profit_summary
from finance_agent.src.reports.generator import report_generator


@cl.on_chat_start
async def start():
    """聊天开始时的初始化"""
    await cl.Message(
        content="""# 财务数据分析助手

您好！我是您的财务数据分析助手，可以帮您：

1. **查询数据库** - 分析亚马逊交易数据
2. **读取文件** - Excel/CSV 文件分析
3. **生成报表** - 完整的财务报表和月度汇总
4. **利润分析** - 按SKU计算利润

---

## 【重要】SKU类型说明

本系统使用两种不同的SKU，请务必了解：

| 类型 | 字段名 | 说明 |
|------|--------|------|
| **亚马逊SKU** | `Amazon_SKU` / `sellerSku` | 亚马逊平台商品标识符，用于订单交易、佣金计算 |
| **产品SKU** | `产品SKU` | 公司内部产品标识符，用于成本核算、内部报表 |

两种SKU通过 `sku_mapping` 表关联。

---

**可用命令：**
- "生成财务报表" - 生成包含所有字段的完整报表（可下载）
- "生成月度报表" - 按月份汇总的数据（可下载）
- "查询数据库结构" - 查看数据库表和字段
- "读取xxx文件" - 分析特定文件
- "帮我分析利润" - 计算利润汇总

请告诉我您需要什么帮助？
"""
    ).send()


@cl.on_message
async def main(message: cl.Message):
    """处理用户消息"""
    user_input = message.content.strip()

    report_keywords = ['财务报表', '生成报表', '月度报表', '月报']
    profit_keywords = ['利润', '分析利润']

    if any(keyword in user_input for keyword in report_keywords):
        msg = cl.Message(content="正在生成报表，请稍候...")
        await msg.send()

        try:
            if '月度' in user_input or '月报' in user_input:
                df, filepath = report_generator.generate_monthly_report()
                report_type = "月度报表"
            else:
                df, filepath = report_generator.generate_full_report()
                report_type = "财务报表"

            elements = [cl.File(path=filepath, name=f"{report_type}.xlsx")]
            await cl.Message(
                content=f"✅ {report_type}生成成功！\n\n📥 点击下方按钮下载文件：\n\n**文件路径：** `{filepath}`",
                elements=elements
            ).send()

            preview_df = df.head(10)
            preview_msg = f"**{report_type}预览（前10行）：**\n\n"
            preview_msg += "| " + " | ".join(preview_df.columns.tolist()) + " |\n"
            preview_msg += "| " + " | ".join(["---"] * len(preview_df.columns)) + " |\n"

            for _, row in preview_df.iterrows():
                formatted_row = []
                for val in row:
                    if pd.isna(val):
                        formatted_row.append("")
                    elif isinstance(val, (int, float)):
                        if abs(val) >= 100 or val == 0:
                            formatted_row.append(f"{val:,.2f}")
                        else:
                            formatted_row.append(str(val))
                    else:
                        formatted_row.append(str(val))
                preview_msg += "| " + " | ".join(formatted_row) + " |\n"

            await cl.Message(content=preview_msg).send()

        except Exception as e:
            await cl.Message(content=f"❌ 生成报表失败: {str(e)}").send()

    elif any(keyword in user_input for keyword in profit_keywords):
        msg = cl.Message(content="正在计算利润分析，请稍候...")
        await msg.send()

        try:
            result = calculate_profit_summary.invoke({})
            await cl.Message(content=result).send()
        except Exception as e:
            await cl.Message(content=f"❌ 分析失败: {str(e)}").send()

    else:
        msg = cl.Message(content="正在处理您的请求...")
        await msg.send()

        try:
            response = run_agent(user_input)
            msg.content = response
            await msg.update()
        except Exception as e:
            msg.content = f"❌ 处理出错: {str(e)}"
            await msg.update()


if __name__ == "__main__":
    import os
    from chainlit.cli import run_chainlit

    # 检查是否已经在 Chainlit 环境中运行，避免重复启动
    if not os.environ.get("CHAINLIT_RUN"):
        # run_chainlit 会自动寻找当前文件并启动服务
        # 你可以在这里指定 port, host, headful(是否自动打开浏览器) 等参数
        run_chainlit(__file__)