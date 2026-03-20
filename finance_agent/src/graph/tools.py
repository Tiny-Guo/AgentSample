"""
LangGraph 工具定义
"""
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode
import os
import pandas as pd
from ..data.database import db
from ..data.file_handler import file_handler, bill_parser
from ..reports.generator import report_generator


@tool
def query_database(sql: str) -> str:
    """
    执行SQL查询并返回结果

    Args:
        sql: SQL查询语句

    Returns:
        查询结果的表格形式
    """
    try:
        df = db.query(sql)
        if df.empty:
            return "查询结果为空"

        # 限制返回行数
        if len(df) > 100:
            return f"查询结果共 {len(df)} 行:\n\n" + df.head(100).to_string()
        return f"查询结果共 {len(df)} 行:\n\n" + df.to_string()
    except Exception as e:
        return f"查询错误: {str(e)}"


@tool
def get_database_tables() -> str:
    """
    获取数据库中所有表的名称

    Returns:
        表名列表
    """
    try:
        tables = db.get_tables()
        return "数据库中的表:\n" + "\n".join([f"- {t}" for t in tables])
    except Exception as e:
        return f"获取表列表错误: {str(e)}"


@tool
def get_table_schema(table_name: str) -> str:
    """
    获取指定表的结构（列名和类型）

    Args:
        table_name: 表名

    Returns:
        表结构信息
    """
    try:
        df = db.get_table_schema(table_name)
        return f"表 {table_name} 的结构:\n\n" + df.to_string()
    except Exception as e:
        return f"获取表结构错误: {str(e)}"


@tool
def list_data_files(extension: str = None) -> str:
    """
    列出数据目录下的文件

    Args:
        extension: 文件扩展名过滤，如 '.xlsx', '.csv'

    Returns:
        文件列表
    """
    try:
        files = file_handler.list_files(extension)
        if not files:
            return "未找到文件"

        return "数据文件列表:\n" + "\n".join([f"- {f}" for f in files])
    except Exception as e:
        return f"获取文件列表错误: {str(e)}"


@tool
def read_excel_file(filepath: str, sheet_name: int = 0) -> str:
    """
    读取Excel文件内容

    Args:
        filepath: 文件路径
        sheet_name: 工作表名称或索引，默认0

    Returns:
        文件内容预览
    """
    try:
        df = file_handler.read_excel(filepath, sheet_name)
        if df.empty:
            return "文件为空"

        # 限制返回行数
        preview = df.head(20)
        info = f"文件: {filepath}\n"
        info += f"行数: {len(df)}, 列数: {len(df.columns)}\n"
        info += f"列名: {', '.join(df.columns.tolist())}\n\n"
        info += "前20行数据:\n" + preview.to_string()

        return info
    except Exception as e:
        return f"读取文件错误: {str(e)}"


@tool
def read_csv_file(filepath: str) -> str:
    """
    读取CSV文件内容

    Args:
        filepath: 文件路径

    Returns:
        文件内容预览
    """
    try:
        df = file_handler.read_csv(filepath)
        if df.empty:
            return "文件为空"

        preview = df.head(20)
        info = f"文件: {filepath}\n"
        info += f"行数: {len(df)}, 列数: {len(df.columns)}\n"
        info += f"列名: {', '.join(df.columns.tolist())}\n\n"
        info += "前20行数据:\n" + preview.to_string()

        return info
    except Exception as e:
        return f"读取文件错误: {str(e)}"


@tool
def generate_full_financial_report(start_date: str = None, end_date: str = None, year: int = None, month: int = None) -> str:
    """
    生成完整的财务报表，支持按时间筛选

    【时间筛选方式 - 优先使用日期范围】
    方式1：使用日期范围（推荐）
        start_date: 开始日期，格式 YYYY-MM-DD，如 "2025-11-01"
        end_date: 结束日期，格式 YYYY-MM-DD，如 "2026-01-31"
    方式2：使用年份+月份（单个月份）
        year: 筛选年份，如2025
        month: 筛选月份，如11

    【重要】时间筛选说明：
    - transactions表: 使用date_time字段筛选（格式如"31 Mar 2025 23:02:59 UTC"）
    - b2c_order_charges表: 使用计费时间_Billing_Time字段筛选（格式如"2025-03-12 09:57:18 GMT+00:00"）
    - import_date 字段不参与时间筛选

    【字段说明】

    | 字段名 | 数据来源 | 计算方式 |
    |--------|----------|----------|
    | sellerSku | transactions.Amazon_SKU | 直接取值 |
    | 产品信息 | transactions.description | 直接取值 |
    | 产品sku | products.产品SKU | 通过 sku_mapping 表关联 Amazon_SKU |
    | 站点 | transactions.marketplace | 直接取值 |
    | 店铺 | transactions.source_file | 直接取值 |
    | 销售状态 | products.销售状态 | 通过 sku_mapping 关联获取 |
    | 币种 | transactions.marketplace | amazon.com→USD, amazon.co.uk→GBP, amazon.de/fr/it/es→EUR 等 |
    | 销量 | transactions.quantity | type=Order 时按 Amazon_SKU 分组累加 |
    | FBM销量 | transactions.quantity | type=Order 且 fba_fees=0 时按 Amazon_SKU 分组累加 |
    | 退款量 | transactions.quantity | type=Refund 时按 Amazon_SKU 分组累加 |
    | FBM退款量 | transactions.quantity | type=Refund 且 fba_fees=0 时按 Amazon_SKU 分组累加 |
    | FBM销售额 | transactions.product_sales | type=Order 时按 Amazon_SKU 分组累加 |
    | FBM退款金额 | transactions.product_sales | type=Refund 且 fba_fees=0 时按 Amazon_SKU 分组累加 |
    | 其他调整收入 | transactions.total | type=Adjustment 时按 Amazon_SKU 分组累加 |
    | FBA销售佣金 | transactions.selling_fees | 全部求和 |
    | FBA配送费 | transactions.fba_fees | 全部求和 |
    | FBA配送费退款 | transactions.fba_fees | fba_fees<0 的求和 |
    | FBM配送费 | b2c_order_charges表 | 各项费用之和 |
    | 产品税 | transactions.product_sales_tax | 全部求和 |
    | 礼品包装税 | transactions.giftwrap_credits_tax | 全部求和 |
    | 商品成本 | products.采购价 | 根据 sku_mapping.product_sku 关联 |
    | 平台毛利 | transactions.total | 按 Amazon_SKU 分组累加 |

    Returns:
        报表文件路径和数据预览
    """
    try:
        # 根据时间过滤生成报表
        time_desc = ""
        if start_date and end_date:
            time_desc = f"{start_date} 至 {end_date}"
        elif year and month:
            time_desc = f"{year}年{month}月"
        elif year:
            time_desc = f"{year}年"

        df, filepath = report_generator.generate_full_report(start_date=start_date, end_date=end_date, year=year, month=month)

        if df.empty:
            return f"未找到{time_desc}的财务报表数据"

        # 计算汇总
        summary_cols = ['销量', 'FBM销量', '退款量', 'FBM退款量', 'FBM销售额', 'FBM退款金额',
                       '其他调整收入', 'FBA销售佣金平台其他收入汇总', 'FBA配送费', 'FBA配送费退款',
                       'FBM配送费', '产品税', '礼品包装税', '商品成本', '平台毛利']

        result = f"财务报表生成成功！\n\n"
        result += f"时间范围: {time_desc if time_desc else '全部'}\n"
        result += f"文件路径: {filepath}\n\n"
        result += "【汇总信息】\n"
        result += f"- 总SKU数: {len(df)}\n"

        for col in summary_cols:
            if col in df.columns:
                val = df[col].sum()
                if isinstance(val, (int, float)):
                    result += f"- {col}: {val:,.2f}\n"
                else:
                    result += f"- {col}: {val}\n"

        # 添加净利润汇总
        if '净利润' in df.columns:
            result += f"- 净利润: {df['净利润'].sum():,.2f}\n"

        result += f"\n【报表预览 - 前10行】\n"
        result += df.head(10).to_string()

        return result
    except Exception as e:
        return f"生成报表错误: {str(e)}"


@tool
def generate_monthly_summary(year: int = None, month: int = None) -> str:
    """
    生成月度汇总报表

    Args:
        year: 年份，如2025
        month: 月份，如4

    Returns:
        月度报表数据
    """
    try:
        df, filepath = report_generator.generate_monthly_report(year, month)

        result = "月度报表生成成功！\n\n"
        result += f"文件路径: {filepath}\n\n"
        result += f"报表内容:\n{df.to_string()}"

        return result
    except Exception as e:
        return f"生成月度报表错误: {str(e)}"


@tool
def calculate_profit_summary(year: int = None, month: int = None) -> str:
    """
    计算利润汇总，支持按时间筛选

    Args:
        year: 筛选年份，如2025
        month: 筛选月份，如11

    返回按SKU分组的利润汇总数据

    Returns:
        利润汇总报表
    """
    # 时间过滤条件 - date_time 格式如 "31 Mar 2025 23:02:59 UTC"
    date_filter = ""
    if year:
        date_filter += f" AND t.date_time LIKE '%{year}%'"
        if month:
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_name = month_names.get(month, '')
            if month_name:
                date_filter += f" AND t.date_time LIKE '%{month_name}%'"

    try:
        time_desc = ""
        if year and month:
            time_desc = f"{year}年{month}月"
        elif year:
            time_desc = f"{year}年"

        query = f"""
        SELECT
            t.Amazon_SKU AS sellerSku,
            p.产品SKU,
            SUM(CASE WHEN t.type = 'Order' THEN t.product_sales ELSE 0 END) AS 销售额,
            SUM(t.total) AS 平台毛利,
            COALESCE(min_cost.采购价, 0) AS 商品成本
        FROM transactions t
        LEFT JOIN sku_mapping sm ON t.Amazon_SKU = sm.seller_sku
        LEFT JOIN products p ON sm.product_sku = p.产品SKU
        LEFT JOIN (
            SELECT 产品SKU, 采购价
            FROM products
            WHERE 采购价 IS NOT NULL
        ) min_cost ON p.产品SKU = min_cost.产品SKU
        WHERE 1=1 {date_filter}
        GROUP BY t.Amazon_SKU, p.产品SKU
        ORDER BY 销售额 DESC
        LIMIT 50
        """
        df = db.query(query)

        if df.empty:
            return f"未找到{time_desc}的利润汇总数据"

        # 计算利润
        df['预估利润'] = df['平台毛利'] - df['商品成本']

        result = f"利润汇总报表 (Top 50 SKU) - {time_desc if time_desc else '全部'}\n\n"
        result += df.to_string()

        # 汇总
        result += f"\n\n汇总:"
        result += f"\n  总销售额: {df['销售额'].sum():,.2f}"
        result += f"\n  总平台毛利: {df['平台毛利'].sum():,.2f}"
        result += f"\n  总商品成本: {df['商品成本'].sum():,.2f}"
        result += f"\n  总预估利润: {df['预估利润'].sum():,.2f}"

        return result
    except Exception as e:
        return f"计算利润汇总错误: {str(e)}"


@tool
def extract_1510_bill_summary() -> str:
    """
    提取所有1510账单封面费用汇总

    从 data/财务账单/海外仓账单（各自建立不同表-混合文件）/1510 目录下
    读取所有账单文件，提取账单封面中的关键费用信息。

    提取内容：
    - 账单编号
    - 客户名称
    - 费用周期
    - 入库单费用(Inbound Charges)
    - 仓租费用(Storage Charges)
    - 其他费用(Other Charges)

    Returns:
        账单汇总表格 + Excel文件路径
    """
    try:
        # 解析所有1510账单
        df = bill_parser.parse_all_bills()

        if df.empty:
            return "未找到1510账单文件或解析失败"

        # 选择需要显示的列
        display_cols = ['账单编号', '费用周期', '入库单费用', '仓租费用', '其他费用', '客户名称']
        display_cols = [c for c in display_cols if c in df.columns]

        # 计算汇总
        summary = {
            '入库单费用': df['入库单费用'].sum() if '入库单费用' in df.columns else 0,
            '仓租费用': df['仓租费用'].sum() if '仓租费用' in df.columns else 0,
            '其他费用': df['其他费用'].sum() if '其他费用' in df.columns else 0,
        }

        # 导出到Excel
        output_path = bill_parser.export_to_excel(df)

        # 格式化结果
        result = "=== 1510账单封面汇总 ===\n\n"
        result += f"共找到 {len(df)} 个账单文件\n\n"

        # 汇总信息
        result += "【费用汇总】\n"
        result += f"- 总入库单费用: {summary['入库单费用']:,.2f}\n"
        result += f"- 总仓租费用: {summary['仓租费用']:,.2f}\n"
        result += f"- 总其他费用: {summary['其他费用']:,.2f}\n"
        result += f"- 费用总计: {sum(summary.values()):,.2f}\n\n"

        # 账单明细表格
        result += "【账单明细】\n"
        result += df[display_cols].to_string(index=False)

        result += f"\n\n已导出至: {output_path}"

        return result
    except Exception as e:
        return f"提取1510账单汇总错误: {str(e)}"


@tool
def extract_single_1510_bill(filepath: str = None) -> str:
    """
    提取指定1510账单的封面费用

    Args:
        filepath: 账单文件路径（如不提供则列出所有可用账单）

    Returns:
        该账单的封面费用详情
    """
    try:
        # 如果没有提供路径，列出所有可用账单
        if filepath is None or filepath.strip() == "":
            files = bill_parser.list_bill_files()
            if not files:
                return "未找到1510账单文件"

            result = "可用账单文件列表:\n\n"
            for i, f in enumerate(files, 1):
                result += f"{i}. {os.path.basename(f)}\n"
            result += "\n请提供完整的账单文件路径"
            return result

        # 解析指定账单
        if not os.path.exists(filepath):
            return f"文件不存在: {filepath}"

        bill_data = bill_parser.parse_bill_cover(filepath)

        if '错误' in bill_data:
            return f"解析账单失败: {bill_data['错误']}"

        # 格式化结果
        result = "=== 账单封面信息 ===\n\n"
        result += f"文件: {bill_data.get('文件名', '')}\n\n"

        result += "【基本信息】\n"
        result += f"- 账单编号: {bill_data.get('账单编号', 'N/A')}\n"
        result += f"- 客户名称: {bill_data.get('客户名称', 'N/A')}\n"
        result += f"- 费用周期: {bill_data.get('费用周期', 'N/A')}\n\n"

        result += "【费用信息】\n"
        result += f"- 入库单费用: {bill_data.get('入库单费用', 0):,.2f}\n"
        result += f"- 仓租费用: {bill_data.get('仓租费用', 0):,.2f}\n"
        result += f"- 其他费用: {bill_data.get('其他费用', 0):,.2f}\n"

        total = (bill_data.get('入库单费用', 0) +
                 bill_data.get('仓租费用', 0) +
                 bill_data.get('其他费用', 0))
        result += f"- 费用总计: {total:,.2f}\n"

        return result
    except Exception as e:
        return f"提取账单信息错误: {str(e)}"


@tool
def get_data_time_range() -> str:
    """
    查询数据库中实际的数据时间范围

    返回数据库中 transactions 表的时间范围信息，帮助AI在生成报表时使用正确的时间过滤。

    【重要说明】
    - date_time 字段：亚马逊原始交易日期，用于按交易时间筛选
    - import_date 字段：数据导入到系统的时间，不用于交易时间筛选

    Returns:
        数据库时间范围信息，包括各月份数据量
    """
    try:
        # 查询 date_time 字段的时间范围
        query1 = """
        SELECT
            MIN(t.date_time) AS earliest_transaction,
            MAX(t.date_time) AS latest_transaction,
            COUNT(CASE WHEN t.date_time IS NOT NULL AND t.date_time != '' THEN 1 END) AS records_with_date
        FROM transactions t
        """
        result1 = db.query(query1)

        # 查询 import_date 字段的时间范围
        query2 = """
        SELECT
            MIN(import_date) AS earliest_import,
            MAX(import_date) AS latest_import,
            COUNT(*) AS total_records
        FROM transactions
        """
        result2 = db.query(query2)

        # 统计各年份数据量
        query3 = """
        SELECT
            SUBSTRING_INDEX(SUBSTRING_INDEX(t.date_time, ' ', 3), ' ', -1) AS year,
            COUNT(*) AS record_count
        FROM transactions t
        WHERE t.date_time IS NOT NULL AND t.date_time != ''
        GROUP BY year
        ORDER BY year DESC
        """
        result3 = db.query(query3)

        # 查询 transactions 表的年月统计（分三种格式分别查询，然后在Python中合并）
        month_map = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
                     'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}

        # 格式1: "31 Mar 2025" 或 "1 Apr 2025" (D Mon YYYY)
        query4a = """
        SELECT
            SUBSTRING_INDEX(SUBSTRING_INDEX(date_time, ' ', 3), ' ', -1) as yr,
            SUBSTRING_INDEX(SUBSTRING_INDEX(date_time, ' ', 2), ' ', -1) as mo,
            COUNT(*) as cnt
        FROM transactions
        WHERE date_time REGEXP '^[0-9]+\\ [A-Z][a-z]{2}\\ [0-9]+'
        GROUP BY yr, mo
        """
        df4a = db.query(query4a)

        # 格式2: "Apr 1, 2025" (Mon D, YYYY)
        query4b = """
        SELECT
            REGEXP_SUBSTR(date_time, '[0-9]{4}') as yr,
            SUBSTRING_INDEX(date_time, ' ', 1) as mo,
            COUNT(*) as cnt
        FROM transactions
        WHERE date_time REGEXP '^[A-Z][a-z]{2}\\ [0-9]+,'
        GROUP BY yr, mo
        """
        df4b = db.query(query4b)

        # 格式3: "01.05.2025" (DD.MM.YYYY)
        query4c = """
        SELECT
            SUBSTRING(date_time, 7, 4) as yr,
            CASE SUBSTRING(date_time, 4, 2)
                WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
                WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
                WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
                WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
            END as mo,
            COUNT(*) as cnt
        FROM transactions
        WHERE date_time REGEXP '^[0-9]+\\.[0-9]+\\.[0-9]+'
        GROUP BY yr, mo
        """
        df4c = db.query(query4c)

        # 合并结果
        for df in [df4a, df4b, df4c]:
            if 'mo' in df.columns:
                df['mo'] = df['mo'].map(month_map)

        all_monthly = pd.concat([df4a, df4b, df4c])
        all_monthly['year_month'] = all_monthly['yr'].astype(str) + '-' + all_monthly['mo'].astype(str)
        monthly_stats = all_monthly.groupby('year_month')['cnt'].sum().reset_index()
        monthly_stats = monthly_stats.sort_values('year_month', ascending=False)

        # 统计B2C费用表各月份数据量
        query5 = """
        SELECT
            LEFT(计费时间_Billing_Time, 7) AS ym,
            COUNT(*) AS record_count
        FROM b2c_order_charges
        GROUP BY ym
        ORDER BY ym DESC
        """
        result5 = db.query(query5)

        result = "=== 数据库时间范围信息 ===\n\n"

        result += "【transactions交易表 - 数据时间范围】\n"
        if not result1.empty:
            result += f"- 最早交易: {result1['earliest_transaction'].iloc[0]}\n"
            result += f"- 最晚交易: {result1['latest_transaction'].iloc[0]}\n"
            result += f"- 有交易日期的记录数: {result1['records_with_date'].iloc[0]}\n"

        result += "\n【transactions交易表 - 各月份数据量】\n"
        if not monthly_stats.empty:
            result += "格式: 年月 - 记录数\n"
            for _, row in monthly_stats.iterrows():
                result += f"- {row['year_month']}: {int(row['cnt'])} 条\n"

        result += "\n【b2c_order_charges费用表 - 各月份数据量】\n"
        if not result5.empty:
            result += "格式: 年月 - 记录数\n"
            for _, row in result5.iterrows():
                result += f"- {row['ym']}: {row['record_count']} 条\n"

        result += "\n【数据导入日期 (import_date字段)】\n"
        if not result2.empty:
            result += f"- 最早导入: {result2['earliest_import'].iloc[0]}\n"
            result += f"- 最晚导入: {result2['latest_import'].iloc[0]}\n"
            result += f"- 总记录数: {result2['total_records'].iloc[0]}\n"

        result += "\n【使用提示】\n"
        result += "生成报表时：\n"
        result += "1. 请使用 date_time 字段的时间作为筛选条件（不是 import_date）\n"
        result += "2. 如果用户请求的时间范围超出可用数据，\n"
        result += "   - 告知用户哪些时间段没有数据\n"
        result += "   - 自动使用存在的可用数据进行计算\n"
        result += "   - 在报表中标注实际使用的数据范围\n"
        result += "3. 时间格式：\n"
        result += "   - transactions表: '31 Mar 2025' -> 使用月份名称过滤\n"
        result += "   - b2c表: '2025-03-12' -> 使用 YYYY-MM 格式过滤\n"

        return result
    except Exception as e:
        return f"查询时间范围错误: {str(e)}"


# 工具列表
TOOLS = [
    query_database,
    get_database_tables,
    get_table_schema,
    get_data_time_range,
    list_data_files,
    read_excel_file,
    read_csv_file,
    generate_full_financial_report,
    generate_monthly_summary,
    calculate_profit_summary,
    extract_1510_bill_summary,
    extract_single_1510_bill,
]

# 创建ToolNode
tool_node = ToolNode(TOOLS)
