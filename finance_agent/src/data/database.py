"""
MySQL 数据库连接和查询
"""
import pymysql
import pandas as pd
from typing import Optional
from ..config import db_config


class Database:
    """数据库连接管理器"""

    def __init__(self):
        self.connection: Optional[pymysql.Connection] = None

    def connect(self) -> pymysql.Connection:
        """建立数据库连接"""
        if self.connection is None or not self.connection.open:
            self.connection = pymysql.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.user,
                password=db_config.password,
                database=db_config.database,
                charset=db_config.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
        return self.connection

    def close(self):
        """关闭数据库连接"""
        if self.connection and self.connection.open:
            self.connection.close()
            self.connection = None

    def query(self, sql: str, params: tuple = None) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                results = cursor.fetchall()
                return pd.DataFrame(results)
        finally:
            pass  # 保持连接以便后续使用

    def execute(self, sql: str, params: tuple = None) -> int:
        """执行INSERT/UPDATE/DELETE并返回影响的行数"""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                result = cursor.execute(sql, params)
                conn.commit()
                return result
        finally:
            pass

    def get_tables(self) -> list:
        """获取所有表名"""
        df = self.query("SHOW TABLES")
        return df.iloc[:, 0].tolist()

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """获取表结构"""
        return self.query(f"SHOW COLUMNS FROM {table_name}")


# 全局数据库实例
db = Database()


def get_full_report_query(start_date: str = None, end_date: str = None,
                         year: int = None, month: int = None) -> str:
    """
    获取完整报表的SQL查询

    Args:
        start_date: 开始日期，格式 YYYY-MM-DD，如 "2025-11-01"
        end_date: 结束日期，格式 YYYY-MM-DD，如 "2026-01-31"
        year: 筛选年份，如2025（与日期范围二选一）
        month: 筛选月份，如11

    Returns:
        SQL查询语句

    【时间字段格式说明】
    - transactions.date_time: 格式如 "31 Mar 2025 23:02:59 UTC"
    - b2c_order_charges.计费时间_Billing_Time: 格式如 "2025-03-12 09:57:18 GMT+00:00"
    """
    # 构建 transactions 表的时间过滤条件
    date_filter = ""
    if start_date and end_date:
        # 使用日期范围过滤
        # 需要将 YYYY-MM-DD 转换为月份名称进行过滤
        start_yr, start_mo, _ = start_date.split('-')
        end_yr, end_mo, _ = end_date.split('-')

        # 生成月份列表
        months_list = []
        current_year, current_month = int(start_yr), int(start_mo)
        target_end_year, target_end_month = int(end_yr), int(end_mo)

        month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                      7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}

        while (current_year < target_end_year) or (current_year == target_end_year and current_month <= target_end_month):
            months_list.append((current_year, current_month))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        if months_list:
            conditions = []
            for y, m in months_list:
                # 格式是 "Nov 2025"，所以顺序是 月份 年份
                conditions.append(f"t.date_time LIKE '%{month_names[m]}%{y}%'")
            month_conditions = " OR ".join(conditions)
            date_filter = f" AND ({month_conditions})"
    elif year:
        date_filter += f" AND t.date_time LIKE '%{year}%'"
        if month:
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_name = month_names.get(month, '')
            if month_name:
                date_filter += f" AND t.date_time LIKE '%{month_name}%'"

    # B2C费用时间过滤 - 使用相同逻辑
    b2c_date_filter = ""
    if start_date and end_date:
        start_yr, start_mo, _ = start_date.split('-')
        end_yr, end_mo, _ = end_date.split('-')

        months_list = []
        current_year, current_month = int(start_yr), int(start_mo)
        target_end_year, target_end_month = int(end_yr), int(end_mo)

        while (current_year < target_end_year) or (current_year == target_end_year and current_month <= target_end_month):
            months_list.append(f"{current_year}-{current_month:02d}%")
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        if months_list:
            month_conditions = " OR ".join([f"bc.`计费时间_Billing_Time` LIKE '{m}%'" for m in months_list])
            b2c_date_filter = f" AND ({month_conditions})"
    elif year:
        b2c_date_filter += f" AND bc.`计费时间_Billing_Time` LIKE '{year}%'"
        if month:
            b2c_date_filter += f" AND bc.`计费时间_Billing_Time` LIKE '{year}-{month:02d}%'"

    return f"""
    SELECT
        t.Amazon_SKU AS sellerSku,
        t.description AS 产品信息,
        p.产品SKU AS 产品sku,
        t.marketplace AS 站点,
        t.source_file AS 店铺,
        p.销售状态,
        CASE t.marketplace
            WHEN 'amazon.com' THEN 'USD'
            WHEN 'amazon.co.uk' THEN 'GBP'
            WHEN 'amazon.de' THEN 'EUR'
            WHEN 'amazon.fr' THEN 'EUR'
            WHEN 'amazon.it' THEN 'EUR'
            WHEN 'amazon.es' THEN 'EUR'
            WHEN 'amazon.co.jp' THEN 'JPY'
            WHEN 'amazon.com.au' THEN 'AUD'
            ELSE 'USD'
        END AS 币种,

        -- 销量 (type=Order 按SKU累加)
        SUM(CASE WHEN t.type = 'Order' THEN t.quantity ELSE 0 END) AS 销量,

        -- FBM销量 (type=Order 且 fba_fees=0)
        SUM(CASE WHEN t.type = 'Order' AND (t.fba_fees IS NULL OR t.fba_fees = 0)
            THEN t.quantity ELSE 0 END) AS FBM销量,

        -- 退款量 (type=Refund 按SKU累加)
        SUM(CASE WHEN t.type = 'Refund' THEN t.quantity ELSE 0 END) AS 退款量,

        -- FBM退款量 (type=Refund 且 fba_fees=0)
        SUM(CASE WHEN t.type = 'Refund' AND (t.fba_fees IS NULL OR t.fba_fees = 0)
            THEN t.quantity ELSE 0 END) AS FBM退款量,

        -- FBM销售额 (type=Order)
        SUM(CASE WHEN t.type = 'Order' THEN t.product_sales ELSE 0 END) AS FBM销售额,

        -- FBM退款金额 (type=Refund 且 fba_fees=0)
        SUM(CASE WHEN t.type = 'Refund' AND (t.fba_fees IS NULL OR t.fba_fees = 0)
            THEN t.product_sales ELSE 0 END) AS FBM退款金额,

        -- 其他调整收入 (type=Adjustment)
        SUM(CASE WHEN t.type = 'Adjustment' THEN t.total ELSE 0 END) AS 其他调整收入,

        -- FBA销售佣金
        SUM(t.selling_fees) AS FBA销售佣金平台其他收入汇总,

        -- FBA配送费
        SUM(t.fba_fees) AS FBA配送费,

        -- FBA配送费退款 (fba_fees < 0)
        SUM(CASE WHEN t.fba_fees < 0 THEN t.fba_fees ELSE 0 END) AS FBA配送费退款,

        -- FBM配送费 (从b2c_order_charges表获取)
        COALESCE(SUM(bc.订单操作费_Handling_Fee + bc.尾程运费_Rate + bc.燃油附加费_Fuel_Surcharge +
            bc.HGV_HGV_Surcharge + bc.CO2排放费_CO2_Emission + bc.高速公路费_Toll_Surcharge +
            bc.包装材料费_Packing_Material_Fee + bc.物流标签费_Shipping_label_fee +
            bc.托盘占用费_Pallet_Fee + bc.交通拥堵费_Congestion + bc.超偏远附加费_Remote +
            bc.超重超尺附加费_Overweight_Oversize_Charges + bc.工时费_Labour_Fee +
            bc.拦截服务费_Intercept_service_Fee + bc.特殊附加费_Special_Surcharge +
            bc.VAT税费_VAT + bc.优惠总金额_Total_discount_amount), 0) AS FBM配送费,

        -- 产品税
        SUM(t.product_sales_tax) AS 产品税,

        -- 礼品包装税
        SUM(t.giftwrap_credits_tax) AS 礼品包装税,

        -- 平台毛利
        SUM(t.total) AS 平台毛利,

        -- 时间范围
        MIN(t.date_time) AS 开始时间,
        MAX(t.date_time) AS 结束时间

    FROM transactions t
    LEFT JOIN sku_mapping sm ON t.Amazon_SKU = sm.seller_sku
    LEFT JOIN products p ON sm.product_sku = p.产品SKU
    LEFT JOIN order_reference_mapping orm ON t.order_id = orm.order_id
    LEFT JOIN b2c_order_charges bc ON orm.reference_no = bc.参考号_Reference_NO {b2c_date_filter}
    WHERE 1=1 {date_filter}
    GROUP BY t.Amazon_SKU, t.description, p.产品SKU, t.marketplace, t.source_file, p.销售状态
    ORDER BY t.Amazon_SKU
    """


def get_monthly_report_query(year: int = None, month: int = None) -> str:
    """
    获取月度汇总报表的SQL查询

    Args:
        year: 筛选年份，如2025
        month: 筛选月份，如11

    Returns:
        SQL查询语句

    【时间字段格式】: "31 Mar 2025 23:02:59 UTC"
    """
    # 时间过滤条件 - date_time 格式如 "31 Mar 2025"
    date_filter = ""
    if year:
        date_filter += f" AND t.date_time LIKE '%{year}%'"
        if month:
            month_names = {1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
                          7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
            month_name = month_names.get(month, '')
            if month_name:
                date_filter += f" AND t.date_time LIKE '%{month_name}%'"

    # 提取年月 - 格式 "31 Mar 2025 23:02:59 UTC" -> "2025-03"
    # 日期格式：日 月 年 时:分:秒 时区
    # 所以用第3段获取年，第2段获取月
    month_sql = """
        CONCAT(
            SUBSTRING_INDEX(SUBSTRING_INDEX(t.date_time, ' ', 3), ' ', -1),
            '-',
            CASE SUBSTRING_INDEX(SUBSTRING_INDEX(t.date_time, ' ', 2), ' ', -1)
                WHEN 'Jan' THEN '01'
                WHEN 'Feb' THEN '02'
                WHEN 'Mar' THEN '03'
                WHEN 'Apr' THEN '04'
                WHEN 'May' THEN '05'
                WHEN 'Jun' THEN '06'
                WHEN 'Jul' THEN '07'
                WHEN 'Aug' THEN '08'
                WHEN 'Sep' THEN '09'
                WHEN 'Oct' THEN '10'
                WHEN 'Nov' THEN '11'
                WHEN 'Dec' THEN '12'
            END
        )
    """

    return f"""
    SELECT
        {month_sql} AS 月份,
        t.marketplace AS 站点,
        COUNT(DISTINCT t.order_id) AS 订单数,
        SUM(CASE WHEN t.type = 'Order' THEN t.quantity ELSE 0 END) AS 销量,
        SUM(CASE WHEN t.type = 'Refund' THEN t.quantity ELSE 0 END) AS 退款量,
        SUM(CASE WHEN t.type = 'Order' THEN t.product_sales ELSE 0 END) AS 销售额,
        SUM(t.total) AS 平台毛利
    FROM transactions t
    WHERE 1=1 {date_filter}
    GROUP BY 月份, 站点
    ORDER BY 月份 DESC, 站点
    """
