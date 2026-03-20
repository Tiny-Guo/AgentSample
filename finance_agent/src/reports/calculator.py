"""
利润计算模块
"""
import pandas as pd
from ..data.database import db


def calculate_profit(report_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算利润相关字段

    利润 = 销售收入 - 平台费用 - 物流成本 - 产品成本
    """
    df = report_df.copy()

    # 平台毛利 = FBM销售额 - FBM退款金额 + 其他调整收入 + FBA销售佣金 + FBA配送费 + FBA配送费退款
    if 'FBM销售额' in df.columns and 'FBM退款金额' in df.columns:
        df['平台毛利'] = (
            df['FBM销售额'].fillna(0) -
            df['FBM退款金额'].fillna(0) +
            df.get('其他调整收入', pd.Series([0]*len(df))).fillna(0) +
            df.get('FBA销售佣金', pd.Series([0]*len(df))).fillna(0) +
            df.get('FBA配送费', pd.Series([0]*len(df))).fillna(0) +
            df.get('FBA配送费退款', pd.Series([0]*len(df))).fillna(0)
        )

    return df


def get_product_cost() -> pd.DataFrame:
    """获取商品成本（采购价）- 只返回必要的列避免重复"""
    query = """
    SELECT
        sku_mapping.seller_sku,
        products.`采购价`
    FROM sku_mapping
    LEFT JOIN products ON sku_mapping.product_sku = products.`产品SKU`
    WHERE products.`采购价` IS NOT NULL AND products.`采购价` > 0
    """
    return db.query(query)


def add_product_cost(report_df: pd.DataFrame) -> pd.DataFrame:
    """为报表添加商品成本"""
    cost_df = get_product_cost()
    df = report_df.copy()

    # 检查是否有数据
    if cost_df.empty:
        df['商品成本'] = 0
        return df

    # 重命名避免与报表列冲突
    cost_df = cost_df.rename(columns={'seller_sku': 'seller_sku_temp'})

    # 合并商品成本
    df = df.merge(
        cost_df[['seller_sku_temp', '采购价']],
        left_on='sellerSku',
        right_on='seller_sku_temp',
        how='left'
    )
    df = df.rename(columns={'采购价': '商品成本'})
    df = df.drop(columns=['seller_sku_temp'], errors='ignore')

    # 填充空值为0
    df['商品成本'] = df['商品成本'].fillna(0)

    return df


def get_fbm_shipping_cost(order_ids: list = None) -> pd.DataFrame:
    """获取FBM配送费（来自b2c_order_charges）"""
    # b2c_order_charges 的列名映射
    fbm_cost_columns = [
        '订单操作费__Handling_Fee_',
        '尾程运费__Rate_',
        '燃油附加费__Fuel_Surcharge_',
        'HGV__HGV_Surcharge_',
        'CO2排放费__CO2_Emission_',
        '高速公路费__Toll_Surcharge_',
        '包装材料费__Packing_Material_Fee_',
        '物流标签费__Shipping_label_fee_',
        '托盘占用费__Pallet_Fee_',
        '交通拥堵费__Congestion_',
        '超偏远附加费__Remote_',
        '超重超尺附加费__Overweight___Oversize_Charges_',
        '工时费__Labour_Fee_',
        '拦截服务费__Intercept_service_Fee_',
        '特殊附加费__Special_Surcharge_',
        'VAT税费__VAT_',
        '优惠总金额__Total_discount_amount_'
    ]

    if order_ids:
        placeholders = ','.join(['%s'] * len(order_ids))
        col_expr = ',\n            '.join([
            f'COALESCE(CAST(REPLACE(b.`{col}`, ",", "") AS DECIMAL(15,2)), 0) AS `{col}`'
            for col in fbm_cost_columns
        ])
        total_expr = ' + '.join([
            f'COALESCE(CAST(REPLACE(b.`{col}`, ",", "") AS DECIMAL(15,2)), 0)'
            for col in fbm_cost_columns
        ])
        query = f"""
        SELECT
            b.`参考号__Reference_NO__`,
            {col_expr},
            ({total_expr}) AS 总费用
        FROM b2c_order_charges b
        WHERE b.`参考号__Reference_NO__` IN ({placeholders})
        """
        return db.query(query, tuple(order_ids))
    else:
        col_expr = ',\n        '.join([
            f'COALESCE(CAST(REPLACE(b.`{col}`, ",", "") AS DECIMAL(15,2)), 0) AS `{col}`'
            for col in fbm_cost_columns
        ])
        total_expr = ' + '.join([
            f'COALESCE(CAST(REPLACE(b.`{col}`, ",", "") AS DECIMAL(15,2)), 0)'
            for col in fbm_cost_columns
        ])
        query = f"""
        SELECT
            b.`参考号__Reference_NO__`,
            {col_expr},
            ({total_expr}) AS 总费用
        FROM b2c_order_charges b
        """
        return db.query(query)
