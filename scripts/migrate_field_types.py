"""
数据库字段类型迁移脚本

将 transactions 和 b2c_order_charges 表的 VARCHAR 字段转换为正确的数值类型
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import db_config


def get_connection():
    return pymysql.connect(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        charset=db_config.charset,
        cursorclass=pymysql.cursors.DictCursor
    )


def migrate_transactions():
    """迁移 transactions 表的数值字段"""
    conn = get_connection()
    cursor = conn.cursor()

    migrations = [
        ('quantity', 'INT', 'SIGNED'),
        ('product_sales', 'DECIMAL(15,2)', 'REPLACE'),
        ('product_sales_tax', 'DECIMAL(15,2)', 'REPLACE'),
        ('postage_credits', 'DECIMAL(15,2)', 'REPLACE'),
        ('shipping_credits_tax', 'DECIMAL(15,2)', 'REPLACE'),
        ('gift_wrap_credits', 'DECIMAL(15,2)', 'REPLACE'),
        ('giftwrap_credits_tax', 'DECIMAL(15,2)', 'REPLACE'),
        ('regulatory_fee', 'DECIMAL(15,2)', 'REPLACE'),
        ('tax_on_regulatory_fee', 'DECIMAL(15,2)', 'REPLACE'),
        ('promotional_rebates', 'DECIMAL(15,2)', 'REPLACE'),
        ('promotional_rebates_tax', 'DECIMAL(15,2)', 'REPLACE'),
        ('marketplace_withheld_tax', 'DECIMAL(15,2)', 'REPLACE'),
        ('selling_fees', 'DECIMAL(15,2)', 'REPLACE'),
        ('fba_fees', 'DECIMAL(15,2)', 'REPLACE'),
        ('other_transaction_fees', 'DECIMAL(15,2)', 'REPLACE'),
        ('other', 'DECIMAL(15,2)', 'REPLACE'),
        ('total', 'DECIMAL(15,2)', 'REPLACE'),
    ]

    print("=== 迁移 transactions 表 ===")
    for field, target_type, transform in migrations:
        try:
            cursor.execute(f"SHOW COLUMNS FROM transactions WHERE Field = '{field}'")
            result = cursor.fetchone()
            if not result:
                print(f"  跳过 {field}: 字段不存在")
                continue

            current_type = result['Type']
            if 'decimal' in current_type.lower() or 'int' in current_type.lower():
                print(f"  跳过 {field}: 已是 {current_type}")
                continue

            if transform == 'REPLACE':
                alter_sql = f"""
                ALTER TABLE transactions
                MODIFY COLUMN {field} {target_type} DEFAULT 0
                """
            else:
                alter_sql = f"""
                ALTER TABLE transactions
                MODIFY COLUMN {field} {target_type} DEFAULT 0
                """

            cursor.execute(alter_sql)
            print(f"  ✓ {field}: {current_type} → {target_type}")
        except Exception as e:
            print(f"  ✗ {field}: {str(e)[:60]}")

    conn.commit()
    cursor.close()
    conn.close()


def migrate_b2c_order_charges():
    """迁移 b2c_order_charges 表的数值字段"""
    conn = get_connection()
    cursor = conn.cursor()

    migrations = [
        ('SKU数量__SKU_Quantity_', 'INT', 'SIGNED'),
        ('订单操作费__Handling_Fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('尾程运费__Rate_', 'DECIMAL(15,2)', 'REPLACE'),
        ('燃油附加费__Fuel_Surcharge_', 'DECIMAL(15,2)', 'REPLACE'),
        ('HGV__HGV_Surcharge_', 'DECIMAL(15,2)', 'REPLACE'),
        ('CO2排放费__CO2_Emission_', 'DECIMAL(15,2)', 'REPLACE'),
        ('高速公路费__Toll_Surcharge_', 'DECIMAL(15,2)', 'REPLACE'),
        ('包装材料费__Packing_Material_Fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('物流标签费__Shipping_label_fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('托盘占用费__Pallet_Fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('交通拥堵费__Congestion_', 'DECIMAL(15,2)', 'REPLACE'),
        ('超偏远附加费__Remote_', 'DECIMAL(15,2)', 'REPLACE'),
        ('超重超尺附加费__Overweight___Oversize_Charges_', 'DECIMAL(15,2)', 'REPLACE'),
        ('工时费__Labour_Fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('拦截服务费__Intercept_service_Fee_', 'DECIMAL(15,2)', 'REPLACE'),
        ('特殊附加费__Special_Surcharge_', 'DECIMAL(15,2)', 'REPLACE'),
        ('退件运费__退件运费_', 'DECIMAL(15,2)', 'REPLACE'),
        ('VAT税费__VAT_', 'DECIMAL(15,2)', 'REPLACE'),
        ('优惠总金额__Total_discount_amount_', 'DECIMAL(15,2)', 'REPLACE'),
    ]

    print("\n=== 迁移 b2c_order_charges 表 ===")
    for field, target_type, transform in migrations:
        try:
            cursor.execute(f"SHOW COLUMNS FROM b2c_order_charges WHERE Field = '{field}'")
            result = cursor.fetchone()
            if not result:
                print(f"  跳过 {field}: 字段不存在")
                continue

            current_type = result['Type']
            if 'decimal' in current_type.lower() or 'int' in current_type.lower():
                print(f"  跳过 {field}: 已是 {current_type}")
                continue

            alter_sql = f"""
            ALTER TABLE b2c_order_charges
            MODIFY COLUMN `{field}` {target_type} DEFAULT 0
            """

            cursor.execute(alter_sql)
            print(f"  ✓ {field}: {current_type} → {target_type}")
        except Exception as e:
            print(f"  ✗ {field}: {str(e)[:60]}")

    conn.commit()
    cursor.close()
    conn.close()


def add_indexes():
    """添加常用索引"""
    conn = get_connection()
    cursor = conn.cursor()

    indexes = [
        ("transactions", "idx_transactions_date", "date_time(10)"),
        ("transactions", "idx_transactions_type", "type"),
        ("transactions", "idx_transactions_sku", "Amazon_SKU(50)"),
        ("transactions", "idx_transactions_order", "order_id(50)"),
        ("b2c_order_charges", "idx_b2c_reference", "参考号__Reference_NO__(50)"),
        ("b2c_order_charges", "idx_b2c_billing_time", "计费时间__Billing_Time_(10)"),
    ]

    print("\n=== 添加索引 ===")
    for table, idx_name, idx_col in indexes:
        try:
            cursor.execute(f"CREATE INDEX {idx_name} ON {table}({idx_col})")
            print(f"  ✓ {table}.{idx_name}")
        except Exception as e:
            if 'Duplicate' in str(e):
                print(f"  - {table}.{idx_name}: 已存在")
            else:
                print(f"  ✗ {table}.{idx_name}: {str(e)[:50]}")

    conn.commit()
    cursor.close()
    conn.close()


def verify_migration():
    """验证迁移结果"""
    conn = get_connection()
    cursor = conn.cursor()

    print("\n=== 验证 transactions 表 ===")
    cursor.execute("DESCRIBE transactions")
    for row in cursor.fetchall():
        print(f"  {row['Field']}: {row['Type']}")

    print("\n=== 验证 b2c_order_charges 表 ===")
    cursor.execute("DESCRIBE b2c_order_charges")
    for row in cursor.fetchall():
        print(f"  {row['Field']}: {row['Type']}")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    print("=" * 60)
    print("数据库字段类型迁移")
    print("=" * 60)

    migrate_transactions()
    migrate_b2c_order_charges()
    add_indexes()
    verify_migration()

    print("\n" + "=" * 60)
    print("迁移完成！")
    print("=" * 60)
