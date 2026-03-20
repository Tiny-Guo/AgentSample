import os
import sys
import csv
import pymysql
import re
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, db_config

BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', '部分店铺收入', '亚马逊按月份')
DB_NAME = db_config.database
TABLE_NAME = 'transactions'

# 列名映射表（统一不同格式的列名）
# 格式: 原始列名小写 -> 统一列名
COLUMN_MAPPING = {
    # 英文列名标准化
    'date/time': 'date_time',
    'settlement id': 'settlement_id',
    'order id': 'order_id',
    'sku': 'Amazon_SKU',
    'order city': 'order_city',
    'order state': 'order_state',
    'order postal': 'order_postal',
    'tax collection model': 'tax_collection_model',
    'product sales': 'product_sales',
    'product sales tax': 'product_sales_tax',
    'shipping credits': 'postage_credits',
    'shipping credits tax': 'shipping_credits_tax',
    'gift wrap credits': 'gift_wrap_credits',
    'giftwrap credits tax': 'giftwrap_credits_tax',
    'regulatory fee': 'regulatory_fee',
    'tax on regulatory fee': 'tax_on_regulatory_fee',
    'promotional rebates': 'promotional_rebates',
    'promotional rebates tax': 'promotional_rebates_tax',
    'marketplace withheld tax': 'marketplace_withheld_tax',
    'selling fees': 'selling_fees',
    'fba fees': 'fba_fees',
    'other transaction fees': 'other_transaction_fees',
    # 美式/英式拼写统一
    'fulfilment': 'fulfilment',
    'fulfillment': 'fulfilment',
    'fulfil': 'fulfilment',
    'fulfill': 'fulfilment',
    # 德文列名映射
    'datum/uhrzeit': 'date_time',
    'abrechnungsnummer': 'settlement_id',
    'bestellnummer': 'order_id',
    'marktplatz': 'marketplace',
    'menge': 'quantity',
    'beschreibung': 'description',
    'amazon_sku': 'Amazon_SKU',
    'typ': 'type',
}

# 统一列顺序 (匹配现有表结构)
UNIFIED_COLUMNS = [
    'date_time', 'settlement_id', 'type', 'order_id', 'Amazon_SKU', 'description',
    'quantity', 'marketplace', 'fulfilment', 'order_city', 'order_state',
    'order_postal', 'tax_collection_model', 'product_sales', 'product_sales_tax',
    'postage_credits', 'shipping_credits_tax', 'gift_wrap_credits',
    'giftwrap_credits_tax', 'promotional_rebates', 'promotional_rebates_tax',
    'marketplace_withheld_tax', 'selling_fees', 'fba_fees',
    'other_transaction_fees', 'other', 'total'
]


def sanitize_column_name(name):
    """清理列名"""
    if not name or not str(name).strip():
        return 'column'
    name = str(name).strip()
    name = ''.join(c if c.isalnum() or c in '_' else '_' for c in name)
    if name and name[0].isdigit():
        name = 'col_' + name
    # 转小写
    name = name.lower()
    return name


def create_database(connection):
    """创建数据库"""
    cursor = connection.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.execute(f"USE {DB_NAME}")
    cursor.close()
    print(f"数据库 {DB_NAME} 已创建/已选中")


def read_csv_data(filepath):
    """读取CSV文件，返回标准化后的数据和原始数据"""
    data_rows = []
    header = None

    with open(filepath, 'r', encoding='utf-8-sig', errors='ignore') as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, 1):
            if line_num == 8:
                header = row
            elif line_num > 8:
                if any(cell.strip() for cell in row if cell):
                    data_rows.append(row)

    return header, data_rows


def build_column_mapping(header):
    """
    构建列名到索引的映射
    优先使用 COLUMN_MAPPING 映射，其次使用清理后的列名
    """
    col_to_idx = {}

    for idx, col_name in enumerate(header):
        if not col_name:
            continue

        col_lower = col_name.lower().strip()

        # 1. 首先检查 COLUMN_MAPPING 映射
        if col_lower in COLUMN_MAPPING:
            mapped_name = COLUMN_MAPPING[col_lower]
            if mapped_name not in col_to_idx:
                col_to_idx[mapped_name] = idx

        # 2. 然后使用清理后的列名
        clean_name = sanitize_column_name(col_name)
        if clean_name not in col_to_idx:
            col_to_idx[clean_name] = idx

        # 3. 原始小写列名作为备用
        if col_lower not in col_to_idx:
            col_to_idx[col_lower] = idx

    return col_to_idx


def standardize_row(row, col_to_idx, unified_cols):
    """将一行数据标准化到统一格式"""
    std_row = []
    for col in unified_cols:
        if col in col_to_idx:
            idx = col_to_idx[col]
            std_row.append(row[idx] if idx < len(row) else '')
        else:
            std_row.append('')

    return std_row


def get_table_columns(connection):
    """获取现有表的列"""
    cursor = connection.cursor()
    cursor.execute(f"SHOW COLUMNS FROM {TABLE_NAME}")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns


def create_table(connection):
    """创建或更新表结构"""
    cursor = connection.cursor()

    # 检查表是否存在
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
    table_exists = cursor.fetchone()

    if table_exists:
        print(f"表 {TABLE_NAME} 已存在，跳过创建")
        cursor.close()
        return

    # 创建新表，使用正确的数值类型
    col_defs = [
        "`date_time` VARCHAR(50)",
        "`settlement_id` VARCHAR(100)",
        "`type` VARCHAR(100)",
        "`order_id` VARCHAR(100)",
        "`Amazon_SKU` VARCHAR(100)",
        "`description` TEXT",
        "`quantity` INT DEFAULT 0",
        "`marketplace` VARCHAR(50)",
        "`fulfilment` VARCHAR(20)",
        "`order_city` VARCHAR(100)",
        "`order_state` VARCHAR(100)",
        "`order_postal` VARCHAR(20)",
        "`tax_collection_model` VARCHAR(50)",
        "`product_sales` DECIMAL(15,2) DEFAULT 0",
        "`product_sales_tax` DECIMAL(15,2) DEFAULT 0",
        "`postage_credits` DECIMAL(15,2) DEFAULT 0",
        "`shipping_credits_tax` DECIMAL(15,2) DEFAULT 0",
        "`gift_wrap_credits` DECIMAL(15,2) DEFAULT 0",
        "`giftwrap_credits_tax` DECIMAL(15,2) DEFAULT 0",
        "`promotional_rebates` DECIMAL(15,2) DEFAULT 0",
        "`promotional_rebates_tax` DECIMAL(15,2) DEFAULT 0",
        "`marketplace_withheld_tax` DECIMAL(15,2) DEFAULT 0",
        "`selling_fees` DECIMAL(15,2) DEFAULT 0",
        "`fba_fees` DECIMAL(15,2) DEFAULT 0",
        "`other_transaction_fees` DECIMAL(15,2) DEFAULT 0",
        "`other` DECIMAL(15,2) DEFAULT 0",
        "`total` DECIMAL(15,2) DEFAULT 0",
        "`source_file` VARCHAR(255)",
        "`import_date` DATE",
    ]

    create_sql = f"""
    CREATE TABLE {TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(col_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

    cursor.execute(create_sql)
    cursor.close()
    print(f"表 {TABLE_NAME} 已创建（使用正确数值类型）")


def convert_value(col_name, value):
    """将值转换为正确的类型"""
    if value is None or value == '':
        return None

    # 数值类型字段
    numeric_fields = {
        'quantity': int,
        'product_sales': float,
        'product_sales_tax': float,
        'postage_credits': float,
        'shipping_credits_tax': float,
        'gift_wrap_credits': float,
        'giftwrap_credits_tax': float,
        'promotional_rebates': float,
        'promotional_rebates_tax': float,
        'marketplace_withheld_tax': float,
        'selling_fees': float,
        'fba_fees': float,
        'other_transaction_fees': float,
        'other': float,
        'total': float,
    }

    if col_name in numeric_fields:
        # 去除逗号并转换
        clean_value = str(value).replace(',', '').strip()
        if clean_value == '' or clean_value == '-':
            return 0
        try:
            return numeric_fields[col_name](clean_value)
        except (ValueError, TypeError):
            return 0

    return value


def insert_data(connection, data_rows, source_file):
    """插入数据，返回插入的行数"""
    if not data_rows:
        return 0

    cursor = connection.cursor()

    col_names = ', '.join([f"`{col}`" for col in UNIFIED_COLUMNS])
    col_names += ", `source_file`, `import_date`"

    placeholders = ', '.join(['%s'] * (len(UNIFIED_COLUMNS) + 2))
    insert_sql = f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES ({placeholders})"

    import_date = datetime.now().strftime('%Y-%m-%d')

    batch_size = 1000
    batch = []
    inserted_count = 0

    for row in data_rows:
        # 转换数值字段
        converted_row = [convert_value(col, val) for col, val in zip(UNIFIED_COLUMNS, row)]
        row_data = converted_row + [source_file, import_date]
        batch.append(row_data)

        if len(batch) >= batch_size:
            try:
                cursor.executemany(insert_sql, batch)
                connection.commit()
                inserted_count += len(batch)
            except Exception as e:
                print(f"    批量插入错误: {e}")
                connection.rollback()
                # 逐条插入尝试
                for single_row in batch:
                    try:
                        cursor.execute(insert_sql, single_row)
                        connection.commit()
                        inserted_count += 1
                    except:
                        pass
            batch = []

    if batch:
        try:
            cursor.executemany(insert_sql, batch)
            connection.commit()
            inserted_count += len(batch)
        except Exception as e:
            print(f"    批量插入错误: {e}")
            connection.rollback()
            for single_row in batch:
                try:
                    cursor.execute(insert_sql, single_row)
                    connection.commit()
                    inserted_count += 1
                except:
                    pass

    cursor.close()
    return inserted_count


def main():
    print("=" * 60)
    print("开始导入亚马逊按月份交易数据")
    print("=" * 60)

    # 连接数据库
    print("\n1. 连接数据库...")
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("   数据库连接成功")
    except Exception as e:
        print(f"   数据库连接失败: {e}")
        return

    # 创建数据库
    print("\n2. 创建/选择数据库...")
    create_database(connection)

    # 创建表
    print("\n3. 检查数据表...")
    create_table(connection)

    # 遍历所有月份文件夹
    print("\n4. 扫描月份文件夹...")

    total_files = 0
    total_rows = 0
    total_inserted = 0

    # 按月份顺序处理
    month_folders = sorted([f for f in os.listdir(BASE_DIR) if f.endswith('月')])

    for month_folder in month_folders:
        month_path = os.path.join(BASE_DIR, month_folder)
        if not os.path.isdir(month_path):
            continue

        csv_files = [f for f in os.listdir(month_path) if f.endswith('.csv')]

        if not csv_files:
            continue

        print(f"\n   处理 {month_folder} ({len(csv_files)} 个文件)")

        for csv_file in csv_files:
            filepath = os.path.join(month_path, csv_file)
            total_files += 1

            print(f"\n   处理: {csv_file}")

            # 读取CSV
            header, data_rows = read_csv_data(filepath)

            if not header or not data_rows:
                print(f"     跳过: 无数据")
                continue

            # 构建列映射
            col_to_idx = build_column_mapping(header)

            # 标准化数据
            standardized_rows = []

            for row in data_rows:
                std_row = standardize_row(row, col_to_idx, UNIFIED_COLUMNS)
                standardized_rows.append(std_row)

            print(f"     读取 {len(standardized_rows)} 条数据")

            # 直接插入所有数据（不移除重复，因为同一订单可能包含多个SKU）
            if standardized_rows:
                count = insert_data(connection, standardized_rows, csv_file)
                total_inserted += count
                print(f"     插入 {count} 条")

            total_rows += len(standardized_rows)

    print("\n" + "=" * 60)
    print(f"处理完成!")
    print(f"  总文件数: {total_files}")
    print(f"  总数据行: {total_rows}")
    print(f"  插入行数: {total_inserted}")
    print("=" * 60)

    # 显示统计信息
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) as total FROM {TABLE_NAME}")
    print(f"\n当前表总记录数: {cursor.fetchone()[0]}")

    cursor.execute(f"SELECT LEFT(source_file, 40) as source, COUNT(*) as cnt FROM {TABLE_NAME} GROUP BY source_file ORDER BY cnt DESC LIMIT 10")
    print("\n各文件数据量 (前10):")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
