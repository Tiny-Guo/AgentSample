"""
SKU Mapping 映射表导入脚本

从 sellersku-productsku-asin.csv 导入 sku_mapping 表
"""
import os
import sys
import csv
import pymysql
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, db_config

DB_NAME = db_config.database
CSV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'sellersku-productsku-asin.csv')


def create_sku_mapping_table(connection):
    """创建 sku_mapping 表"""
    cursor = connection.cursor()

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{DB_NAME}'
        AND table_name = 'sku_mapping'
    """)

    if cursor.fetchone()[0] > 0:
        print("表 sku_mapping 已存在，删除重建...")
        cursor.execute("DROP TABLE IF EXISTS sku_mapping")

    create_sql = """
    CREATE TABLE sku_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        seller_sku VARCHAR(255) NOT NULL COMMENT '亚马逊SKU',
        product_sku VARCHAR(255) DEFAULT NULL COMMENT '产品SKU',
        asin VARCHAR(50) DEFAULT NULL COMMENT 'ASIN',
        marketplace VARCHAR(50) DEFAULT NULL COMMENT '站点',
        source_file VARCHAR(255) DEFAULT NULL COMMENT '来源文件',
        created_at DATETIME DEFAULT NULL COMMENT '创建时间',
        import_date DATE DEFAULT NULL COMMENT '导入日期',
        UNIQUE KEY uk_seller_sku (seller_sku),
        INDEX idx_product_sku (product_sku),
        INDEX idx_asin (asin)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

    cursor.execute(create_sql)
    connection.commit()
    print("表 sku_mapping 创建成功!")

    cursor.close()


def import_sku_mapping_data(connection):
    """导入 SKU 映射数据"""
    cursor = connection.cursor()

    if not os.path.exists(CSV_FILE):
        print(f"文件不存在: {CSV_FILE}")
        return 0

    print(f"读取文件: {CSV_FILE}")

    success_count = 0
    error_count = 0

    insert_sql = """
    INSERT INTO sku_mapping (seller_sku, product_sku, asin, marketplace, source_file, created_at, import_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        header = next(reader)
        print(f"  表头: {header}")

        for idx, row in enumerate(reader, 1):
            try:
                if len(row) < 4:
                    continue

                seller_sku = row[0].strip() if row[0] else None
                product_sku = row[1].strip() if len(row) > 1 and row[1] else None
                asin = row[2].strip() if len(row) > 2 and row[2] else None
                marketplace = row[3].strip() if len(row) > 3 and row[3] else None
                source_file = row[4].strip() if len(row) > 4 and row[4] else None
                created_at = row[5].strip() if len(row) > 5 and row[5] else None

                if not seller_sku:
                    continue

                import_date = datetime.now().strftime('%Y-%m-%d')

                cursor.execute(insert_sql, (seller_sku, product_sku, asin, marketplace, source_file, created_at, import_date))
                success_count += 1

                if idx % 1000 == 0:
                    connection.commit()
                    print(f"  已处理 {idx} 条...")

            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  错误: {e}")

        connection.commit()

    print(f"导入完成! 成功: {success_count}, 失败: {error_count}")

    cursor.close()
    return success_count


def verify_import(connection):
    """验证导入结果"""
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM sku_mapping")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT seller_sku) FROM sku_mapping")
    unique_seller_sku = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT product_sku) FROM sku_mapping WHERE product_sku IS NOT NULL")
    unique_product_sku = cursor.fetchone()[0]

    print(f"\n=== 验证结果 ===")
    print(f"总记录数: {total}")
    print(f"唯一 seller_sku: {unique_seller_sku}")
    print(f"唯一 product_sku: {unique_product_sku}")

    cursor.execute("SELECT * FROM sku_mapping LIMIT 3")
    print(f"\n示例数据:")
    for row in cursor.fetchall():
        print(f"  {row}")

    cursor.close()


def main():
    print("=" * 60)
    print("导入 SKU Mapping 数据")
    print("=" * 60)

    print("\n1. 连接数据库...")
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("   数据库连接成功")
    except Exception as e:
        print(f"   数据库连接失败: {e}")
        return

    connection.select_db(DB_NAME)

    print("\n2. 创建 sku_mapping 表...")
    create_sku_mapping_table(connection)

    print("\n3. 导入数据...")
    import_sku_mapping_data(connection)

    print("\n4. 验证结果...")
    verify_import(connection)

    connection.close()

    print("\n" + "=" * 60)
    print("SKU Mapping 导入完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
