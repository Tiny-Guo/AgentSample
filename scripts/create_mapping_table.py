import os
import sys
import pymysql
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, db_config

DB_NAME = db_config.database


def create_mapping_table(connection):
    """创建中间关联表"""
    cursor = connection.cursor()

    # 检查表是否已存在
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{DB_NAME}'
        AND table_name = 'order_reference_mapping'
    """)

    if cursor.fetchone()[0] > 0:
        print("表 order_reference_mapping 已存在，是否删除重建? (y/n): ")
        # 直接删除重建
        cursor.execute("DROP TABLE IF EXISTS order_reference_mapping")
        print("已删除旧表")

    # 创建中间关联表（使用与源表相同的排序规则）
    create_sql = """
    CREATE TABLE order_reference_mapping (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(255) NOT NULL,
        reference_no VARCHAR(255) NOT NULL,
        create_date DATE DEFAULT NULL,
        UNIQUE KEY uk_order_ref (order_id, reference_no),
        INDEX idx_order_id (order_id),
        INDEX idx_reference_no (reference_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

    cursor.execute(create_sql)
    connection.commit()
    print("中间关联表创建成功!")

    cursor.close()


def populate_mapping_data(connection):
    """填充现有的关联数据"""
    cursor = connection.cursor()

    # 插入 transactions 和 b2c_order_charges 之间的匹配数据
    # 使用 DISTINCT 确保不插入重复的 (order_id, reference_no) 组合
    insert_sql = """
    INSERT INTO order_reference_mapping (order_id, reference_no, create_date)
    SELECT DISTINCT
        t.order_id,
        b.`参考号_Reference_NO`,
        CURDATE()
    FROM transactions t
    INNER JOIN b2c_order_charges b ON t.order_id = b.`参考号_Reference_NO`
    WHERE t.order_id IS NOT NULL
    AND t.order_id != ''
    AND b.`参考号_Reference_NO` IS NOT NULL
    AND b.`参考号_Reference_NO` != ''
    """

    try:
        cursor.execute(insert_sql)
        connection.commit()
        inserted = cursor.rowcount
        print(f"已插入 {inserted} 条关联数据")
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.rollback()

    cursor.close()


def add_foreign_key_to_mapping_table(connection):
    """为中间表添加外键约束"""
    cursor = connection.cursor()

    # 检查是否已有外键
    cursor.execute(f"""
        SELECT CONSTRAINT_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = '{DB_NAME}'
        AND TABLE_NAME = 'order_reference_mapping'
        AND REFERENCED_TABLE_NAME IS NOT NULL
    """)

    existing_fks = cursor.fetchall()

    # 添加指向 transactions 的外键（如果不存在）
    if not any('fk_mapping_order' in str(fk) for fk in existing_fks):
        try:
            cursor.execute("""
                ALTER TABLE order_reference_mapping
                ADD CONSTRAINT fk_mapping_order
                FOREIGN KEY (order_id) REFERENCES transactions(order_id)
                ON DELETE CASCADE
            """)
            connection.commit()
            print("已添加指向 transactions 的外键")
        except Exception as e:
            print(f"添加外键到 transactions 失败: {e}")
            connection.rollback()

    # 检查是否有指向 b2c_order_charges 的外键
    if not any('fk_mapping_ref' in str(fk) for fk in existing_fks):
        try:
            cursor.execute("""
                ALTER TABLE order_reference_mapping
                ADD CONSTRAINT fk_mapping_ref
                FOREIGN KEY (reference_no) REFERENCES b2c_order_charges(`参考号_Reference_NO`)
                ON DELETE CASCADE
            """)
            connection.commit()
            print("已添加指向 b2c_order_charges 的外键")
        except Exception as e:
            print(f"添加外键到 b2c_order_charges 失败: {e}")
            connection.rollback()

    cursor.close()


def verify_mapping_table(connection):
    """验证中间表数据"""
    cursor = connection.cursor()

    # 统计关联数量
    cursor.execute("SELECT COUNT(*) FROM order_reference_mapping")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT order_id) FROM order_reference_mapping")
    unique_orders = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT reference_no) FROM order_reference_mapping")
    unique_refs = cursor.fetchone()[0]

    print(f"\n=== 中间关联表统计 ===")
    print(f"总关联记录数: {total}")
    print(f"唯一 order_id 数量: {unique_orders}")
    print(f"唯一 reference_no 数量: {unique_refs}")

    # 显示表结构
    cursor.execute("DESCRIBE order_reference_mapping")
    print(f"\n表结构:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]}")

    cursor.close()


def show_sample_data(connection):
    """显示示例数据"""
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM order_reference_mapping LIMIT 5")
    rows = cursor.fetchall()

    print(f"\n示例数据（前5条）:")
    for row in rows:
        print(f"  {row}")

    cursor.close()


def main():
    print("=" * 60)
    print("创建中间关联表 order_reference_mapping")
    print("=" * 60)

    # 连接数据库
    print("\n1. 连接数据库...")
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("   数据库连接成功")
    except Exception as e:
        print(f"   数据库连接失败: {e}")
        return

    connection.select_db(DB_NAME)

    # 创建中间表
    print("\n2. 创建中间关联表...")
    create_mapping_table(connection)

    # 填充数据
    print("\n3. 填充关联数据...")
    populate_mapping_data(connection)

    # 尝试添加外键
    print("\n4. 添加外键约束...")
    add_foreign_key_to_mapping_table(connection)

    # 验证
    print("\n5. 验证结果...")
    verify_mapping_table(connection)
    show_sample_data(connection)

    connection.close()

    print("\n" + "=" * 60)
    print("中间关联表创建完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
