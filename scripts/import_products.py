import os
import sys
import pymysql
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, db_config

DB_NAME = db_config.database

EXCEL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', '产品信息-bb70bb92bf815af387714d37365e5c8d.xlsx')


def infer_column_type(col_name):
    """根据列名推断MySQL字段类型"""
    col_lower = col_name.lower()

    # 短文本字段
    short_text_fields = [
        '产品sku', '产品名称', '产品英文名称', '运营方式', '一级品类',
        '二级品类', '销售状态', '产品款式', '海关中文品名', '海关英文品名',
        '申报币种', '默认采购员', '开发负责人', '销售负责人', '供应商',
        '币种', '设计师', '开始设计时间', '截止设计时间',
        '是否包含电池', '是否含特货', '是否含非液体化妆品', '是否为仿制品',
        '是否需要质检', '产品颜色', '产品尺寸', '中文材质', '英文材质',
        '贴标容易度', '组织机构', '是否组合产品', '附属销售员', '品牌',
        '商品id', '主型号', '产品开发时间', '上架时间', '参考采购链接',
        '中文用途', '英文用途', '物流属性', '箱规名称', '包装方式', '是否退税',
        '产品sku', '产品名称', '产品英文名称', '产品图片', '主图', '附图',
        '销售链接', '产品链接'
    ]

    # 长文本字段
    text_fields = [
        '图片url', '所有产品图片', '产品详细描述', '产品说明', '产品描述',
        '产品自定义属性', '自定义分类', '供应商产品地址', '供应商品号',
        '详细描述', '备注'
    ]

    # 日期字段
    date_fields = [
        '日期', '时间', '日期时间', '创建时间', '更新时间',
        '开始时间', '截止时间', '上架时间', '下架时间',
        '开发时间', '设计时间', '审核时间'
    ]

    # 数值字段
    numeric_fields = [
        '价格', '采购价', '成本', '重量', '体积', '长度', '宽度', '高度',
        '采购参考价', '销售价', '原价', '折扣价', '最小采购量', '交期',
        '库存', '数量', '销售额', '利润', '利润率',
        '尺寸', '重量', '毛重', '净重'
    ]

    # 布尔字段
    bool_fields = [
        '是否', '有没有', '有没有', '是', '否',
        '开关', '启用', '禁用', '激活', '锁定'
    ]

    # 检查匹配
    for field in text_fields:
        if field in col_lower:
            return 'TEXT'

    for field in short_text_fields:
        if field in col_lower:
            return 'VARCHAR(255)'

    for field in date_fields:
        if field in col_lower:
            return 'DATETIME'

    for field in numeric_fields:
        if field in col_lower:
            return 'DECIMAL(15,4)'

    for field in bool_fields:
        if field in col_lower:
            return 'TINYINT(1)'

    # 默认返回 VARCHAR(255)
    return 'VARCHAR(255)'


def create_products_table(connection):
    """创建产品表"""
    cursor = connection.cursor()

    # 读取Excel获取列名
    df = pd.read_excel(EXCEL_FILE, nrows=5)
    columns = df.columns.tolist()

    # 直接使用原始中文列名建表
    columns_def = []
    for col in columns:
        mysql_type = infer_column_type(col)
        columns_def.append(f"`{col}` {mysql_type}")

    # 检查表是否已存在
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{DB_NAME}'
        AND table_name = 'products'
    """)

    if cursor.fetchone()[0] > 0:
        print("表 products 已存在，删除重建...")
        cursor.execute("DROP TABLE IF EXISTS products")

    # 创建表，主键为产品SKU
    create_sql = f"""
    CREATE TABLE products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(columns_def)},
        import_date DATE DEFAULT NULL,
        UNIQUE KEY uk_sku (`产品SKU`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """

    cursor.execute(create_sql)
    connection.commit()
    print("产品表创建成功!")

    cursor.close()


def infer_value_type(col_name, val):
    """根据列名和值推断并转换数据类型"""
    col_lower = col_name.lower()

    # 处理空值
    if pd.isna(val) or val is None:
        return None

    # 日期字段
    date_fields = ['日期', '时间', '日期时间', '创建时间', '更新时间',
                   '开始时间', '截止时间', '上架时间', '下架时间']
    for field in date_fields:
        if field in col_lower:
            if isinstance(val, pd.Timestamp):
                return val.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(val, str):
                return val
            else:
                return str(val)

    # 布尔字段
    bool_fields = ['是否', '有没有', '开关', '启用', '禁用', '激活']
    for field in bool_fields:
        if field in col_lower:
            val_str = str(val).lower().strip()
            if val_str in ['是', 'true', '1', 'yes', '有', '启用', '激活']:
                return 1
            elif val_str in ['否', 'false', '0', 'no', '没有', '禁用']:
                return 0
            return val

    # 数值字段
    numeric_fields = ['价格', '采购价', '成本', '重量', '体积', '长度', '宽度', '高度',
                      '采购参考价', '销售价', '原价', '折扣价', '最小采购量', '交期',
                      '库存', '数量', '销售额', '利润', '利润率']
    for field in numeric_fields:
        if field in col_lower:
            if isinstance(val, (int, float)):
                return float(val)
            try:
                clean_val = str(val).replace(',', '').strip()
                if clean_val and clean_val != '-':
                    return float(clean_val)
            except:
                pass
            return None

    # 浮点数
    if isinstance(val, float):
        return val

    # 字符串
    return str(val) if pd.notna(val) else None


def import_products_data(connection):
    """导入产品数据"""
    cursor = connection.cursor()

    # 读取Excel
    print("读取Excel数据...")
    df = pd.read_excel(EXCEL_FILE)

    # 获取列名
    columns = df.columns.tolist()

    # 构建插入语句
    placeholders = ', '.join(['%s'] * (len(columns) + 1))  # +1 for import_date
    columns_for_insert = ', '.join([f'`{col}`' for col in columns]) + ', import_date'

    insert_sql = f"""
    INSERT INTO products ({columns_for_insert})
    VALUES ({placeholders})
    """

    # 批量提交设置
    batch_size = 500
    import_batch_size = 200

    # 转换数据并插入
    print("导入数据...")
    success_count = 0
    error_count = 0
    batch = []
    total_committed = 0

    for idx, row in df.iterrows():
        try:
            values = []
            for col in columns:
                val = row[col]
                converted_val = infer_value_type(col, val)
                values.append(converted_val)

            values.append(datetime.now().strftime('%Y-%m-%d'))
            batch.append(tuple(values))

            # 批量插入
            if len(batch) >= batch_size:
                try:
                    cursor.executemany(insert_sql, batch)
                    connection.commit()
                    success_count += len(batch)
                    total_committed += len(batch)

                    if total_committed % import_batch_size == 0:
                        print(f"  已处理 {total_committed}/{len(df)} 条记录")

                except Exception as e:
                    connection.rollback()
                    error_count += len(batch)
                    if error_count <= 5:
                        print(f"  批量插入错误: {e}")
                    # 逐条尝试
                    for single_row in batch:
                        try:
                            cursor.execute(insert_sql, single_row)
                            connection.commit()
                            success_count += 1
                        except:
                            pass
                batch = []

        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"  错误: {e}")

    # 提交剩余的数据
    if batch:
        try:
            cursor.executemany(insert_sql, batch)
            connection.commit()
            success_count += len(batch)
        except Exception as e:
            connection.rollback()
            error_count += len(batch)
            print(f"  最后批量插入错误: {e}")
            # 逐条尝试
            for single_row in batch:
                try:
                    cursor.execute(insert_sql, single_row)
                    connection.commit()
                    success_count += 1
                except:
                    pass

    print(f"导入完成! 成功: {success_count}, 失败: {error_count}")

    cursor.close()


def verify_import(connection):
    """验证导入结果"""
    cursor = connection.cursor()

    # 统计数量
    cursor.execute("SELECT COUNT(*) FROM products")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT `产品SKU`) FROM products")
    unique_sku = cursor.fetchone()[0]

    print(f"\n=== 验证结果 ===")
    print(f"总记录数: {total}")
    print(f"唯一SKU数: {unique_sku}")

    # 显示表结构
    cursor.execute("DESCRIBE products")
    print(f"\n表结构（前10列）:")
    for i, row in enumerate(cursor.fetchall()[:10]):
        print(f"  {row[0]}: {row[1]}")

    # 显示示例数据
    cursor.execute("SELECT * FROM products LIMIT 3")
    rows = cursor.fetchall()
    print(f"\n示例数据（显示前5个字段）:")
    for row in rows:
        print(f"  {row[:5]}")

    cursor.close()


def main():
    print("=" * 60)
    print("创建产品表并导入数据")
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

    # 创建表
    print("\n2. 创建产品表...")
    create_products_table(connection)

    # 导入数据
    print("\n3. 导入产品数据...")
    import_products_data(connection)

    # 验证
    print("\n4. 验证结果...")
    verify_import(connection)

    connection.close()

    print("\n" + "=" * 60)
    print("产品表创建并导入完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
