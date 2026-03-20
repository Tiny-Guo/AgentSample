"""
数据库导入总脚本

按正确顺序执行所有数据导入
"""
import os
import sys
import pymysql
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, db_config

DB_NAME = db_config.database


def drop_all_tables(connection):
    """删除所有旧表"""
    cursor = connection.cursor()

    tables = [
        'order_reference_mapping',
        'transactions',
        'b2c_order_charges',
        'b2b_order_charges',
        'deposit',
        'products',
        'sku_mapping',
    ]

    print("删除旧表...")
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"  ✓ 已删除 {table}")
        except Exception as e:
            print(f"  ✗ 删除 {table} 失败: {e}")

    connection.commit()
    cursor.close()


def run_script(script_name):
    """运行指定的导入脚本"""
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    print(f"\n{'='*60}")
    print(f"执行: {script_name}")
    print(f"{'='*60}")
    result = os.system(f"python \"{script_path}\"")
    if result != 0:
        print(f"  ✗ {script_name} 执行失败!")
    return result


def main():
    parser = argparse.ArgumentParser(description='数据库导入总脚本')
    parser.add_argument('--skip-drop', action='store_true', help='跳过删除旧表步骤')
    parser.add_argument('--script', type=str, help='只运行指定的脚本')
    args = parser.parse_args()

    print("=" * 60)
    print("数据库导入总脚本")
    print("=" * 60)

    print("\n0. 连接数据库...")
    try:
        connection = pymysql.connect(**DB_CONFIG)
        print("   数据库连接成功")
    except Exception as e:
        print(f"   数据库连接失败: {e}")
        return

    connection.select_db(DB_NAME)

    # 删除旧表
    if not args.skip_drop:
        print("\n1. 删除旧表...")
        drop_all_tables(connection)
    else:
        print("\n1. 跳过删除旧表步骤")

    connection.close()

    print("\n" + "=" * 60)
    print("准备导入新数据")
    print("=" * 60)

    scripts = [
        ('import_sku_mapping.py', 'SKU映射表'),
        ('import_products.py', '产品信息'),
        ('import_amazon_monthly.py', '亚马逊交易数据'),
        ('import_b2c_charges.py', 'B2C订单费用'),
        ('create_mapping_table.py', '订单关联表'),
    ]

    # 如果指定了单个脚本，只运行那个
    if args.script:
        for script, desc in scripts:
            if script == args.script:
                print(f"\n>>> 只导入 {desc}...")
                result = run_script(script)
                if result != 0:
                    print(f"  ✗ {desc} 导入失败")
                else:
                    print(f"  ✓ {desc} 导入完成")
                break
    else:
        print("\n导入顺序:")
        for i, (script, desc) in enumerate(scripts, 1):
            print(f"  {i}. {script:28} - {desc}")

        print("\n按回车继续，或 Ctrl+C 取消...")
        try:
            input()
        except EOFError:
            pass

        print("\n" + "=" * 60)
        print("开始导入...")
        print("=" * 60)

        for script, desc in scripts:
            print(f"\n>>> 导入 {desc}...")
            result = run_script(script)
            if result != 0:
                print(f"  ✗ {desc} 导入失败，停止执行")
                break
            else:
                print(f"  ✓ {desc} 导入完成")

    print("\n" + "=" * 60)
    print("全部导入完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
