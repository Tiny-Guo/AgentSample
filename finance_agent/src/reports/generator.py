"""
报表生成模块
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional
import pandas as pd
from ..data.database import db, get_full_report_query, get_monthly_report_query
from ..data.file_handler import file_handler
from .calculator import add_product_cost


class ReportGenerator:
    """报表生成器"""

    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or "./reports"
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def generate_full_report(self, start_date: str = None, end_date: str = None,
                            year: int = None, month: int = None) -> tuple[pd.DataFrame, str]:
        """
        生成完整财务报表

        Args:
            start_date: 开始日期，格式 YYYY-MM-DD，如 "2025-11-01"
            end_date: 结束日期，格式 YYYY-MM-DD，如 "2026-01-31"
            year: 筛选年份，如2025（与日期范围二选一）
            month: 筛选月份，如11

        Returns:
            tuple: (DataFrame, file_path)
        """
        # 查询数据库
        df = db.query(get_full_report_query(start_date, end_date, year, month))

        if df.empty:
            return df, ""

        # 添加商品成本
        df = add_product_cost(df)

        # 计算利润
        df['净利润'] = df['平台毛利'].fillna(0) - df['商品成本'].fillna(0)

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if start_date and end_date:
            filename = f"财务报表_{start_date}_{end_date}_{timestamp}.xlsx"
        elif year and month:
            filename = f"财务报表_{year}_{month:02d}_{timestamp}.xlsx"
        elif year:
            filename = f"财务报表_{year}_全年_{timestamp}.xlsx"
        else:
            filename = f"财务报表_全部_{timestamp}.xlsx"
        filepath = os.path.join(self.output_dir, filename)

        # 保存Excel
        df.to_excel(filepath, index=False, engine='openpyxl')

        return df, filepath

    def generate_monthly_report(self, year: int = None, month: int = None) -> tuple[pd.DataFrame, str]:
        """生成月度汇总报表"""
        # 使用新的查询函数
        df = db.query(get_monthly_report_query(year, month))

        if df.empty:
            return df, ""

        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if year and month:
            filename = f"月度报表_{year}_{month:02d}_{timestamp}.xlsx"
        elif year:
            filename = f"月度报表_{year}_全年_{timestamp}.xlsx"
        else:
            filename = f"月度报表_全部_{timestamp}.xlsx"
        filepath = os.path.join(self.output_dir, filename)

        df.to_excel(filepath, index=False, engine='openpyxl')

        return df, filepath

    def read_and_analyze_file(self, filepath: str) -> pd.DataFrame:
        """读取并分析文件"""
        return file_handler.read_file(filepath)

    def export_to_excel(self, df: pd.DataFrame, filename: str = None) -> str:
        """导出DataFrame到Excel"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"导出_{timestamp}.xlsx"

        filepath = os.path.join(self.output_dir, filename)
        df.to_excel(filepath, index=False, engine='openpyxl')
        return filepath


# 全局报表生成器实例
report_generator = ReportGenerator()
