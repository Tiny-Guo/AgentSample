"""
Excel/CSV 文件读取处理
"""
import os
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime
from ..config import data_config


class BillParser:
    """1510账单解析器 - 从账单封面提取关键费用信息"""

    BILL_DIR = "财务账单/海外仓账单（各自建立不同表-混合文件）/1510"

    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or data_config.data_dir

    def get_bill_dir(self) -> str:
        """获取1510账单目录的完整路径"""
        return os.path.join(self.base_dir, self.BILL_DIR)

    def list_bill_files(self) -> List[str]:
        """列出所有1510账单文件"""
        bill_dir = self.get_bill_dir()
        if not os.path.exists(bill_dir):
            return []
        return [os.path.join(bill_dir, f) for f in os.listdir(bill_dir)
                if f.endswith('.xlsx') and f.startswith('bill-')]

    def _extract_value(self, df: pd.DataFrame, row: int, col: int) -> str:
        """安全提取单元格值"""
        try:
            val = df.iloc[row, col]
            return str(val) if pd.notna(val) else ""
        except:
            return ""

    def _extract_period(self, period_str: str) -> str:
        """从费用周期字符串提取年月"""
        if not period_str:
            return ""
        # 格式: "2025-09-01~2025-09-30 GMT+00:00"
        if '~' in period_str:
            start = period_str.split('~')[0]
            return start[:7]  # 返回 "2025-09"
        return period_str[:7]

    def _extract_charges_from_df(self, df: pd.DataFrame) -> Dict:
        """从账单封面DataFrame提取费用数据"""
        result = {
            '账单编号': '',
            '客户名称': '',
            '费用周期': '',
            '费用周期_年月': '',
            '币种_GBP': 0.0,
            '币种_EUR': 0.0,
            '入库单费用': 0.0,
            '仓租费用': 0.0,
            '其他费用': 0.0,
        }

        try:
            # 提取客户名称 (行3, 列1)
            result['客户名称'] = self._extract_value(df, 3, 1)

            # 提取费用周期 (行4, 列1)
            period_str = self._extract_value(df, 4, 1)
            result['费用周期'] = period_str
            result['费用周期_年月'] = self._extract_period(period_str)

            # 提取币种 (行7, 列1-2)
            gbp = self._extract_value(df, 7, 1)
            eur = self._extract_value(df, 7, 2)
            result['币种_GBP'] = gbp
            result['币种_EUR'] = eur

            # 提取入库单费用 (行37, 列1)
            inbound_gbp = self._extract_value(df, 37, 1)
            inbound_eur = self._extract_value(df, 37, 2)
            result['入库单费用'] = self._safe_float(inbound_gbp) + self._safe_float(inbound_eur)

            # 提取仓租费用 (行44, 列1)
            storage_gbp = self._extract_value(df, 44, 1)
            storage_eur = self._extract_value(df, 44, 2)
            result['仓租费用'] = self._safe_float(storage_gbp) + self._safe_float(storage_eur)

            # 提取其他费用 (行60, 列1)
            other_gbp = self._extract_value(df, 60, 1)
            other_eur = self._extract_value(df, 60, 2)
            result['其他费用'] = self._safe_float(other_gbp) + self._safe_float(other_eur)

        except Exception as e:
            print(f"解析账单封面错误: {e}")

        return result

    def _safe_float(self, value: str) -> float:
        """安全转换为浮点数"""
        try:
            if not value or value == 'nan':
                return 0.0
            return float(value)
        except:
            return 0.0

    def parse_bill_cover(self, filepath: str) -> Dict:
        """
        解析单个账单封面

        Args:
            filepath: 账单文件完整路径

        Returns:
            账单封面关键数据字典
        """
        try:
            df = pd.read_excel(filepath, sheet_name=0, header=None)
            result = self._extract_charges_from_df(df)
            result['文件名'] = os.path.basename(filepath)
            # 从文件名提取账单编号
            result['账单编号'] = os.path.basename(filepath).replace('bill-', '').replace('.xlsx', '')
            return result
        except Exception as e:
            return {'错误': str(e), '文件名': os.path.basename(filepath)}

    def parse_all_bills(self) -> pd.DataFrame:
        """
        解析所有1510账单

        Returns:
            包含所有账单封面数据的DataFrame
        """
        files = self.list_bill_files()
        if not files:
            return pd.DataFrame()

        results = []
        for filepath in sorted(files):
            bill_data = self.parse_bill_cover(filepath)
            results.append(bill_data)

        df = pd.DataFrame(results)

        # 按费用周期排序
        if '费用周期' in df.columns:
            df = df.sort_values('费用周期')

        return df

    def export_to_excel(self, df: pd.DataFrame, output_filename: str = None) -> str:
        """
        导出账单汇总到Excel

        Args:
            df: 账单数据DataFrame
            output_filename: 输出文件名（不含路径）

        Returns:
            导出文件的完整路径
        """
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"1510_bill_summary_{timestamp}.xlsx"

        # 确保输出目录存在
        output_dir = os.path.join(self.base_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        output_path = os.path.join(output_dir, output_filename)

        # 导出到Excel
        df.to_excel(output_path, index=False, engine='openpyxl')

        return output_path


class FileHandler:
    """文件处理管理器"""

    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or data_config.data_dir

    def list_files(self, extension: str = None) -> List[str]:
        """列出数据目录下的文件"""
        files = []
        for root, dirs, filenames in os.walk(self.data_dir):
            for filename in filenames:
                if extension is None or filename.endswith(extension):
                    files.append(os.path.join(root, filename))
        return files

    def read_excel(self, filepath: str, sheet_name: int = 0) -> pd.DataFrame:
        """读取Excel文件"""
        return pd.read_excel(filepath, sheet_name=sheet_name)

    def read_csv(self, filepath: str, encoding: str = 'utf-8-sig') -> pd.DataFrame:
        """读取CSV文件"""
        return pd.read_csv(filepath, encoding=encoding)

    def read_file(self, filepath: str) -> pd.DataFrame:
        """根据文件扩展名自动选择读取方式"""
        ext = Path(filepath).suffix.lower()
        if ext in ['.xlsx', '.xls']:
            return self.read_excel(filepath)
        elif ext == '.csv':
            return self.read_csv(filepath)
        else:
            raise ValueError(f"不支持的文件类型: {ext}")

    def get_sheet_names(self, filepath: str) -> List[str]:
        """获取Excel文件的sheet名称列表"""
        xl_file = pd.ExcelFile(filepath)
        return xl_file.sheet_names


# 全局文件处理实例
file_handler = FileHandler()

# 全局账单解析器实例
bill_parser = BillParser()
