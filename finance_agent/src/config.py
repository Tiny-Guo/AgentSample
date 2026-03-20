"""
配置管理模块 - 从根目录统一配置导入
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config import db_config, llm_config, data_config, DatabaseConfig, LLMConfig, DataConfig

__all__ = ['db_config', 'llm_config', 'data_config', 'DatabaseConfig', 'LLMConfig', 'DataConfig']