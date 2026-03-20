"""
统一配置管理模块

所有模块共享的配置，包括：
- 数据库配置
- LLM 配置
- 数据路径配置
"""
import os
from dataclasses import dataclass
from typing import Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "225guoxiao522"
    database: str = "amazon_nov_data"
    charset: str = "utf8mb4"


@dataclass
class LLMConfig:
    provider: str = "zhipu"
    zhipu_api_key: Optional[str] = "8ec5f04880274934988dde0d34fa37ee.NU6GuPWl5DJ48hfT"
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3"


@dataclass
class DataConfig:
    data_dir: str = os.path.join(BASE_DIR, "data")


db_config = DatabaseConfig()
llm_config = LLMConfig()
data_config = DataConfig()


DB_CONFIG = {
    "host": db_config.host,
    "port": db_config.port,
    "user": db_config.user,
    "password": db_config.password,
    "database": db_config.database,
    "charset": db_config.charset,
}
DB_NAME = db_config.database