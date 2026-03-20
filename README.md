# 财务数据分析系统

基于 LangGraph 的财务数据分析助手，支持数据库查询、文件读取、报表生成和利润分析。

## 项目结构

```
Money/
├── config.py                 # 统一配置管理
├── requirements.txt          # Python 依赖
├── finance_app.py           # Chainlit Web 入口（从根目录启动）
│
├── scripts/                  # 数据导入脚本
│   ├── import_amazon_monthly.py   # 导入亚马逊月度交易数据
│   ├── import_b2c_charges.py       # 导入B2C订单费用
│   ├── import_csv_to_mysql.py      # CSV导入MySQL
│   ├── import_products.py          # 导入产品信息
│   └── create_mapping_table.py     # 创建SKU映射表
│
├── finance_agent/           # AI Agent 模块
│   ├── app.py              # Chainlit Web 入口（原始位置）
│   ├── app_streamlit.py    # Streamlit 备选界面
│   ├── test_modules.py     # 模块测试脚本
│   └── src/
│       ├── config.py       # 配置导入（从根目录 config.py 读取）
│       ├── data/
│       │   ├── database.py # 数据库连接
│       │   └── file_handler.py  # 文件读取处理
│       ├── graph/
│       │   ├── agent.py    # LangGraph Agent 主程序
│       │   ├── nodes.py    # Agent 节点（SYSTEM_PROMPT）
│       │   ├── state.py    # Agent 状态定义
│       │   └── tools.py    # 12个工具定义
│       └── reports/
│           ├── generator.py # 报表生成器
│           └── calculator.py # 利润计算器
│
├── data/                    # 数据目录
│   ├── 产品信息-*.xlsx     # 产品信息
│   ├── 部分店铺收入/       # 亚马逊月度收入
│   └── 财务账单/          # 海外仓账单
│
└── example/                 # LangGraph 示例代码
    ├── agent.py
    ├── model_binding_tools.py
    ├── node.py
    └── state.py
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `config.py` 中的数据库配置：

```python
@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "your_password"
    database: str = "amazon_nov_data"
```

### 3. 导入数据

```bash
# 导入亚马逊月度交易数据
python scripts/import_amazon_monthly.py

# 导入B2C订单费用
python scripts/import_b2c_charges.py

# 导入产品信息
python scripts/import_products.py

# 创建SKU映射表
python scripts/create_mapping_table.py
```

### 4. 启动服务

```bash
# 方式一：从根目录启动（推荐）
chainlit run finance_app.py

# 方式二：从 finance_agent 目录启动
cd finance_agent
chainlit run app.py

# 备选：Streamlit 界面
streamlit run finance_agent/app_streamlit.py
```

## 功能模块

### AI Agent (finance_agent)

基于 LangGraph 的对话式 Agent，架构简单清晰：

- **2个节点**: `llm` ↔ `tool`（循环调用直到 LLM 无需工具）
- **12个工具**:

| 工具 | 功能 |
|-----|------|
| `query_database` | 执行 SQL 查询 |
| `get_database_tables` | 获取数据库表列表 |
| `get_table_schema` | 获取表结构 |
| `get_data_time_range` | 查询数据时间范围 |
| `list_data_files` | 列出数据目录文件 |
| `read_excel_file` | 读取 Excel |
| `read_csv_file` | 读取 CSV |
| `generate_full_financial_report` | 生成完整财务报表 |
| `generate_monthly_summary` | 生成月度汇总报表 |
| `calculate_profit_summary` | 计算利润汇总 |
| `extract_1510_bill_summary` | 提取1510账单汇总 |
| `extract_single_1510_bill` | 提取单个1510账单 |

### 报表生成

支持以下报表类型：

1. **完整财务报表** - 包含所有字段的详细报表
2. **月度汇总报表** - 按月份汇总的数据
3. **利润分析报表** - 按SKU计算利润

### SKU 类型说明

本系统使用两种不同的 SKU：

| 类型 | 字段名 | 说明 |
|------|--------|------|
| **亚马逊SKU** | `Amazon_SKU` / `sellerSku` | 亚马逊平台商品标识符 |
| **产品SKU** | `产品SKU` | 公司内部产品标识符 |

两种 SKU 通过 `sku_mapping` 表关联。

## 数据库表结构

- `transactions`: 亚马逊月度交易数据（使用 Amazon_SKU）
- `products`: 产品信息表（使用 产品SKU）
- `sku_mapping`: SKU映射表
- `b2c_order_charges`: B2C订单费用表

## 配置说明

所有配置集中在根目录 `config.py`:

- `DatabaseConfig`: 数据库配置
- `LLMConfig`: LLM 配置（支持 ZhipuAI/OpenAI/Anthropic/Ollama）
- `DataConfig`: 数据目录配置