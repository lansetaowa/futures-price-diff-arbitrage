from dotenv import load_dotenv
import os

# 获取 config.py 的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# load proxies
load_dotenv(os.path.join(BASE_DIR, 'proxy.env'))
BINANCE_PROXY = os.getenv('BINANCE_PROXY')
GATE_PROXY = os.getenv('GATE_PROXY')

# load Gateio api
load_dotenv(os.path.join(BASE_DIR, "gate_api.env"))
GATEIO_API_KEY = os.getenv('G_KEY')
GATEIO_API_SECRET = os.getenv('G_SECRET')

# load Binance api
load_dotenv(os.path.join(BASE_DIR,"binance_api.env"))
BINANCE_API_KEY = os.getenv('B_KEY')
BINANCE_API_SECRET = os.getenv('B_SECRET')

# 黑名单币种：Gate 和 Binance 虽然 symbol 一致，但实际资产不同
SYMBOL_BLACKLIST = ["NEIROUSDT"]
