from dotenv import load_dotenv
import os

# load proxies
load_dotenv('proxy.env')
BINANCE_PROXY = os.getenv('BINANCE_PROXY')
GATE_PROXY = os.getenv('GATE_PROXY')

