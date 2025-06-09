"""
合约下单的模块
- gate/binance设置合约杠杆
- gate/binance合约下单，市价单
"""
import ccxt
from config import BINANCE_API_KEY, BINANCE_API_SECRET, GATEIO_API_KEY, GATEIO_API_SECRET, BINANCE_PROXY, GATE_PROXY
from gate_api import FuturesApi, Configuration, ApiClient
from gate_api.exceptions import ApiException

class BinanceFuturesTrader:
    # Binance的symbol格式为：BTCUSDT
    def __init__(self, api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET):
        self.exchange = ccxt.binanceusdm({
            'apiKey': api_key,
            'secret': api_secret
        })

        self.exchange.httpsProxy = BINANCE_PROXY
        self.markets = self.exchange.load_markets()

    # 设置杠杆
    def set_leverage(self, symbol, leverage):
        try:
            self.exchange.set_leverage(leverage=leverage, symbol=symbol)
        except Exception as e:
            print(f"[Error] Binance - Can't set future leverage for {symbol}: {e}")

    # 获取合约账户余额
    def get_balance(self):
        try:
            balance = self.exchange.fetch_balance()['info']['availableBalance']
            return balance
        except Exception as e:
            print(f"[Error] Binance - Can't get future account balance: {e}")

    # 根据合约下单方向，获取相应的实时orderbook price
    def get_orderbook_price(self, symbol, side):
        ob = self.exchange.fetch_order_book(symbol)
        if side == 'long': # 做多需要buy
            return ob['asks'][0][0]  # best ask price for buy
        elif side == 'short': # 做空需要sell
            return ob['bids'][0][0]  # best bid price for sell

    # 转化下单金额至quantity
    def usdt_to_quantity(self, symbol, usdt_amount, side):
        try:
            price = self.get_orderbook_price(symbol, side)
            quantity = usdt_amount / price
            return float(self.exchange.amount_to_precision(symbol, quantity))
        except Exception as e:
            print(f"[Error] Binance - Can't convert usdt to quantity for {symbol}: {e}")

    # 合约下市价单
    # create_order里，amount实际指quantity
    def place_market_order(self, symbol, side, amount):

        positionSide = None
        if side=='long':
            side = 'BUY'
            positionSide='LONG'
        elif side=='short':
            side = 'SELL'
            positionSide = 'SHORT'

        try:
            q = self.usdt_to_quantity(symbol=symbol, usdt_amount=amount)
            future_order = self.exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=q,
                params={'positionSide': positionSide}
            )
            print("Binance Future market order is placed: ", future_order)
            return future_order

        except Exception as e:
            print(f"[Error] Binance - Can't place future market order for {symbol}: {e}")

    # 市价关仓
    def close_position(self, symbol, position):
        # position='long' 表示平多（卖出），position='short' 表示平空（买入）
        if position == 'long':
            side = 'SELL'
            positionSide = 'LONG'
        elif position == 'short':
            side = 'BUY'
            positionSide = 'SHORT'

        try:
            positions = self.exchange.fetch_positions([symbol], params={})
            position_qty = 0
            for pos in positions:
                if pos['info']['symbol'] == symbol and pos['side'].lower() == position:
                    position_qty = abs(pos['contracts'])
            if position_qty > 0:
                close_order = self.exchange.create_order(symbol=symbol,
                                                  type='market',
                                                  side=side,
                                                  amount=position_qty,
                                                  params={
                'positionSide': positionSide
            })
                return close_order
        except Exception as e:
            print(f"[Error] Binance - Can't close future market for {symbol}: {e}")


class GateFuturesTrader:
    # Gate的symbol格式为：BTC_USDT
    def __init__(self):
        # ccxt initialization
        self.exchange = ccxt.gateio({
            'apiKey': GATEIO_API_KEY,
            'secret': GATEIO_API_SECRET,
            'options': {
                'defaultType': 'swap' # gate里的永续合约是swap
            }
        })
        self.exchange.httpsProxy = GATE_PROXY
        self.markets = self.exchange.load_markets()

        # Gate api initialization
        self.config = Configuration(key=GATEIO_API_KEY, secret=GATEIO_API_SECRET)
        self.config.proxy = GATE_PROXY

        self.api_client = ApiClient(self.config)
        self.futures_api = FuturesApi(self.api_client)

    # 设置杠杆
    def set_leverage(self, symbol, leverage):
        try:
            response = self.futures_api.update_position_leverage(
                settle="usdt",
                contract=symbol,
                leverage=str(leverage)  # 杠杆倍数为字符串类型
            )
            return response
        except ApiException as e:
            print(f"❌ 设置杠杆时出错: {e}")

    # 查询合约usdt余额
    def get_available_balance(self):
        try:
            balance_info = self.futures_api.list_futures_accounts(settle='usdt')
            return float(balance_info.available)
        except ApiException as e:
            print(f"❌ 获取 Gate 合约账户余额出错: {e}")

    # 根据合约下单方向，获取相应的实时orderbook price
    def get_orderbook_price(self, symbol, side):
        ob = self.exchange.fetch_order_book(symbol)
        if side == 'long':  # 做多需要buy
            return float(ob['asks'][0][0])  # best ask price for buy
        elif side == 'short':  # 做空需要sell
            return float(ob['bids'][0][0])  # best bid price for sell

    # Gateio获取单个合约规格
    def get_quanto_multiplier(self, symbol):
        try:
            info = self.futures_api.get_futures_contract(settle='usdt', contract=symbol)
            return float(info.quanto_multiplier)  # 每一张合约面值
        except Exception as e:
            print(f"获取合约规格时出错: {e}")

    # 转化下单金额至size
    # 这里面的quanto_multiplier要靠gate的官方api获取,ccxt有误
    def usdt_to_size(self, symbol, usdt_amount, side):

        price = self.get_orderbook_price(symbol, side) # 最新ob价格
        quanto = self.get_quanto_multiplier(symbol=symbol)
        size_raw = usdt_amount / (price * quanto)

        return float(self.exchange.amount_to_precision(symbol, size_raw))

    # 合约市价下单
    # amount对应是size
    def place_market_order(self, symbol, side, amount):

        size = self.usdt_to_size(symbol=symbol, usdt_amount=amount, side=side)
        if side == 'long':
            size = abs(size)
        elif side == 'short':
            size = -abs(size)

        try:
            order = self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,  # 交易对
                    "size": size,  # 合约数量 (正数为开多)
                    "price": "0",  # 市价单, 价格设置为0
                    "tif": "ioc",  # 立即成交或取消
                    "text": "t-api_market",  # 自定义标签
                    "reduce_only": False,  # 不减仓
                    "close": False  # 开仓
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 开多市价单时出错: {e}")
            return None

    # 市价关仓
    def close_position(self, symbol, position):
        # position='long' 表示平多（卖出），position='short' 表示平空（买入）
        if position == 'long':
            auto_size = 'close_long'
        elif position == 'short':
            auto_size = 'close_short'

        try:
            order = self.futures_api.create_futures_order(
                settle="usdt",
                futures_order={
                    "contract": symbol,  # 交易对
                    "size": 0,  # 平仓, 合约数量为0
                    "price": "0",  # 市价单, 价格设置为0
                    "tif": "ioc",  # 立即成交或取消
                    "text": "t-api_market_close",
                    "reduce_only": True,  # 只减仓
                    "close": False,  # 平仓
                    "auto_size": auto_size  # "close_long" or "close_short"
                }
            )
            return order
        except ApiException as e:
            print(f"❌ 平多市价单时出错: {e}")
            return None


if __name__ == '__main__':

    # bfuture_trader = BinanceFuturesTrader()
    gfuture_trader = GateFuturesTrader()

    # mkt = gfuture_trader.markets['BTC_USDT']
    # print(mkt)

    # bfuture_trader.set_leverage(symbol='ICXUSDT', leverage=1)
    # gfuture_trader.set_leverage(symbol='SOL_USDT', leverage=1)

    # print(bfuture_trader.get_balance())
    # print(gfuture_trader.get_balance())
    # #
    symbol = 'SUI_USDT'
    amount = 20
    side = 'short'
    # # p = gfuture_trader.get_orderbook_price(symbol=symbol, side=side)
    # # print(p)
    #
    # q = gfuture_trader.usdt_to_size(symbol=symbol, usdt_amount=amount, side=side)
    # print(q)

    # gfuture_trader.set_leverage(symbol=symbol, leverage=1)
    # order = gfuture_trader.place_market_order(symbol=symbol, side=side, amount=amount)
    # print(order)

    # positions = gfuture_trader.exchange.fetch_positions([symbol], params={})
    # print(positions)
    # print(positions[0]['info']['contract'])
    # print(positions[0]['side'])
    #
    order = gfuture_trader.close_position(symbol=symbol, position=side)
    print(order)

    # bfuture_trader.set_leverage(symbol=symbol, leverage=1)
    # order = bfuture_trader.place_market_order(symbol=symbol, side=side, amount=amount)
    # order = bfuture_trader.close_position(symbol=symbol, position=side)
    # print(order)



