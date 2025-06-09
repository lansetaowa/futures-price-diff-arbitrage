import asyncio
import json
import aiohttp
from datetime import datetime, timezone

from config import BINANCE_PROXY, GATE_PROXY

class BinanceWSClient:

    base_url = "wss://fstream.binance.com/ws"

    def __init__(self, symbol: str, on_update, proxy: str = BINANCE_PROXY):
        self.symbol = symbol.lower() # Binance symbol like 'btcusdt'
        self.proxy = proxy
        self.on_update = on_update

    async def _run_ws_loop(self, url, handler_func):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=self.proxy) as ws:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await handler_func(msg)
            except Exception as e:
                print(f"[Binance WS Error] {e}")
                await asyncio.sleep(5)

    async def subscribe_mark_price(self):
        url = f"{self.base_url}/{self.symbol}@markPrice"
        await self._run_ws_loop(url, self._handle_mark_price)

    async def _handle_mark_price(self, msg):
        data = json.loads(msg.data)
        mark_price = float(data['p'])
        funding_rate = float(data.get('r', 0))
        self.on_update({
            'source': 'binance',
            'symbol': self.symbol,
            'timestamp': datetime.now(timezone.utc),
            'price': mark_price,
            'funding_rate': funding_rate
        })

    async def subscribe_orderbook(self, depth=5):
        url = f"{self.base_url}/{self.symbol}@depth{depth}"
        await self._run_ws_loop(url, self._handle_orderbook)

    async def _handle_orderbook(self, msg):
        data = json.loads(msg.data)
        orderbook = {
            'bids': data.get('b', [])[:5],
            'asks': data.get('a', [])[:5],
        }
        self.on_update({
            'source': 'binance',
            'symbol': self.symbol,
            'timestamp': datetime.now(timezone.utc),
            'orderbook': orderbook
        })

class GateWSClient:
    base_url = "wss://fx-ws.gateio.ws/v4/ws/usdt"

    def __init__(self, symbol: str, on_update, proxy: str = GATE_PROXY):
        self.symbol = symbol.upper()
        self.proxy = proxy
        self.on_update = on_update

    async def _run_ws_loop(self, url, subscribe_msg, handler_func):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, proxy=self.proxy) as ws:
                        await ws.send_json(subscribe_msg)
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await handler_func(msg)
            except Exception as e:
                print(f"[Gate WS Error] {e}")
                await asyncio.sleep(5)

    async def subscribe_ticker(self, channel="futures.tickers"):
        subscribe_msg = {
            "time": int(datetime.now(timezone.utc).timestamp()),
            "channel": channel,
            "event": "subscribe",
            "payload": [self.symbol]
        }
        await self._run_ws_loop(self.base_url, subscribe_msg, self._handle_ticker)

    async def _handle_ticker(self, msg):
        data = json.loads(msg.data)
        if data.get("event") == "update":
            try:
                ticker = data["result"][0]
                mark_price = float(ticker["mark_price"])
                self.on_update({
                    'source': 'gate',
                    'symbol': self.symbol,
                    'timestamp': datetime.now(timezone.utc),
                    'price': mark_price,
                    'funding_rate': None  # 你也可以在 future 做延迟更新
                })
            except Exception as e:
                print(f"[Gate Parse Error] {e}")

    async def subscribe_orderbook(self, depth=20, interval='100ms'):
        subscribe_msg = {
            "time": int(datetime.now(timezone.utc).timestamp()),
            "channel": "futures.order_book_update",
            "event": "subscribe",
            "payload": [self.symbol, interval, str(depth)]
        }
        await self._run_ws_loop(url=self.base_url,
                                subscribe_msg=subscribe_msg,
                                handler_func=self._handle_orderbook)

    async def _handle_orderbook(self, msg):
        data = json.loads(msg.data)
        if data.get("event") == "update":
            try:
                result = data.get("result", {})
                bids = result.get('b', []) or result.get('bids', [])
                asks = result.get('a', []) or result.get('asks', [])

                orderbook = {
                'bids': [(float(bid['p']), float(bid['s'])) for bid in bids[:5]] if bids else [],
                'asks': [(float(ask['p']), float(ask['s'])) for ask in asks[:5]] if asks else [],
            }
                self.on_update({
                    'source': 'gate',
                    'symbol': self.symbol,
                    'timestamp': datetime.now(timezone.utc),
                    'orderbook': orderbook
                })
            except Exception as e:
                print(f"[Gate Orderbook Parse Error] {e}")

if __name__ == "__main__":
    from pprint import pprint
    from shared_data import SharedMarketData

    shared_data = SharedMarketData()

    def on_update_handler(data):
        shared_data.update(data)

    async def print_snapshot_loop():
        while True:
            snapshot = shared_data.get_snapshot()
            if snapshot:
                print("\n[Snapshot @", asyncio.get_running_loop().time(), "]")
                pprint(snapshot)
            await asyncio.sleep(2)

    async def main():
        binance = BinanceWSClient(symbol="rvnusdt", on_update=on_update_handler)
        gate = GateWSClient(symbol="RVN_USDT", on_update=on_update_handler)

        await asyncio.gather(
            # binance.subscribe_mark_price(),
            # gate.subscribe_ticker(),
            binance.subscribe_orderbook(),
            gate.subscribe_orderbook(),
            print_snapshot_loop()
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")