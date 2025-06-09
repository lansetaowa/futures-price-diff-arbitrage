"""
获取相关数据的模块：
- gate/binance 实时资金费率，以及下次的资金费率
"""

import pandas as pd
from datetime import datetime

from gate_api import FuturesApi, Configuration, ApiClient
from binance.client import Client
from config import BINANCE_PROXY, GATE_PROXY

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class GateDataHandler:

    def __init__(self, gate_key=None, gate_secret=None):

        self.config = Configuration(key=gate_key, secret=gate_secret)
        self.config.proxy = GATE_PROXY

        self.api_client = ApiClient(self.config)
        self.futures_api = FuturesApi(self.api_client)

    # Gateio所有合约实时资金费率
    def gate_get_funding_rates(self, symbol_filter="usdt"):

        contracts = self.futures_api.list_futures_contracts(settle=symbol_filter)
        df = pd.DataFrame([{
            'symbol': c.name,
            'mark_price': c.mark_price,
            'gate_funding_rate': c.funding_rate,
            'next_funding_time': c.funding_next_apply,
        } for c in contracts])

        df['gate_funding_rate'] = df['gate_funding_rate'].astype(float)
        df['mark_price'] = df['mark_price'].astype(float)
        df['symbol_renamed'] = df['symbol'].apply(lambda x: x.replace("_", ""))
        df['next_funding_time'] = pd.to_datetime(df['next_funding_time'], unit='s')

        df.sort_values(by="gate_funding_rate", ascending=False, inplace=True)

        return df

    # Gateio单个合约的实时funding rate
    def get_funding_rate(self, symbol):
        try:
            info = self.futures_api.get_futures_contract(settle='usdt', contract=symbol)
            return info.funding_rate
        except Exception as e:
            print(f"[Gate FR] 获取 {symbol} 资金费率失败: {e}")
            return 0.0001

    # Gate上某合约的资金费率历史
    def get_funding_rate_history(self, symbol, limit=500):
        try:
            fr_history = self.futures_api.list_futures_funding_rate_history(settle='usdt', contract=symbol, limit = limit)
            df = pd.DataFrame([{'funding_rate':f.r,
                                'funding_ts':f.t} for f in fr_history])
            df['funding_time'] = pd.to_datetime(df['funding_ts'], unit='s')
            df['funding_rate'] = df['funding_rate'].astype(float)
            df['symbol'] = symbol
            df.rename(columns={'funding_rate': "gate_fr"}, inplace=True)

            return df

        except Exception as e:
            print(f"❌ Error fetching Gate.io funding rate history for {symbol}: {e}")
            return None

    # 近期所有symbol的合约资金费率历史
    def get_all_funding_rate_histories(self, limit=100):
        all_symbols_df = self.gate_get_funding_rates()
        symbols = all_symbols_df['symbol'].tolist()

        all_fr_rows = []

        for symbol in symbols:
            print(f"Fetching funding rate histories for {symbol}...")
            df = self.get_funding_rate_history(symbol, limit=limit)
            if df is not None and not df.empty:
                df['symbol'] = symbol
                all_fr_rows.append(df)

        all_funding_df = pd.concat(all_fr_rows, ignore_index=True)

        return all_funding_df

    # Gate获取某合约的K线数据
    def get_future_klines(self, symbol, ts_from: int = None, interval='1m', limit=10, settle: str = 'usdt'):
        """
                获取 Gate.io 某合约的历史 K 线数据。

                参数：
                - symbol: 合约标识（如 BTC_USDT）
                - ts_from: 起始时间timestamp，int
                - interval: K 线周期（如 '1m', '5m', '1h', '4h', '1d'）
                - limit: 返回多少个
                - settle: 结算币种，默认 'usdt'

                返回：
                - pd.DataFrame，包含 timestamp, open, high, low, close, volume, sum
                """

        # 获取数据
        klines = self.futures_api.list_futures_candlesticks(
            settle=settle,
            contract=symbol,
            _from=ts_from,
            interval=interval,
            limit=limit
        )

        # 转为 DataFrame
        df = pd.DataFrame([{
            'timestamp': k.t,
            'time': pd.to_datetime(k.t, unit='s'),
            'open': float(k.o),
            'high': float(k.h),
            'low': float(k.l),
            'close': float(k.c),
            'volume': float(k.v) if hasattr(k, 'v') else None,
            'sum': float(k.sum) if hasattr(k, 'sum') else None
        } for k in klines])

        return df

    # 某symbol过去24小时交易量
    def get_24tradevol(self, symbol):

        try:
            tickers = self.futures_api.list_futures_tickers(settle='usdt')
            for t in tickers:
                if t.contract == symbol:
                    # print(t)
                    return t.volume_24h_settle

        except Exception as e:
            print(f"Can't get futures trade volume info: {e}")

    # 所有tickers
    def get_tickers(self):

        try:
            tickers = self.futures_api.list_futures_tickers(settle='usdt')
            df = pd.DataFrame([{
                "symbol": t.contract,
                "last": t.last,
                "vol_usdt": t.volume_24h_settle
            } for t in tickers])
            return df
        except Exception as e:
            print(f"Can't get futures tickers: {e}")

class BinanceDataHandler:

    def __init__(self, api_key=None, api_secret=None):

        self.client = Client(api_key, api_secret,
                             requests_params={
                'proxies': {
                    'http': BINANCE_PROXY,
                    'https': BINANCE_PROXY,
                    }
                })

    @staticmethod
    def transform_df(df):
        df = df.iloc[:, :6]
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Date'] = pd.to_datetime(df['Date'], unit='ms')
        df.set_index('Date', inplace=True)
        # df.index = df.index.tz_convert('America/New_York')
        df = df.astype(float)

        return df

    # 获取合约k线数据
    def get_future_klines(self, symbol, interval='1m', start_str=None, end_str=None, limit=1000):

        if start_str:
            starttime = int(datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        if end_str:
            endtime = int(datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

        try:
            raw_data = self.client.futures_klines(symbol=symbol,
                                                  interval=interval,
                                                  startTime=(starttime if start_str else None),
                                                  endTime=(endtime if end_str else None),
                                                  limit=limit)

            df = pd.DataFrame(raw_data)

            return self.transform_df(df)

        except Exception as e:
            print(f"Can't get futures kline: {e}")

    # 获取合约历史资金费率
    def get_funding_rate_history(self, symbol, start_str=None, end_str=None, limit=1000):

        if start_str:
            starttime = int(datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
        if end_str:
            endtime = int(datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

        try:
            raw_data = self.client.futures_funding_rate(symbol=symbol,
                                                  startTime=(starttime if start_str else None),
                                                  endTime=(endtime if end_str else None),
                                                  limit=limit)

            df = pd.DataFrame(raw_data)
            df['Date'] = pd.to_datetime(df['fundingTime']//1000, unit = 's')
            df.rename(columns={'fundingRate':"binance_fr"}, inplace=True)

            return df

        except Exception as e:
            print(f"Can't get futures funding rate: {e}")

    # Binance上所有合约symbol的status
    def bi_get_all_contract_status(self):
        data = self.client.futures_exchange_info()
        symbols = data.get('symbols', [])
        rows = []
        for symbol_info in symbols:
            rows.append({
                'symbol': symbol_info['symbol'],
                'status': symbol_info['status']
            })
        df = pd.DataFrame(rows)

        return df

    # Binance上某symbol的过去24小时的成交额
    def get_24tradevol(self, symbol):
        try:
            ticker = self.client.futures_ticker(symbol=symbol)
            return float(ticker['quoteVolume'])
        except Exception as e:
            print(f"Can't get futures trade volume info: {e}")

if __name__ == '__main__':

    gdata_handler = GateDataHandler()
    bdata_handler = BinanceDataHandler()

    print()

    # v = gdata_handler.get_24tradevol(symbol='BEAMX_USDT')
    # print(v)
    # df = bdata_handler.bi_get_all_contract_status()
    # print(df[df['symbol']=='RAYUSDT'])

    # df = bdata_handler.get_funding_rate_history(symbol='AIOTUSDT')
    # print(df.info())
    # print(df.head())

    # b_df = bdata_handler.get_future_klines(symbol='BTCUSDT', interval='1m', limit=1000)
    # # print(b_df.info())
    # print(b_df.head())
    #
    # fr_df = gdata_handler.get_funding_rate_history(symbol='AIOT_USDT')
    # print(fr_df.info())
    # print(fr_df.head())
    #
    # merged_df = AnalysisUtils.merge_fr(df, fr_df)
    # print(merged_df.info())
    # print(merged_df.head())
    # #
    # start_str = '2025-05-15 12:00:00'
    # ts_from = int(time.mktime(datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").timetuple()))
    # print(ts_from)
    # ts_from = 1748404800 - 60*30
    # g_df = gdata_handler.get_future_klines(symbol='BTC_USDT',
    #                                       ts_from=None,
    #                                       interval='1m',limit=1000)
    # print(g_df.head())
    #
    # merged_df = AnalysisUtils.merge_klines(b_df, g_df)
    # print(merged_df.head())
    # print(merged_df.info())
    #
    # close_rt = AnalysisUtils.calc_close_return(df, window=6)
    # print(close_rt)

    # modified_df = AnalysisUtils.add_return_to_funding_df(funding_df=fr_df,gdata_handler=gdata_handler,bar_limit=3,interval='5m')
    # print(modified_df)
    #
    # slope = ArbitrageUtils.calc_close_slope(df)
    # print(slope)
