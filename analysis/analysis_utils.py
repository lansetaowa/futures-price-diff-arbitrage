import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

from data import BinanceDataHandler, GateDataHandler

pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_rows', None)     # 显示所有行
pd.set_option('display.width', 1000)        # 设置显示宽度
pd.set_option('display.max_colwidth', None) # 设置列内容的最大宽度

class AnalysisUtils:

    def __init__(self):
        self.bdata_handler = BinanceDataHandler()
        self.gdata_handler = GateDataHandler()

    # merge两个平台同个合约的close价格，并计算价差
    @staticmethod
    def merge_klines(binance_df, gate_df):
        binance_df = binance_df.copy()
        binance_df = binance_df['Close']

        gate_df = gate_df.copy()
        gate_df = gate_df[['time','close']].set_index('time')

        merged_df = pd.merge(left=binance_df, right=gate_df, how='inner', left_index=True, right_index=True)
        merged_df.rename(columns={'Close':'b_close', 'close':'g_close'}, inplace=True)

        merged_df['diff_pct'] = (merged_df['b_close'] - merged_df['g_close'])/merged_df['b_close']

        return merged_df

    # 获取两个平台的合约价格历史，merge并计算价差
    def get_futures_diff(self, symbol, interval='1m', start=None, limit=1000):

        b_df = self.bdata_handler.get_future_klines(symbol=symbol, interval=interval, start_str=start, limit=limit)

        if start:
            date_format = "%Y-%m-%d %H:%M:%S"
            dt_object = datetime.strptime(start, date_format)
            ts = dt_object.timestamp()

        g_df = self.gdata_handler.get_future_klines(symbol=symbol.replace("USDT", "_USDT"),
                                               ts_from=(ts if start else None),
                                               interval=interval,
                                               limit=limit)

        merged_df = self.merge_klines(binance_df=b_df, gate_df=g_df)

        return merged_df

    # merge两个平台资金费率
    @staticmethod
    def merge_fr(binance_df, gate_df):

        merged_df = pd.merge(left=binance_df, right=gate_df, left_on='Date', right_on='funding_time', how='inner')

        return merged_df[['funding_time','binance_fr','gate_fr']].set_index('funding_time')

    # 获取两个平台的资金费率历史，并merge
    def get_futures_fr(self, symbol, start=None, limit=1000):
        b_fr = self.bdata_handler.get_funding_rate_history(symbol=symbol, start_str=start, limit=limit)
        g_fr = self.gdata_handler.get_funding_rate_history(symbol=symbol.replace("USDT", "_USDT"), limit=limit)
        merged_df = self.merge_fr(binance_df=b_fr, gate_df=g_fr)

        return merged_df

    # merge上述两个平台的价格历史和资金费率历史
    def merge_diff_fr(self, symbol, interval='5m', limit=1500):
        diff_df = self.get_futures_diff(symbol, interval=interval, limit=limit)
        fr_df = self.get_futures_fr(symbol)
        merged_df = pd.merge(left=diff_df, right=fr_df, left_index=True, right_index=True, how='left')

        return merged_df

    # plot上述两个平台的价格历史和资金费率历史
    @staticmethod
    def plot_diff_fr(merged_df, symbol):
        merged_copy = merged_df.copy()

        merged_copy['diff_pct'] = pd.to_numeric(merged_copy['diff_pct'], errors='coerce')
        merged_copy['binance_fr'] = pd.to_numeric(merged_copy['binance_fr'], errors='coerce')
        merged_copy['gate_fr'] = pd.to_numeric(merged_copy['gate_fr'], errors='coerce')

        plt.figure(figsize=(8, 4))
        plt.plot(merged_copy.index, merged_copy['diff_pct'], label='Future Close Diff Pct', linewidth=1)
        plt.scatter(merged_copy.index, merged_copy['binance_fr'], label='Binance FR', color='orange', marker='o',
                    alpha=0.7)
        plt.scatter(merged_copy.index, merged_copy['gate_fr'], label='Gate FR', color='green', marker='^', alpha=0.7)
        plt.axhline(y=merged_copy['diff_pct'].describe()['50%'], color='red', linestyle='--', linewidth=1)
        plt.xlabel('Date')
        plt.ylabel('Future Close Diff Pct')
        plt.title(f'{symbol} future klines diff pct trend')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    # 完整分析，包含获取数据和plot
    def full_analysis(self, symbol, interval='5m', limit=1500):
        """
        symbol: e.g. "AIOTUSDT"
        """
        merged_df = self.merge_diff_fr(symbol, interval=interval, limit=limit)
        b_vol = self.bdata_handler.get_24tradevol(symbol)
        g_vol = self.gdata_handler.get_24tradevol(symbol=symbol.replace("USDT","_USDT"))
        print(f"binance last 24hour vol in usdt is: {b_vol}")
        print(f"gate last 24hour vol in usdt is: {g_vol}")
        self.plot_diff_fr(merged_df, symbol)

if __name__ == '__main__':

    analyzer = AnalysisUtils()
    df = analyzer.merge_diff_fr(symbol='AIOTUSDT')
    print(df.head())
    print(df.info())