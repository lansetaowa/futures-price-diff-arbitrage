import pandas as pd

class ArbitrageBacktester:

    def __init__(self, df, upper_threshold=0.006, lower_threshold=-0.006, fee_rate=0.0005, init_capital=10000):
        self.df = df.copy()
        self.upper_threshold = upper_threshold
        self.lower_threshold = lower_threshold
        self.fee_rate = fee_rate
        self.init_capital = init_capital

        self.position = None  # 'long_binance' or 'short_binance'
        self.entry_price_b = None
        self.entry_price_g = None
        self.entry_time = None
        self.entry_notional = None  # 单边名义本金金额
        self.pnl_history = []
        self.current_pnl = 0.0

    def run(self):
        for dt, row in self.df.iterrows():
            diff_pct = row['diff_pct']
            b_price = row['b_close']
            g_price = row['g_close']
            b_fr = self._safe_float(row['binance_fr'])
            g_fr = self._safe_float(row['gate_fr'])

            # 开仓
            if self.position is None:
                if diff_pct > self.upper_threshold:
                    self._open_position(direction="short_binance", b_price=b_price, g_price=g_price, dt=dt)
                elif diff_pct < self.lower_threshold:
                    self._open_position(direction="long_binance", b_price=b_price, g_price=g_price, dt=dt)
                continue

            # apply资金费率
            if not pd.isna(b_fr) and not pd.isna(g_fr):
                self._apply_funding_fee(b_fr=b_fr, g_fr=g_fr, dt=dt)

            # 平仓
            if self.position == 'short_binance' and diff_pct <=0:
                self._close_position(b_price=b_price, g_price=g_price, dt=dt)
            elif self.position == 'long_binance' and diff_pct >=0:
                self._close_position(b_price=b_price, g_price=g_price, dt=dt)

        # 回测结束，若还有position就强制平仓
        if self.position is not None:
            self._close_position(b_price=b_price, g_price=g_price, dt=dt, forced=True)

        return pd.DataFrame(self.pnl_history)

    @staticmethod
    def _safe_float(val):
        if pd.isna(val):
            return None
        try:
            return float(str(val).strip())
        except:
            return None

    def _open_position(self, direction, b_price, g_price, dt):
        self.position = direction
        self.entry_price_b = b_price
        self.entry_price_g = g_price
        self.entry_time = dt

        # 每边按照初始资金做全仓
        self.entry_notional = self.init_capital
        b_fee = self.entry_notional * self.fee_rate
        g_fee = self.entry_notional * self.fee_rate
        self.current_pnl -= (b_fee + g_fee)

        print(f"[OPEN] {dt} 开仓方向: {direction}, b_price: {b_price:.5f}, g_price: {g_price:.5f}, 手续费: {b_fee + g_fee:.2f}")

        self.pnl_history.append({
            'type': 'open_position',
            'time': dt,
            'position': self.position,
            'pnl': - (b_fee + g_fee),
            'duration_minutes': None
        })

    def _apply_funding_fee(self, b_fr, g_fr, dt):
        funding_pnl = 0
        if self.position == 'short_binance': # 币安做空，gate做多
            funding_pnl += self.entry_notional * b_fr # 做空的position的funding_pnl和funding rate符号相同
            funding_pnl -= self.entry_notional * g_fr
        elif self.position == 'long_binance': # 币安做多，gate做空
            funding_pnl -= self.entry_notional * b_fr
            funding_pnl += self.entry_notional * g_fr

        self.current_pnl += funding_pnl

        print(f"[FUNDING] {dt} 方向: {self.position}, binance_fr: {b_fr:.6f}, gate_fr: {g_fr:.6f}, 资金费用: {funding_pnl:.2f}")

        self.pnl_history.append({
            'type': 'funding',
            'time': dt,
            'position': self.position,
            'pnl': funding_pnl,
            'duration_minutes': None
        })

    def _close_position(self, b_price, g_price, dt, forced=False):
        if self.position == 'short_binance': # 币安做空，gate做多
            pnl = (self.entry_price_b - b_price) * self.entry_notional / self.entry_price_b + \
                  (g_price - self.entry_price_g) * self.entry_notional / self.entry_price_g
        elif self.position == 'long_binance': # 币安做多，gate做空
            pnl = (b_price - self.entry_price_b) * self.entry_notional / self.entry_price_b + \
                  (self.entry_price_g - g_price) * self.entry_notional / self.entry_price_g
        else:
            pnl = 0

        # 平仓手续费
        b_fee = self.entry_notional * self.fee_rate
        g_fee = self.entry_notional * self.fee_rate
        pnl -= (b_fee + g_fee)

        self.current_pnl += pnl

        duration_minute = (dt - self.entry_time).total_seconds() / 60

        print(f"[CLOSE] {dt} 平仓方向: {self.position}, b_price: {b_price:.5f}, g_price: {g_price:.5f}, 盈亏: {pnl:.2f}, 持仓时间: {duration_minute:.1f} min, 强平: {forced}")

        self.pnl_history.append({
            'type': 'close_position',
            'time': dt,
            'position': self.position,
            'pnl': pnl,
            'duration_minutes': duration_minute,
            'forced_exit': forced
        })

        # 清空仓位
        self.position = None
        self.entry_price_b = None
        self.entry_price_g = None
        self.entry_time = None
        self.entry_notional = None
        self.current_pnl = 0

if __name__ == '__main__':

    from analysis_utils import AnalysisUtils

    symbol = 'BIDUSDT'
    analyzer = AnalysisUtils()
    df = analyzer.merge_diff_fr(symbol)

    bt = ArbitrageBacktester(df, upper_threshold=0.008, lower_threshold=-0.005)
    result_df = bt.run()

    # 查看策略盈亏和持仓详情
    print(result_df)
    print(f"\n总盈亏：{result_df['pnl'].sum():.4f}")
    print(f"总交易次数：{(result_df['type'] == 'close_position').sum()}")
    print(f"资金费用记录次数：{(result_df['type'] == 'funding').sum()}")

