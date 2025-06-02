class SharedMarketData:
    def __init__(self):
        self.snapshot = {}

    def update(self, data: dict):
        symbol = data['symbol']
        source = data['source']
        self.snapshot[f"{source}_{symbol}"] = data

    def get_snapshot(self):
        return self.snapshot.copy()  # 返回副本避免线程/协程问题