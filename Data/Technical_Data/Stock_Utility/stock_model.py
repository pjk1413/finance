

"""

"""
class stock_daily:
    """
    daily stock model for storing and transferring data to and from the database
    """
    def __init__(self, symbol, open, close, high, low, adj_close, volume):
        self.symbol = symbol
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.adj_close = adj_close
        self.volume = volume
        self.dividend = None
        self.split = None
        self.sma = None
        self.ema = None
        self.macd = None
        self.stoch = None
        self.rsi = None
        self.willr = None
        self.adx = None
        self.cci = None
        self.aroon = None
        self.bbands = None
        self.sar = None
        self.ad = None
        self.obv = None

    def set_indicators(self):
        pass

    def export_json(self):
        pass

