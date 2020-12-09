




class stock_series:
    """retrieve stock data in various intervals"""
    def __init__(self, symbol):
        self.symbol = symbol
        self.list_week = []
        self.list_month = []
        self.list_quarter = []
        self.list_semi_annual = []
        self.list_year = []
