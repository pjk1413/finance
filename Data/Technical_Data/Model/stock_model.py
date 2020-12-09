import inspect

class stock_model:
    """
    daily stock model for storing and transferring data to and from the database
    """
    def __init__(self):
    # def __init__(self, symbol, open, close, high, low, adj_close, volume, date):
        self.symbol = None
        self.date = None
        self.open = None
        self.close = None
        self.high = None
        self.low = None
        self.adj_close = None
        self.volume = None
        self.dividend = None
        self.split = None
        self.sma_ind = None
        self.ema_ind = None
        self.macd_ind = None
        self.stoch_ind = None
        self.rsi_ind = None
        self.willr_ind = None
        self.adx_ind = None
        self.cci_ind = None
        self.aroon_ind = None
        self.bbands_ind = None
        self.psar_ind = None # NEEDS TESTING
        self.ad_ind = None # NEEDS FURTHER TESTING
        self.obv_ind = None # NEEDS FURTHER TESTING


    def get_model(self):
        """
        Takes an empty model and creates a list of attributes that can be used to create a table in a database, automatically updates
        :return: list of string representing attributes of class
        """
        list = []
        for x in inspect.getmembers(self):
            if x[1] == None:
                if not '__' in x[0]:
                    list.append(x[0])
        return list


    def get_model_indicators(self):
        """
                Takes an empty model and creates a list of attributes (only indicators) that can be used to
                create a table in a database, automatically updates
                :return: list of string representing attributes (indicators only) of class
                """
        list = []
        for x in inspect.getmembers(self):
            if x[1] == None:
                if not '__' in x[0]:
                    if '_ind' in x[0]:
                        list.append(x[0])
        return list

    def set_indicators(self):
        pass


    def export_json(self):
        pass

