

class zirra_api:
    def __init__(self, symbol, date_range):
        self.symbol = symbol
        self.date_range = date_range
        self.validate = False

    def validate_data(self, data):
        return True

    def clean_data(self, data):
        pass

    def clean_date_range(self):
        pass

    def get_stock_data_single(self):
        return ""

    def get_stock_data_multi(self):
        return ""

    def get_stock_data(self):
        return ""