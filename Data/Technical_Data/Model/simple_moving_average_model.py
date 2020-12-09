


class sma:
    """stores, retrieves, calculates simple moving average for period high, low, open, close"""
    def __init__(self):
        self.high_sma = None
        self.low_sma = None
        self.open_sma = None
        self.close_sma = None
        self.adj_close_sma = None

    @staticmethod
    def to_database(list_of_entries):
        str = ""
        for entry in list_of_entries:
            str += f"{entry}, "
        if str[len(str)-2:len(str)] == ", ":
            str = str[0:len(str)-2]
        return str

    @staticmethod
    def parse_from_database(entry):
        if entry is not None:
            entry = entry.split(" : ")
            list = []
            for x in entry:
                list.append(float(x.strip()))
            return list
