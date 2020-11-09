

class ticker_import_txt:
    def __init__(self):
        pass

    def read_ticker_symbol(self, path):
        file = open(path, 'r')
        Lines = file.readlines()

        for line in Lines:
            ticker_symbol = line.split("\t")[0]
            description = line.split("\t")[1]
            print(ticker_symbol)
            print(description)
            print("-----------------------")

