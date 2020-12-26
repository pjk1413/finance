from Init.init import startup
import Data.Technical_Data.Retrieve_Data.retrieve_technical as bulk

if __name__ == '__main__':
    bulk.retrieve_technical_bulk().run_data_load(range='historical')

    # startup()







