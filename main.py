from Init.init import startup
import Data.Technical_Data.Retrieve_Data.retrieve_technical as bulk
import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment_bulk as rs
if __name__ == '__main__':
    # bulk.retrieve_technical_bulk().run_data_load(range='historical')
    # startup()
    rs.retrieve_technical_bulk().run_data_load(range='historical')








