import sys

list_args = sys.argv

for arg in list_args:
    if arg == 'run_schedule':
        import Init.schedule as schedule
        schedule.run_schedule()
    if arg == 'retrieve_technical_data':
        import Data.Technical_Data.Retrieve_Data.retrieve_technical as rt
        rt.retrieve_technical_bulk().run_data_load(range='historical')
    if arg == 'retrieve_sentiment_data':
        import Data.Sentiment_Data.Retrieve_Data.retrieve_sentiment as rs
        rs.retrieve_sentiment_data().run_data_load(range='historical')

