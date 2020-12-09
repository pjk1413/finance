import Data.config_read as read_values

def list_of_schema():
    config = read_values.config()
    return [config.stock_db_name, config.sentiment_db_name,
                  config.utility_db_name, config.predict_db_name, config.fundamental_db_name]
