
import datetime

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# TODO create a robust logging system
def log_status(message, error=None):
    STATUS = f"STATUS {current_time}: {message}"
    print(STATUS)
