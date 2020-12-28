import Database.Service.email_service as send_email
import datetime

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# TODO create a robust logging system
def log_status(message, location, print=False, log=True, email=False):
    STATUS = f"STATUS {current_time}: @{location} - {message}"
    if print:
        print(STATUS)
    if log:
        filename = datetime.datetime.now().strftime('%Y-%m-%d')
        f = open(f"./Logger/logs/{filename}_STATUS_LOG.txt", "a")
        f.write(STATUS)
    if email:
        send_email.send_email().send_email(STATUS)

def log_error(message, location, print=False, log=True, email=False):
    ERROR = f"ERROR {current_time}: @{location} - {message}"
    if print:
        print(ERROR)
    if log:
        filename = datetime.datetime.now().strftime('%Y-%m-%d')
        f = open(f"./Logger/logs/{filename}_ERROR_LOG.txt", "a")
        f.write(ERROR)
    if email:
        send_email.send_email().send_email(ERROR)



