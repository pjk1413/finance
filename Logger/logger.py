import Database.Service.email_service as send_email
import datetime

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# TODO create a robust logging system
def log_status(message, location, to_print=False, log=True, email=False):
    STATUS = f"STATUS {current_time}: @{location} - {message}"
    if to_print:
        print(STATUS)
    if log:
        filename = datetime.datetime.now().strftime('%Y-%m-%d')
        f = open(f"./Logger/logs/{filename}_STATUS_LOG.txt", "a")
        f.write(STATUS + "\n")
    if email:
        send_email.send_email().send_email(STATUS)

def log_error(message, location, to_print=False, log=True, email=False):
    ERROR = f"ERROR {current_time}: @{location} - {message}"
    if to_print:
        print(ERROR)
    if log:
        filename = datetime.datetime.now().strftime('%Y-%m-%d')
        f = open(f"./Logger/logs/{filename}_ERROR_LOG.txt", "a")
        f.write(ERROR + "\n")
    if email:
        send_email.send_email().send_email(ERROR)



