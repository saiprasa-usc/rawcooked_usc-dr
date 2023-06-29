# utils/logging_utils.py

import datetime

def log(logfile, message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d - %H:%M:%S")
    log_message = f"{message} - {timestamp}"
    with open(logfile, "a") as file:
        file.write(log_message + "\n")
    print(log_message)
