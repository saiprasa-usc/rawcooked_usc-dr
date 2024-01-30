# utils/logging_utils.py

import datetime


def log(logfile, message):
    """
    Function to append message to a log file
    @param logfile: The path of the log file
    @param message: The message that we want to append
    """

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d - %H:%M:%S")
    log_message = f"{timestamp} - {message}"
    with open(logfile, "a") as file:
        file.write(log_message + "\n")
    print(log_message)
