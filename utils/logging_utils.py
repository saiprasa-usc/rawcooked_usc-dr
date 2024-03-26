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


def write_permanent_logs(source_temp_file, target_file):
    """Function to permanently write temp log files to the central log file

    This function moves the files that are created during script execution to the central
    permanent log file of that script.
    :param source_temp_file: temporary log file created during script execution
    :param target_file: centralized log file for each script
    """

    with open(target_file, 'a') as target:
        with open(source_temp_file, 'r') as source:
            target.write(source.read())