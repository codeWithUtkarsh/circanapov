import os
from datetime import datetime
import pyfiglet



LOG_DIR = "logs"

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def log_message(message: str):
    """Logs a message with a timestamp to the log file."""
    timestamp = datetime.now().isoformat()
    log_file = os.path.join(LOG_DIR, "scraping_log.txt")
    with open(log_file, "a") as file:
        file.write(f"{timestamp} - {message}\n")
    print(f'log_info:{message}')

def print_banner(message: str):
    banner = pyfiglet.figlet_format(message)
    print(banner)