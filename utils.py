import logging
import platform

system = platform.system()

LOG_LEVEL = logging.INFO
LOG_FORMAT = ("[%(asctime)s] %(filename)s:%(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")


class Color:
    gray = "\u001b[38;5;240m"
    yellow = "\u001b[38;5;220m"
    red = "\u001b[38;5;196m"
    magenta = "\u001b[38;5;163m"
    reset = "\u001b[0m"


class ColorFormatter(logging.Formatter):
    """Logging formatter that highlights with ANSI color"""

    FORMATS = {
        logging.DEBUG: f"{Color.gray}{LOG_FORMAT[0]}{Color.reset}",
        logging.INFO: LOG_FORMAT[0],
        logging.WARNING: f"{Color.yellow}{LOG_FORMAT[0]}{Color.reset}",
        logging.ERROR: f"{Color.red}{LOG_FORMAT[0]}{Color.reset}",
        logging.CRITICAL: f"{Color.magenta}{LOG_FORMAT[0]}{Color.reset}",
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(fmt=log_fmt, datefmt=LOG_FORMAT[1])
        return formatter.format(record)


stdout_handler = logging.StreamHandler()
stdout_handler.setFormatter(ColorFormatter())

file_handler = logging.FileHandler("poker.log")
file_handler.setLevel(LOG_LEVEL)
formatter = logging.Formatter(fmt=LOG_FORMAT[0], datefmt=LOG_FORMAT[1])
file_handler.setFormatter(formatter)

logger = logging.getLogger("PokerHandsDataset")
logger.addHandler(stdout_handler)
logger.addHandler(file_handler)
logger.setLevel(LOG_LEVEL)