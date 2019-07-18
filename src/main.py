from loguru import logger

logger.add("../logs/Sync_scanner.log", rotation="12:00", compression="zip")
logger.debug("That's it, beautiful and simple logging!")
 