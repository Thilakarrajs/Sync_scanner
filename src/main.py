from loguru import logger
from Processor import ScanProcessor

logger.add("../logs/Sync_scanner.log", rotation="12:00", compression="zip")
processor = ScanProcessor()
processor.initProcessor()
