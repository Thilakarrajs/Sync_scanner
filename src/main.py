import time
from loguru import logger
from Processor import ScanProcessor

start_time = time.time()
logger.add("../logs/Sync_scanner.log", rotation="12:00", compression="zip")
processor = ScanProcessor()
coreList = processor.initProcessor()
processor.destroyProcessor()
logger.info("--- %s Total seconds ---" % (time.time() - start_time))
