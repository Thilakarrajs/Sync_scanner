from loguru import logger

class SyncScannerUtil:
    @staticmethod
    def _build_query(queryStr, offsetvalue, numberOfRecords, primaryKey):
        try:
             queryStr = queryStr + " order by " + primaryKey +" asc limit "+str(numberOfRecords) +" offset " + str(offsetvalue)
             #logger.info("Builed query "+ queryStr)
             return queryStr;
        except Exception as e:
            logger.error(e)