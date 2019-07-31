from loguru import logger

class SyncScannerUtil:
    @staticmethod
    def _build_query(queryStr, offsetvalue, numberOfRecords, primaryKey):
        try:
             queryStr = queryStr + " order by " + primaryKey +" desc limit "+str(numberOfRecords) +" offset " + str(offsetvalue)
             #logger.info("Builed query "+ queryStr)
             return queryStr;
        except Exception as e:
            logger.error(e)
            
    @staticmethod
    def _build_missing_query(queryStr, primaryKey):
        try:
             queryStr = queryStr.split("where",1)[0]  
             queryStr =  queryStr + " where " +primaryKey + " in (%s)"  
             #logger.info("Builed query "+ queryStr)
             #exit(0)
             return queryStr;
        except Exception as e:
            logger.error(e)