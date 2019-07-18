from loguru import logger
from ConfigReader import ConfigurationReader
from DataBaseConnector import DataBaseConnector

class ScanProcessor:
    global primaryDB
    global localDB
    global secondaryDB
    def initProcessor(self):
        logger.info("Sync Scanner started ")
        try:
            readerObj = ConfigurationReader() 
            primaryDB = DataBaseConnector(readerObj.getConfigValue('PRIMARY_DB', 'HOST'), readerObj.getConfigValue('PRIMARY_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('PRIMARY_DB', 'PASSWORD'),readerObj.getConfigValue('PRIMARY_DB', 'PORT'), readerObj.getConfigValue('PRIMARY_DB', 'DATABASE_NAME')) 
            
            secondaryDB = DataBaseConnector(readerObj.getConfigValue('SECONDARY_DB', 'HOST'), readerObj.getConfigValue('SECONDARY_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('SECONDARY_DB', 'PASSWORD'),readerObj.getConfigValue('SECONDARY_DB', 'PORT'), readerObj.getConfigValue('SECONDARY_DB', 'DATABASE_NAME'))
            
            localDB = DataBaseConnector(readerObj.getConfigValue('LOCAL_DB', 'HOST'), readerObj.getConfigValue('LOCAL_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('LOCAL_DB', 'PASSWORD'),readerObj.getConfigValue('LOCAL_DB', 'PORT'), readerObj.getConfigValue('LOCAL_DB', 'DATABASE_NAME'))
            
            
        except Exception as e:
            logger.error(e)
        finally:
            primaryDB._close_db_connection()
            secondaryDB._close_db_connection()
            localDB._close_db_connection()