import mysql.connector
from loguru import logger
class DataBaseConnector:
    def __init__(self,host='',userName='',password='',port=0,schemaName=''):
        self.host = host
        self.userName = userName
        self.password=password
        self.port=port
        self.schemaName=schemaName
        
        try:
           self._db_connection = mysql.connector.connect(
                  host=self.host,
                  user=self.userName,
                  passwd=self.password,
                  database=self.schemaName)
           self._db_cur = self._db_connection.cursor()
           logger.info('database connected with host '+self.host)
           logger.info('Selected database schema name '+self.schemaName)
         
        except Exception as e:
            logger.error(e)
    
    def _close_db_connection(self):
        try:
            self._db_cur.close()
            self._db_connection.close()
            logger.info('database dis-connected with host '+self.host)
        except Exception as e:
            logger.error(e)