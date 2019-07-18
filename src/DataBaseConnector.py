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
                  database=self.schemaName,
                  port=self.port)
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
            
    def _get_db_records(self,queryStr):
        try:
            self._db_cur.execute(queryStr)
            #dataRows = self._db_cur.fetchall()
            result = []
            columns = tuple( [d[0]  for d in self._db_cur.description] )
            for row in  self._db_cur:
               result.append(dict(zip(columns, row)))
            return result
        except Exception as e:
            logger.error(e)
            
    def _get_scaning_records(self,queryStr,primaryKey):
        try:
            self._db_cur.execute(queryStr)
            resultArray = []
            columns = tuple( [d[0]  for d in self._db_cur.description] )
            for row in  self._db_cur:
                    temp = dict(zip(columns, row))
                    primaryVlaue = temp.get(primaryKey)
                    resultdict = {primaryVlaue :temp}
                    resultArray.append(resultdict)
            logger.info('Total Results: '+str(len(resultArray)))
            return resultArray
        except Exception as e:
            logger.error(e)
            
    def _insert_Scan_results(self, scannerResut):
        try:
            values = (scannerResut.processUUID , scannerResut.typeOfResult,scannerResut.primarySchema,scannerResut.primaryValue,scannerResut.secondarySchema,scannerResut.secondaryValue)
            insertQuery = "INSERT INTO scanner_results ( process_uuid,scan_core, primary_data_schema, primary_data_value, secondary_data_schema, secondary_data_value) VALUES (%s,%s, %s,%s, %s,%s) "
            self._db_cur.execute(insertQuery, values)
            self._db_connection.commit()
            logger.info("data inserted")
        except Exception as e:
            logger.error(e)
            
    def _export_scan_results(self, processUUID):
        try:
            queryStr= queryStr = 'SELECT process_uuid,scan_core, primary_data_schema, primary_data_value, secondary_data_schema, secondary_data_value FROM scanner_results where process_uuid = %s'
            values =(processUUID, )
            self._db_cur.execute(queryStr, values)
            dataRows = self._db_cur.fetchall()
            logger.info('Total _export_scan_results Results: '+str(len(dataRows)))
            return dataRows
        except Exception as e:
            logger.error(e)