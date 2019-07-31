import mysql.connector
import time

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
            start_time = time.time()
            logger.info("Getting Datas from "+self.host)
            self._db_cur.execute(queryStr)
            #dataRows = self._db_cur.fetchall()
            result = []
            columns = tuple( [d[0]  for d in self._db_cur.description] )
            for row in  self._db_cur:
               result.append(dict(zip(columns, row)))
            logger.info("--- %s Total seconds ---" % (time.time() - start_time))
            return result
        except Exception as e:
            logger.error(e)
            
    def _get_scaning_records(self,queryStr,primaryKey):
        try:
            start_time = time.time()
            logger.info("Getting Datas from "+self.host)
            logger.info('Executing query :' +queryStr)
            self._db_cur.execute(queryStr)
            resultArray = []
            columns = tuple( [d[0]  for d in self._db_cur.description] )
            for row in  self._db_cur:
                    temp = dict(zip(columns, row))
                    primaryVlaue = temp.get(primaryKey)
                    resultdict = {primaryVlaue :temp}
                    resultArray.append(resultdict)
            logger.info('Total Results: '+str(len(resultArray)))
            logger.info("--- %s Total seconds for _get_scaning_records---" % (time.time() - start_time))
            return resultArray
        except Exception as e:
            logger.error(e)
     
    def _get_scaning_missing_records(self,queryStr,primaryKey, listData):
        try:
            start_time = time.time()
            logger.info("Getting Datas from "+self.host)
            logger.info('Executing query :' +queryStr)
            tempVar  = ','.join(['%s'] * len(listData))
            self._db_cur.execute(queryStr % tempVar,
                tuple(listData))
            #self._db_cur.execute(queryStr)
            resultArray = []
            columns = tuple( [d[0]  for d in self._db_cur.description] )
            for row in  self._db_cur:
                    temp = dict(zip(columns, row))
                    primaryVlaue = temp.get(primaryKey)
                    resultdict = {primaryVlaue :temp}
                    resultArray.append(resultdict)
            logger.info('Total Results: '+str(len(resultArray)))
            logger.info("--- %s Total seconds for _get_scaning_records---" % (time.time() - start_time))
            return resultArray
        except Exception as e:
            logger.error(e)
            
    def _insert_Scan_results(self, scannerResut):
        try:
            start_time = time.time()
            values = (scannerResut.processUUID , scannerResut.scanCore, scannerResut.typeOfResult,scannerResut.primarySchema,scannerResut.primaryValue,scannerResut.secondarySchema,scannerResut.secondaryValue,scannerResut.primaryKeyValue)
            insertQuery = "INSERT INTO scanner_results ( process_uuid,scan_core,issue_type, primary_data_schema, primary_data_value, secondary_data_schema, secondary_data_value,primary_key_value) VALUES (%s,%s, %s,%s, %s,%s,%s,%s) "
            logger.info(values)
            self._db_cur.execute(insertQuery, values)
            self._db_connection.commit()
            logger.info("data inserted")
            logger.info("--- %s Total seconds for _insert_Scan_results---" % (time.time() - start_time))
        except Exception as e:
            logger.error(e)
            
    def _export_scan_results(self, processUUID):
        try:
            processUUID = 'SHIPPING_ORDER'
            start_time = time.time()
            queryStr= queryStr = 'SELECT process_uuid,scan_core,issue_type,primary_key_value, primary_data_schema, primary_data_value, secondary_data_schema, secondary_data_value FROM scanner_results where scan_core = %s'
            values =(processUUID, )
            self._db_cur.execute(queryStr, values)
            dataRows = self._db_cur.fetchall()
            logger.info('Total _export_scan_results Results: '+str(len(dataRows)))
            logger.info("--- %s Total seconds for _export_scan_results---" % (time.time() - start_time))
            return dataRows
        except Exception as e:
            logger.error(e)
            
    def _create_core_running_status(self, corename, coreUUID):
        try:
            start_time = time.time()
            values = (corename,coreUUID)
            insertQuery = "INSERT INTO core_running_status ( core_name,core_process_id) VALUES (%s,%s) "
            self._db_cur.execute(insertQuery, values)
            self._db_connection.commit()
            logger.info("data inserted")
            logger.info("--- %s Total seconds for _create_core_running_status---" % (time.time() - start_time))
        except Exception as e:
            logger.error(e)
    
    def _update_core_running_status(self, processedLine, offset,coreUUID):
        try:
            start_time = time.time()
            values = (processedLine,offset,coreUUID)
            updateQuery = "update core_running_status set processed_line=%s, last_offset=%s where core_process_id=%s"
            self._db_cur.execute(updateQuery, values)
            self._db_connection.commit()
            logger.info("data updated")
           # logger.info("--- %s Total seconds for _create_core_running_status---" % (time.time() - start_time))
        except Exception as e:
            logger.error(e)
            