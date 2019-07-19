import uuid
import os
import tempfile
from multiprocessing import Process
from ScannerResult import ScannerResult
from CSVFileUtils import CSVFileUtil
from MailSender import MailHelper

from loguru import logger
from ConfigReader import ConfigurationReader
from DataBaseConnector import DataBaseConnector


class ScanProcessor:
  
    def __init__(self):
         self.primaryDB =None
         self.secondaryDB =None
         self.processUUID =None
         self.localDB =None
         self.csvFileName = None

    def getSecondaryDBValue(self,key, secondaryDBResult):
        for res in secondaryDBResult:
            for tempKey,value in res.items():
                if(tempKey == key):
                    return value
        return None
        
    def trigger_process(self,core):
        logger.info('Syn '+ core.get('core_name') +" core started " )
        try:
            mappingLocation = core.get('mapping_location')
            csvUtil = CSVFileUtil(mappingLocation)
            csvData = csvUtil.getCSVfileValue() 
            primaryQueryStr = core.get('primary_db_query')
            secondaryQueryStr = core.get('secondary_db_query')
            primaryDBResult = self.primaryDB._get_scaning_records(core.get('primary_db_query'),core.get('primary_db_primary_key'))
            secondaryDBResult = self.secondaryDB._get_scaning_records(core.get('secondary_db_query'),core.get('secondary_db_primary_key'))
            #logger.info(primaryDBResult)
            #logger.info(secondaryDBResult)
            processedLine = 1
            for res in primaryDBResult:
                #logger.info(res)
                for key,value in res.items():
                    primaryDBValue = value
                    secondaryDBValue = self.getSecondaryDBValue(key, secondaryDBResult)
                    if(secondaryDBValue != None):
                        logger.info('Processed Line : '+str(processedLine) +" with "+ str(key))
                        processedLine = 1 + processedLine
                        self.compareValue(csvData, primaryDBValue, secondaryDBValue, key)
        except Exception as e:
            logger.error(e)
    
    def compareValue(self, rows, primaryDBValue, secondaryDBValue,primaryKeyValue):
        try:
            logger.info("compareValue invokes ")
            for row in rows:
                counter= int(1)
                primaryValue = None
                secondaryValue= None
                primaryColumn = None
                secondaryColumn= None
                for col in row:
                    if counter %2 ==0:
                        secondaryColumn= col
                        secondaryValue = secondaryDBValue.get(col)
                    else:
                        primaryColumn = col
                        primaryValue = primaryDBValue.get(col)
                        counter=counter+1
                if col =='dutiable_value':
                    primaryValue = str(primaryValue)
                    secondaryValue = str(secondaryValue)
                #logger.info(primaryValue)
                #logger.info(secondaryValue)        
                if primaryValue != secondaryValue :
                    logger.critical("Data mismatched ")
                    scannerResut =  ScannerResult(self.processUUID ,'data-mistmacthed', primaryColumn ,primaryValue, secondaryColumn ,secondaryValue, primaryKeyValue)
                    self.localDB._insert_Scan_results(scannerResut)
        except Exception as e:
            logger.error(e)
                   
    def initProcessor(self):
        self.processUUID = str(uuid.uuid4())
        logger.info("Sync Scanner started process id "+self.processUUID)
        try:
            readerObj = ConfigurationReader() 
            self.primaryDB = DataBaseConnector(readerObj.getConfigValue('PRIMARY_DB', 'HOST'), readerObj.getConfigValue('PRIMARY_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('PRIMARY_DB', 'PASSWORD'),readerObj.getConfigValue('PRIMARY_DB', 'PORT'), readerObj.getConfigValue('PRIMARY_DB', 'DATABASE_NAME')) 
            
            self.secondaryDB = DataBaseConnector(readerObj.getConfigValue('SECONDARY_DB', 'HOST'), readerObj.getConfigValue('SECONDARY_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('SECONDARY_DB', 'PASSWORD'),readerObj.getConfigValue('SECONDARY_DB', 'PORT'), readerObj.getConfigValue('SECONDARY_DB', 'DATABASE_NAME'))
            
            self.localDB = DataBaseConnector(readerObj.getConfigValue('LOCAL_DB', 'HOST'), readerObj.getConfigValue('LOCAL_DB', 'USER_NAME'),
                                          readerObj.getConfigValue('LOCAL_DB', 'PASSWORD'),readerObj.getConfigValue('LOCAL_DB', 'PORT'), readerObj.getConfigValue('LOCAL_DB', 'DATABASE_NAME'))
            
            coreList = self.localDB._get_db_records(readerObj.getConfigValue('LOCAL_DB', 'CORE_QUERY'))
            for core in coreList:
                self.trigger_process(core) 
            
            exportDataList = self.localDB._export_scan_results(self.processUUID)
            if(len(exportDataList) > 0) :
                tempDir =tempfile.gettempdir()
                unique_filename = str(uuid.uuid4())
                self.csvFileName= tempDir+"/"+unique_filename+".csv"
                CSVFileUtil.writeDataInCsv(self.csvFileName,exportDataList)   
               # MailHelper.sendMailNotification(self.csvFileName ,readerObj)
            else:
                logger.info('No Mismatch data found on process '+self.processUUID)
        except Exception as e:
            logger.error(e)
      
    
    def destroyProcessor(self): 
        try:
            if(self.csvFileName != None):
                os.remove(self.csvFileName)
            self.primaryDB._close_db_connection()
            self.secondaryDB._close_db_connection()
            self.localDB._close_db_connection()
            logger.info("Sync Scanner  process id "+self.processUUID +" completed")
        except Exception as e:
            logger.error(e)