import uuid
import os
import tempfile
import decimal
import datetime
from multiprocessing import Process
from ScannerResult import ScannerResult
from CSVFileUtils import CSVFileUtil
from MailSender import MailHelper
from SynScannerUtil import SyncScannerUtil
from loguru import logger
from ConfigReader import ConfigurationReader
from DataBaseConnector import DataBaseConnector
from builtins import str



class ScanProcessor:
  
    def __init__(self):
         self.primaryDB =None
         self.secondaryDB =None
         self.processUUID =None
         self.localDB =None
         self.csvFileName = None
         self.primaryDBResult = None
         self.secondaryDBResult = None

    def getSecondaryDBValue(self,key, secondaryDBResult):
        for res in secondaryDBResult:
            for tempKey,value in res.items():
                if(tempKey == key):
                    return value
        return None
    
    def getDBKeyValue(self,dBResult):
        keyList = []
        for res in dBResult:
            for tempKey,value in res.items():
                 keyList.append(tempKey)
        
        return keyList
    
    def trigger_process(self,coreUUID,core , offsetValue, processedLine):
        logger.info('Syn '+ core.get('core_name') + "-"+ coreUUID +" core started with offset value : "+str(offsetValue) )
        try:
            mappingLocation = core.get('mapping_location')
            csvUtil = CSVFileUtil(mappingLocation)
            csvData = csvUtil.getCSVfileValue() 
            primaryQueryStr = core.get('primary_db_query')
            numberOfRecords = self.readerObj.getConfigValue('APP_CONFIG', 'NUMBER_OF_RECORDS')
            primaryQueryStr = SyncScannerUtil._build_query(primaryQueryStr,offsetValue ,int(numberOfRecords),core.get('primary_db_primary_key') )
            secondaryQueryStr = core.get('secondary_db_query')
            secondaryQueryStr = SyncScannerUtil._build_query(secondaryQueryStr,offsetValue ,self.readerObj.getConfigValue('APP_CONFIG', 'NUMBER_OF_RECORDS'),core.get('secondary_db_primary_key') )
            primaryDBResult = self.primaryDB._get_scaning_records(primaryQueryStr,core.get('primary_db_primary_key'))
            secondaryDBResult = self.secondaryDB._get_scaning_records(secondaryQueryStr,core.get('secondary_db_primary_key'))
            self.primaryDBResult = self.getDBKeyValue(primaryDBResult) 
            self.secondaryDBResult = self.getDBKeyValue(secondaryDBResult) 
             
            
            for res in primaryDBResult:
                #logger.info(res)
                for key,value in res.items():
                    primaryDBValue = value
                    secondaryDBValue = self.getSecondaryDBValue(key, secondaryDBResult)
                    if(secondaryDBValue != None):
                        logger.info('Processed Line : '+str(processedLine) +" with "+ str(key))
                        processedLine = 1 + processedLine
                        self.primaryDBResult.remove(key)  
                        self.secondaryDBResult.remove(key)
                        self.compareValue(csvData, primaryDBValue, secondaryDBValue, key,core.get('core_name'))
            
            
            if(len(self.primaryDBResult) !=0 or len(self.secondaryDBResult)!=0 ):
                if(len(self.primaryDBResult) !=0):
                    missingQueryStr= SyncScannerUtil._build_missing_query(secondaryQueryStr,core.get('secondary_db_primary_key'))
                    missingRecords = self.secondaryDB._get_scaning_missing_records(missingQueryStr,core.get('secondary_db_primary_key'),self.primaryDBResult)
                    for res in missingRecords:
                        for key,value in res.items():
                            self.primaryDBResult.remove(key) 
                    if(len(self.primaryDBResult) !=0):
                        for tempValue in self.primaryDBResult:
                            scannerResut =  ScannerResult(self.processUUID ,'data-not-found', core.get('primary_db_primary_key') ,tempValue,  core.get('secondary_db_primary_key') ,'NOT_FOUND', tempValue,core.get('core_name'))
                            self.localDB._insert_Scan_results(scannerResut)
                    #print('value for primary missing')
                    #print(self.primaryDBResult) 
                if(len(self.secondaryDBResult) !=0):
                    missingQueryStr= SyncScannerUtil._build_missing_query(primaryQueryStr,core.get('primary_db_primary_key'))
                    missingRecords = self.primaryDB._get_scaning_missing_records(missingQueryStr,core.get('primary_db_primary_key'),self.secondaryDBResult)
                    for res in missingRecords:
                        for key,value in res.items():
                            self.secondaryDBResult.remove(key)
                    if(len(self.secondaryDBResult) !=0):
                        for tempValue in self.secondaryDBResult:
                            scannerResut =  ScannerResult(self.processUUID ,'data-not-found', core.get('primary_db_primary_key') ,'NOT_FOUND',  core.get('secondary_db_primary_key') ,tempValue, tempValue,core.get('core_name'))
                            self.localDB._insert_Scan_results(scannerResut)  
                    #print('value for secondary missing')
                    #print(self.secondaryDBResult)
            if(len(primaryDBResult) == 0 and  len(secondaryDBResult) == 0):
                return 1
            else:
                logger.info('========================================primaryDBResult '+str(len(self.primaryDBResult)))
                logger.info('========================================primaryDBResult '+str(len(self.secondaryDBResult)))
                logger.info(self.primaryDBResult)
                logger.info(self.secondaryDBResult)
                self.localDB._update_core_running_status(processedLine,offsetValue,coreUUID)
                self.trigger_process(coreUUID,core, offsetValue+int(numberOfRecords),processedLine)
        except Exception as e:
            logger.error(e)
    
    def compareValue(self, rows, primaryDBValue, secondaryDBValue,primaryKeyValue,coreName):
        try:
            #logger.info("compareValue invokes ")
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
                 
                if((col == 'open_time'  or col == 'close_time') and primaryValue == None and secondaryValue =='00:00'  ):
                   continue
                if(col == 'reference2'):
                    primaryValue = primaryValue.rstrip()
                if(col == 'pickup_date'):
                     primaryValue = primaryValue.date()
                     secondaryValue =secondaryValue.date()
                
                if primaryValue == None and type(secondaryValue) == float and secondaryValue == 0.0:
                    continue 
                if primaryValue ==""  and secondaryValue == None:
                    continue  
                if primaryValue == None and type(secondaryValue) == int and secondaryValue == 0:
                    continue
                if primaryValue == None and type(secondaryValue) == str and secondaryValue == 'NULL':
                    continue
                if secondaryValue == None and type(primaryValue) == float and primaryValue == 0.0:
                    continue  
                if secondaryValue == None and type(primaryValue) == int and primaryValue == 0:
                    continue  
                if col =='billing_type' and primaryValue == None and secondaryValue == 'Soluship Acct':
                    continue   
                if col =='actual_cost' and primaryValue == None and secondaryValue == 0.0:
                    continue 
                if col =='billed_weight' and primaryValue == None and secondaryValue == 0.0:
                    continue 
                if col =='leg_order_id' and primaryValue == None and secondaryValue == 0.0:
                    continue
                
                if(col == 'scheduled_ship_date'):
                     primaryValue = primaryValue.date()
                if(col == 'parent_invoice_id' and primaryValue != None):
                    primaryValue = int(primaryValue)
                if(col == 'amount' and primaryValue != None):
                    primaryValue =  round(primaryValue,2)      
                if type(primaryValue) == decimal.Decimal and type(secondaryValue) == float:
                    primaryValue = float(primaryValue)
                if((col == 'close_min' or col == 'ready_min') and primaryValue != None and type(primaryValue) == str and secondaryValue != None and type(secondaryValue) == str):
                      primaryValue = int (primaryValue)
                      secondaryValue = int (secondaryValue)
                if primaryValue != None and type(primaryValue) == str and secondaryValue != None and type(secondaryValue) == str:
                    primaryValue =primaryValue.rstrip()
                    secondaryValue =secondaryValue.rstrip()
                    
                if(col == 'exchange_rate'):
                      primaryValue = round(primaryValue,2)
                if(col == 'billed_weight'):
                      primaryValue = round(primaryValue,2)
                if(col == 'total_weight'):
                     primaryValue = round(primaryValue,2)
                #logger.info(primaryValue)
                #logger.info(secondaryValue)        
                if primaryValue != secondaryValue :
                    logger.critical("Data mismatched found!!!")
                    scannerResut =  ScannerResult(self.processUUID ,'data-mistmacthed', primaryColumn ,primaryValue, secondaryColumn ,secondaryValue, primaryKeyValue,coreName)
                    self.localDB._insert_Scan_results(scannerResut)
        except Exception as e:
            logger.error(e)
                   
    def initProcessor(self):
        self.processUUID = str(uuid.uuid4())
        logger.info("Sync Scanner started process id "+self.processUUID)
        try:
            self.readerObj = ConfigurationReader() 
            self.primaryDB = DataBaseConnector(self.readerObj.getConfigValue('PRIMARY_DB', 'HOST'), self.readerObj.getConfigValue('PRIMARY_DB', 'USER_NAME'),
                                          self.readerObj.getConfigValue('PRIMARY_DB', 'PASSWORD'),self.readerObj.getConfigValue('PRIMARY_DB', 'PORT'), self.readerObj.getConfigValue('PRIMARY_DB', 'DATABASE_NAME')) 
            
            self.secondaryDB = DataBaseConnector(self.readerObj.getConfigValue('SECONDARY_DB', 'HOST'), self.readerObj.getConfigValue('SECONDARY_DB', 'USER_NAME'),
                                          self.readerObj.getConfigValue('SECONDARY_DB', 'PASSWORD'),self.readerObj.getConfigValue('SECONDARY_DB', 'PORT'), self.readerObj.getConfigValue('SECONDARY_DB', 'DATABASE_NAME'))
            
            self.localDB = DataBaseConnector(self.readerObj.getConfigValue('LOCAL_DB', 'HOST'), self.readerObj.getConfigValue('LOCAL_DB', 'USER_NAME'),
                                          self.readerObj.getConfigValue('LOCAL_DB', 'PASSWORD'),self.readerObj.getConfigValue('LOCAL_DB', 'PORT'), self.readerObj.getConfigValue('LOCAL_DB', 'DATABASE_NAME'))
            
            coreList = self.localDB._get_db_records(self.readerObj.getConfigValue('LOCAL_DB', 'CORE_QUERY'))
            for core in coreList:
                coreUUID = str(uuid.uuid4())
                self.localDB._create_core_running_status(core.get('core_name'),coreUUID)
                self.trigger_process(coreUUID,core, 0,1) 
            
            exportDataList = self.localDB._export_scan_results(self.processUUID)
            if(len(exportDataList) > 0) :
                tempDir =tempfile.gettempdir()
                unique_filename = str(uuid.uuid4())
                self.csvFileName= tempDir+"/"+unique_filename+".csv"
                CSVFileUtil.writeDataInCsv(self.csvFileName,exportDataList)   
                MailHelper.sendMailNotification(self.csvFileName ,self.readerObj)
            else:
                logger.info('No Mismatch data found on process '+self.processUUID)
        except Exception as e:
            logger.error(e)
      
    
    
    def destroyProcessor(self): 
        try:
            if(self.csvFileName != None):
                print()
                #os.remove(self.csvFileName)
            self.primaryDB._close_db_connection()
            self.secondaryDB._close_db_connection()
            self.localDB._close_db_connection()
            logger.info("Sync Scanner  process id "+self.processUUID +" completed")
        except Exception as e:
            logger.error(e)
            
    def export_data_csv(self): 
        try:
            exportDataList = self.localDB._export_scan_results(self.processUUID)
            if(len(exportDataList) > 0) :
                tempDir =tempfile.gettempdir()
                unique_filename = str(uuid.uuid4())
                self.csvFileName= tempDir+"/"+unique_filename+".csv"
                CSVFileUtil.writeDataInCsv(self.csvFileName,exportDataList)   
                MailHelper.sendMailNotification(self.csvFileName ,self.readerObj)
            else:
                logger.info('No Mismatch data found on process '+self.processUUID)
        except Exception as e:
            logger.error(e)
        except Exception as e:
            logger.error(e)
            
                    