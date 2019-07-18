import csv
from loguru import logger

class CSVFileUtil:
    def __init__(self,urlpath):
        self.fileUrl = urlpath
     
    def getCSVfileValue(self):
     logger.info('CSV location '+self.fileUrl)
     try:
         filename = self.fileUrl
         fields = [] 
         rows = []  
         with open(filename) as csvfile:
             csvreader = csv.reader(csvfile, delimiter=',')
             fields = next(csvreader)
             for row in csvreader:
                # print(row)
                 rows.append(row)
             return rows
     except Exception as e:
         logger.error(e)    
    @staticmethod     
    def writeDataInCsv (fileName, dataList ):
     logger.info('CSV location '+fileName)
     try:
         fp = open(fileName, 'w')
         myFile = csv.writer(fp)
         myFile.writerows(dataList)
         fp.close()
     except Exception as e:
         logger.error(e)    