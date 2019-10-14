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
         fieldnames = ['Scan process uuid', 'Module', 'Issue type','Primary Key Value','Old Soluship Compare Column','Old Soluship Compare Column Value','Laravel Soluship Compare Column', 'Laravel Soluship Compare Column value'] 
         dataList.insert(0, fieldnames)
         with open(fileName, 'w', newline='') as outfile:
             myFile = csv.writer(outfile)
             #myFile.writeheader(fieldnames)
             myFile.writerows(dataList)
         outfile.close()
     except Exception as e:
         logger.error(e)    