class ScannerResult:
    
    def __init__(self,processUUID ,typeOfRes,primarySchema,primaryValue, secondarySchema, secondaryValue, primaryKeyValue,scanCore):
        self.processUUID = processUUID
        self.typeOfResult = typeOfRes
        self.primarySchema = primarySchema
        self.primaryValue = primaryValue
        self.secondarySchema = secondarySchema
        self.secondaryValue = secondaryValue
        self.primaryKeyValue = primaryKeyValue
        self.scanCore = scanCore