class ScannerResult:
    
    def __init__(self,processUUID ,typeOfRes,primarySchema,primaryValue, secondarySchema, secondaryValue):
        self.processUUID = processUUID
        self.typeOfResult = typeOfRes
        self.primarySchema = primarySchema
        self.primaryValue = primaryValue
        self.secondarySchema = secondarySchema
        self.secondaryValue = secondaryValue