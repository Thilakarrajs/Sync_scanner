import configparser
from loguru import logger
class ConfigurationReader(object):

    def __init__(self):
        
        try:
            self.configParser = configparser.RawConfigParser()
            configFilePath = r'../config/configuration.cfg'
            self.configParser.read(configFilePath)
            logger.info("configuration file loaded :"+configFilePath)
        except Exception as e:
            logger.error(e)
        
    def getConfigValue(self,settings, key):
        try:
            value = self.configParser.get(settings, key)  
            return value
        except Exception as e:
            logger.error(e)
 
