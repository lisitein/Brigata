import pandas as pd
import json
import csv

class Handler:
    def __init__(self):
        self.dbPathOrUrl=''
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    def setDbPathOrUrl(self,pathOrUrl):
        self.dbPathOrUrl=str(pathOrUrl)
        return True

#goal of UploadHandler is to recognize the format of the file
class UploadHandler(Handler): 
    def __init__(self):
        super().__init__()  # I am not sure about params here
        # something about setDbPathOrUrl(path)
        
    def pushDataToDb(self, path):
        db_path = self.getDbPathOrUrl()
        if not db_path:
            print("Error: No database path or URL provided. Please call setDbPathOrUrl() first.")
            return False
        if path.endswith('.csv'):
            journal_handler = JournalUploadHandler()
            journal_handler.setDbPathOrUrl(db_path)
            result = journal_handler.pushDataToDb(path)
            return result
        elif path.endswith('.json'):
            category_handler = CategoryUploadHandler()
            category_handler.setDbPathOrUrl(db_path)
            result = category_handler.pushDataToDb(path)
            return result
        else:
            print("Error: Unsupported file format: {path}")
            return False