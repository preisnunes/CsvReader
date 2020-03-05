import csv

class CsvReader:

    def __init__(self, filePath, columnsToExtract):
        self._file = open(filePath, 'r')
        self._reader = csv.reader(self._file)
        self._setColumns(columnsToExtract)
        self.setChunkSize(20)
        self.stopIteration = False
    
    def __iter__(self):
        return self

    def __next__(self):
        if self.stopIteration:
            raise StopIteration
        try:
            self._setNextChunk()
        except StopIteration:
            self.stopIteration = True
        
        return self._chunk

    def setChunkSize(self, size):
        self.size = size

    def getChunkSize(self):
        return self.size

    def _setColumns(self, columnsNames :list):
        headers = next(self._reader)
        columns = []
        
        for index, name in enumerate(headers):
            if name not in columnsNames:
                continue
            columns.append((index, name))
        
        self._columns = dict(columns)

    def _setNextChunk(self):
        self._chunk = []
        
        for _ in range(0, self.getChunkSize()):
            rowAsDict = {}
            row = next(self._reader)
            
            for index, name in self._columns.items():
                rowAsDict[name] = row[index]
            
            self._chunk.append(rowAsDict)
        
    def __del__(self):
        self._file.close()
        



    

