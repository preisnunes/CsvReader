import csv
from collections import namedtuple, OrderedDict

class CsvReader:
    """
    This class retrieves the content of a csv file in smaller chunks. Each chunk is a list of namedtuples
    It assumes that the first csv file row identifies the names of the columns.
    """

    def __init__(self, filePath :str, name :str, fields :list):
        """
        CsvReader constructor. Initializes all the variables that will be necessary to loop on the csvfile.
        Attributes
        ----------
        filePath :str
            Path to the csv file
        name :str
            Name of the namedtuple that will be generated for each file's row
        fields: list
            List of file column names (that must be present at the files's header) that will be extracted from each row
            These fields are also be the ones used for the namedtuple definition
        """
        self._file = open(filePath, 'r')
        self._reader = csv.reader(self._file)
        self._setColumns(fields)
        self.nameTupple = name
        self.setChunkSize()
        self.stopFileIteration = False
    
    def __iter__(self):
        return self

    def __next__(self):
        if self.stopFileIteration:
            raise StopIteration
        
        try:
            self._setChunk()
        except StopIteration:
            self.stopFileIteration = True
        
        return self._chunk

    def setChunkSize(self, size :int = 20):
        self.size = size

    def getChunkSize(self):
        return self.size

    def _setColumns(self, fields :list):
        columns = []
        
        for index, name in enumerate(next(self._reader)):
            if name not in fields:
                continue
            columns.append((index, name))
        
        self._columns = OrderedDict(columns)

    def _setChunk(self):
        self._chunk = []
        rowTupple = namedtuple(self.nameTupple, self._columns.values())
        
        for _ in range(self.getChunkSize()):
            fields = []
            row = next(self._reader)
            
            for index in self._columns.keys():
                fields.append(row[index])
            
            self._chunk.append(rowTupple(*fields))
        
    def __del__(self):
        self._file.close()
        



    

