import json
from typing import List

class DtColumn(object):
    def __init__(self, name, count, max , min, distinct_count, *args, **kwargs):
        self.column_name = name
        self.count = count
        self.last = str(max)
        self.first = str(min)
        self.unique_per = str(distinct_count)
    
    @classmethod
    def from_json(cls, data):
        return cls(**data)

class NumColumn(object):
    def __init__(self, name, count, max , min, avg, stddev, *args, **kwargs):
        self.column_name = name
        self.count = count
        self.max = str(max)
        self.min = str(min)
        self.mean = str(avg)
        self.std = str(stddev)
    
    @classmethod
    def from_json(cls, data):
        return cls(**data)

class CharColumn(object):
    def __init__(self, name, count, freq, most_freq_val, distinct_count, *args, **kwargs):
        self.column_name = name
        self.count = count
        self.freq = freq
        self.top = str(most_freq_val)
        self.unique_per = str(distinct_count)

    @classmethod
    def from_json(cls, data):
        return cls(**data)

class Metrics(object):
    def __init__(self, charList: List[CharColumn], numList: List[NumColumn], dtList: List[DtColumn]):
        self.char_columns = charList
        self.num_columns = numList
        self.dt_columns = dtList
        self.row_count = 0

    @classmethod
    def from_json(cls, data):
        charList = list(map(CharColumn.from_json, data["charList"]))
        return cls(charList)

    @classmethod
    def from_json(cls, data):
        numList = list(map(NumColumn.from_json, data["numList"]))
        return cls(numList)
    
    @classmethod
    def from_json(cls, data):
        dtList = list(map(DtColumn.from_json, data["dtList"]))
        return cls(dtList)

class TableObject(object):
    def __init__(self, metrics, structure, table_name):
        self.engine = "engine"
        self.db_name = "db_name"
        self.metrics = metrics
        self.structure = structure
        self.table_name = table_name
        self.schema_name = "schema_name"

    @classmethod
    def from_json(cls, data):
        metrics = map(Metrics.from_json, data["metrics"])
        return cls(metrics)

    @classmethod
    def from_json(cls, data):
        structure = map(Structure.from_json, data["structure"])
        return cls(structure)

class Folder(object):
    def __init__(self, folderName, parentFolder):
        self.ParentFolder = parentFolder
        self.FolderName = folderName

    @classmethod
    def from_json(cls, data):
        return cls(**data)

class Nodes(object):
    def __init__(self, mainFolder, subFolders: List[Folder]):
        self.MainFolder = mainFolder
        self.SubFolders = subFolders

    @classmethod
    def from_json(cls, data):
        mainFolder = map(Folder.from_json, data["mainFolder"])
        return cls(mainFolder)

    @classmethod
    def from_json(cls, data):
        subFolders = list(map(Folder.from_json, data["subFolders"]))
        return cls(subFolders)

class Colume(object):
    def __init__(self, type, nullable, column_Name, *args, **kwargs):
        self.type = type
        self.nullable = bool(nullable)
        self.column_name =column_Name
        self.autoincrement = False

    @classmethod
    def from_json(cls, data):
        return cls(**data)

class Structure(object):
    def __init__(self, columes: List[Colume]):
        self.columes = columes
        self.fk_keys = []
        self.is_view = False
        self.pk_constraint = {}

    @classmethod
    def from_json(cls, data):
        columes = list(map(Colume.from_json, data["columes"]))
        return cls(columes)

class CatalogueAsset(object):
    def __init__(self, tables: List[TableObject], nodes, asset_name):
        self.Tables = tables
        self.Nodes = nodes
        self.AssetManagement = False
        self.AssetName = asset_name

    @classmethod
    def from_json(cls, data):
        tables = list(map(TableObject.from_json, data["tables"]))
        return cls(tables)

    @classmethod
    def from_json(cls, data):
        nodes = map(Nodes.from_json, data["nodes"])
        return cls(nodes)