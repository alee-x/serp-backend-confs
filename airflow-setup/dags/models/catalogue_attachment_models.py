from typing import List


class Folder:
    FolderName: str
    ParentFolder: str

    def __init__(self, FolderName: str, ParentFolder: str) -> None:
        self.FolderName = FolderName
        self.ParentFolder = ParentFolder


class Nodes:
    MainFolder: Folder
    SubFolders: List[Folder]

    def __init__(self, MainFolder: Folder, SubFolders: List[Folder]) -> None:
        self.MainFolder = MainFolder
        self.SubFolders = SubFolders


class SourceDataSet:
    AssetManagement: bool
    AssetName: str
    Nodes: Nodes

    def __init__(self, AssetManagement: bool, AssetName: str, Nodes: Nodes) -> None:
        self.AssetManagement = AssetManagement
        self.AssetName = AssetName
        self.Nodes = Nodes


class CatalogueAttachmentAsset:
    FileName: str
    JsonStringSent: str
    Nodes: Nodes
    SourceDataSet: SourceDataSet

    def __init__(self, FileName: str, JsonStringSent: str, Nodes: Nodes, SourceDataSet: SourceDataSet) -> None:
        self.FileName = FileName
        self.JsonStringSent = JsonStringSent
        self.Nodes = Nodes
        self.SourceDataSet = SourceDataSet
