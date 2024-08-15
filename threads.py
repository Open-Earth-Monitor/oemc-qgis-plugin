from qgis.PyQt.QtXml import QDomDocument
from qgis.PyQt.QtCore import pyqtSignal, QRunnable
from qgis.core import QgsTask, QgsRasterLayer, QgsProject

from urllib.request import urlopen
from pystac_client.client import Client

class CatalogThread(QgsTask):
    """
        accesses the stac and collects the title and id of the collections
        returns a dictionary of title:id pairs

        Inputs:
            url : url of the catalog 
    """
    result = pyqtSignal(dict)

    def __init__(self, url):
        super().__init__("Collection event", QgsTask.CanCancel)
        self.url = url
        self.data = dict()

    def run(self) -> bool:
        catalog = Client.open(self.url)
        for i in catalog.links:
            if i.rel == 'child':
                self.data[i.title] = i.target.split('/')[1]
                
        return True
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(dict(sorted(self.data.items())))

class ItemThread(QgsTask):
    """
        Accesses to the given catalog and collects the id of the items 
        using given collection id

        Inputs:
            url : url of the catalog
            collection_id: id of the selected collection

    """
    result = pyqtSignal(list)

    def __init__(self, url, collection_id):
        super().__init__("Item event", QgsTask.CanCancel)
        self.url = url
        self.id = collection_id
        self.item_ids = None

    def run(self) -> bool:
        catalog = Client.open(self.url)
        items = catalog.get_collection(self.id).get_items()
        self.item_ids = [item.id for item in items]
        return True
    
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.item_ids)

class AssetThread(QgsTask):
    """
        Inputs:
            url : url of the catalog
            collection_id: id of the selected collection
            item_ids: all items that is nested under the selected collection
            selected_item_indexes: indexes of the selected items
    """
    result = pyqtSignal(list)

    def __init__(self, url, collection_id, selected_items):
        super().__init__("Asset event", QgsTask.CanCancel)
        self.url = url
        self.collection_id = collection_id
        self.item_ids = selected_items
        self.unique = []

    def run(self) -> bool:
        catalog = Client.open(self.url)
        collection = catalog.get_collection(self.collection_id)

        for item_id in self.item_ids:
            assets = collection.get_item(item_id).to_dict()['assets']
            for asset in assets.keys():
                if not (
                    asset.endswith('view') or
                    asset.endswith('nail') or
                    asset.endswith('sld') or
                    asset.endswith('qml')
                ):
                    if asset not in self.unique:
                        self.unique.append(asset)
            return True

    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.unique)
            # self.result.emit([self.unique, self.urls, self.qml])

class HypertextThread(QgsTask):
    result = pyqtSignal(list)
    def __init__(self, url, collection_id, item_ids, asset_ids):
        super().__init__("HyperText event", QgsTask.CanCancel)

        self.url = url
        self.collection_id = collection_id
        self.item_ids = item_ids
        self.asset_ids = asset_ids
        self.data = []

    def run(self) -> bool:
        catalog = Client.open(self.url)
        collection = catalog.get_collection(self.collection_id)
        for item_id in self.item_ids:
            assets = collection.get_item(item_id).to_dict()['assets']
            try:
                qml_file = assets['qml']['href']
            except KeyError:
                qml_file = None
            for asset_id in self.asset_ids:
                href = assets[asset_id]['href']
                self.data.append((item_id, asset_id, href, qml_file))
        return True
    
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.data)

class RegisterData(QRunnable):
    result = pyqtSignal()

    def __init__(self, data, item_tree):
        super().__init__()
        self.data = data
        self.item_tree = item_tree

    def run(self):
        data_path = f"/vsicurl/{self.data[2]}"
        raster_layer = QgsRasterLayer(data_path, baseName=self.data[1])
        if self.data[1] not in [i.name() for i in self.item_tree.findLayers()]:
            if self.data[3] is not None:
                doc = QDomDocument()
                doc.setContent(urlopen(self.data[3]).read())
                raster_layer.importNamedStyle(doc)
            QgsProject.instance().addMapLayer(mapLayer=raster_layer, addToLegend=False)
            self.item_tree.addLayer(raster_layer)

    def finished(self, result):
        if result:
            self.result.emit()