from pystac_client.client import Client
from urllib.request import urlopen

from qgis.PyQt.QtCore import pyqtSignal, QRunnable
from qgis.PyQt.QtXml import QDomDocument
from qgis.core import QgsTask

from qgis.core import QgsProject, QgsRasterLayer


class CatalogTask(QgsTask):
    """
    This class is created to handle the titles of the collections 
    on the selected catalog.
    
    The result returns a dict that is composed of title descriptin and id of the collections
    inside of the catalog
    catalogSignal returns a stac client object
    """
    catalogSignal = pyqtSignal(object)
    result = pyqtSignal(dict)

    

    def __init__(self, url):
        super().__init__('listing collections', QgsTask.CanCancel)
        self.url = url
        self.title = []
        self.index = []
        self.desc = []
    
    def run(self) -> bool:
        self.catalog = Client.open(self.url)
        for i in self.catalog.links:
            if i.rel == 'child':
                self.index.append(i.target.split('/')[1])
                self.title.append(i.title)
        return True

    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(dict(title=self.title, index=self.index)) # , description=self.desc
            self.catalogSignal.emit(self.catalog)

# class to handle selections in the UI lists
class ListSelectionTask(QgsTask):
    result = pyqtSignal(list)

    def __init__(self, ui_component) -> None:
        super().__init__("item selection from lists", QgsTask.CanCancel)
        self.index = []
        self.ui_component = ui_component

    def run(self) -> bool:
        for element in self.ui_component.selectedItems():
            self.index.append(self.ui_component.indexFromItem(element).row())
        return True
    
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.index)

# to access the items using the threads
class ItemTask(QgsTask):
    result = pyqtSignal(list)
    request = pyqtSignal(list)
    def __init__(self, item_objectId, meta_catalog) -> None:
        super().__init__("listing items")
        self.meta_catalog = meta_catalog
        self.objectId = item_objectId
        self.items = None

    def run(self) -> bool:
        _items = self.meta_catalog.get_collection(self.objectId).get_items()
        self.items = [item.id for item in _items]
        return True

    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.items)

    

# to handle the assets in the selected catalog
class AssetTask(QgsTask):
    result = pyqtSignal(list)
    assetUrls = pyqtSignal(dict)
    def __init__(self,catalog, id_collection, id_item, selected_items) -> None:
        super().__init__("accessing the assets", QgsTask.CanCancel)

        self.catalog = catalog
        self.id_collection = id_collection
        self.id_item = id_item
        self.selected_items= selected_items
        self.unique = []        
        self.colorcodes = dict()

    def run(self) -> bool:
        for item in self.selected_items:
            _item = self.id_item[item]
            # accessing to the assets
            memories = self.catalog.get_collection(self.id_collection).get_item(_item).to_dict()['assets']
            for memory in memories.keys():
                if not ((memory.endswith('view')) or (memory.endswith('nail')) or (memory.endswith("sld")) or (memory.endswith("qml"))):
                    if memory not in self.unique:
                        self.unique.append(memory)
                        
        return True
    
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.unique)
            self.assetUrls.emit(self.colorcodes)

#class to handle with the QML files in the catalog with threads
class StyleTask(QgsTask):
    result = pyqtSignal(dict)
    styleContainer = pyqtSignal(list)
    def __init__(self, catalog, id_collection, id_item, selected_items) -> None:
        super().__init__("resolving styles", QgsTask.CanCancel)
        self.catalog = catalog
        self.id_collection = id_collection
        self.id_item = id_item
        self.selected_items= selected_items
        self.unique = []
        self.colorcodes = dict()
        self.styles = []
    
    def run(self):
        for item in self.selected_items:
            _item = self.id_item[item]
            assets = self.catalog.get_collection(self.id_collection).get_item(_item).to_dict()['assets']
            for asset in assets.keys():
                if asset.endswith('qml'):
                    style_url = assets['qml']['href']
                    if style_url not in self.styles: self.styles.append(style_url)
                    if self.id_collection not in self.colorcodes.keys():
                        style_file = urlopen(style_url)
                        stylebytes = style_file.read()
                        document = QDomDocument()
                        document.setContent(stylebytes)
                        self.colorcodes[self.id_collection] = document
        return True
    
    def finished(self, result: bool):
        if result: 
            self.result.emit(self.colorcodes)
            self.styleContainer.emit(self.styles)

#class to add raster by using thread pool
class registerRastersToServer(QRunnable):
    result = pyqtSignal()

    def __init__(self, id_asset, id_item, id_collection, object_catalog, place, colorschema):
        super().__init__()
        self.id_asset = id_asset
        self.id_item = id_item
        self.id_collection = id_collection
        self.object_catalog = object_catalog
        self.place = place
        self.colorschema = colorschema

    def run(self): 
        file_adress = '/vsicurl/' + self.object_catalog.get_collection(self.id_collection).get_item(self.id_item).to_dict()['assets'][self.id_asset]['href']
        raster_layer = QgsRasterLayer(file_adress, baseName=self.id_asset)

        if self.colorschema:
            raster_layer.importNamedStyle(self.colorschema)
        QgsProject.instance().addMapLayer(mapLayer=raster_layer, addToLegend=False)
        self.place.addLayer(raster_layer)

    def finished(self,result): 
        if result:
            self.result.emit()
