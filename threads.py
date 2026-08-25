from qgis.PyQt.QtCore import pyqtSignal
from qgis.core import QgsTask

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
        self.data = {c.title or c.id: c.id for c in catalog.get_collections()}
        return True
    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(dict(sorted(self.data.items())))

class ItemThread(QgsTask):
    """
        Accesses the given catalog and collects the items and their data
        assets for the given collection id

        Inputs:
            url : url of the catalog
            collection_id: id of the selected collection
    """
    result = pyqtSignal(dict)

    def __init__(self, url, collection_id):
        super().__init__("Item event", QgsTask.CanCancel)
        self.url = url
        self.id = collection_id
        self.data = {'items': [], 'assets': []}

    def run(self) -> bool:
        catalog = Client.open(self.url)
        for item in catalog.get_collection(self.id).get_items():
            self.data['items'].append(item.id)
            assets = item.to_dict()['assets']
            qml_href = self._qml_href(assets)
            for asset_id, asset in assets.items():
                if 'data' in (asset.get('roles') or []):
                    self.data['assets'].append((item.id, asset_id, asset['href'], qml_href))
        return True

    @staticmethod
    def _qml_href(assets):
        for asset in assets.values():
            if 'style' in (asset.get('roles') or []) and \
                    str(asset.get('type', '')).startswith('application/vnd.QGIS.qml'):
                return asset['href']
        return None

    def finished(self, result: bool) -> None:
        if result:
            self.result.emit(self.data)
