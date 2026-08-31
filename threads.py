from qgis.PyQt.QtCore import pyqtSignal
from qgis.core import QgsTask

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
        self.data = {'collection_id': collection_id, 'items': [], 'assets': []}

    def run(self) -> bool:
        catalog = Client.open(self.url)
        collection = next((c for c in catalog.get_collections() if c.id == self.id), None)
        if collection is None:
            return False
        for item in collection.get_items():
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


class MetadataThread(QgsTask):
    """
        Accesses the given catalog and collects the metadata of the selected
        collection, including the thumbnail of its first item.

        Inputs:
            url : url of the catalog
            collection_id: id of the selected collection
    """
    result = pyqtSignal(dict)

    def __init__(self, url, collection_id):
        super().__init__("Metadata event", QgsTask.CanCancel)
        self.url = url
        self.collection_id = collection_id
        self.metadata = None

    def run(self) -> bool:
        catalog = Client.open(self.url)
        collection = next((c for c in catalog.get_collections() if c.id == self.collection_id), None)
        if collection is None:
            return False

        d = collection.to_dict()
        extent = d.get('extent') or {}
        bbox = (extent.get('spatial') or {}).get('bbox') or [[]]
        temporal = (extent.get('temporal') or {}).get('interval') or [[None, None]]

        self.metadata = {
            'title': d.get('title') or d.get('id'),
            'description': d.get('description'),
            'contact_name': d.get('contact_name'),
            'contact_email': d.get('contact_email'),
            'bbox': bbox[0],
            'temporal': temporal[0],
            'thumbnail': self._thumbnail_bytes(collection),
        }
        return True

    def _thumbnail_bytes(self, collection):
        try:
            first = next(iter(collection.get_items()), None)
        except Exception:
            return None
        if first is None:
            return None
        for asset in first.to_dict().get('assets', {}).values():
            if 'thumbnail' in (asset.get('roles') or []):
                try:
                    return urlopen(asset['href'], timeout=30).read()
                except Exception:
                    return None
        return None

    def finished(self, result: bool) -> None:
        if result and self.metadata is not None:
            self.result.emit(self.metadata)
