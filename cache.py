import sqlite3
import os

class Database:
    """
        class to maintain and interact with db
    """
    def __init__(self, catalog_name) -> None:
        self.connection = self._get_connection(catalog_name)
        self.cursor = self.connection.cursor()
            
    def read(self) -> None:
        pass

    def update(self) -> None:
        pass

    def delete(self) -> None:
        pass

    def _get_connection(self, catalog_name) -> tuple:
        # check is there is a file called `db`
        plugin_dir = os.path.dirname(__file__)
        
        if not os.path.isdir(f'{plugin_dir}/db'):
            os.mkdir(f'{plugin_dir}/db')
            print("directory called db is created")
        db_path = f"{plugin_dir}/db/{catalog_name}.db"
        
        # check if exist folder called `catalog_name`
        if not os.path.isfile(db_path):
            connection = sqlite3.connect(db_path)
            self._create_db(connection=connection)
            
        else:
            connection = sqlite3.connect(db_path)

        return connection


    def _create_db(self, connection):
        self.cursor.execute("CREATE TABLE catalog(id, title, description)")
        self.cursor.execute("CREATE TABLE collection(id, title, description, sld, qml)")
        self.cursor.execute("CREATE TABLE item(id, collection_id)")
        self.cursor.execute("CREATE TABLE asset(id, item_id, href)")        
    
    # single event selection case.
    def insert_catalog(self, id, title, description):
        self.cursor.execute(f"INSERT INTO catalog VALUES ({id},{title},{description})")
        self.connection.commit()

    # Single entry event. The user only allowed to select a collection from the UI
    # when the selection is performed on the UI following function needed to be called. 
    def insert_collection(self, id, title, description, sld, qml):
        self.cursor.execute(f"INSET INTO collection VALUES ({id},{title},{description},{sld},{qml})")
        self.connection.commit()

    # The user allowed to select multiple items which needed to be submit into db 
    def insert_items(self, ids, collection_id):
        # multiple id for item
        # single id for collection_id
        data = [(id, collection_id) for id in ids]
        self.cursor.executemany("INSERT INTO item VALUES(?,?)", data)
        self.connection.commit()

    def insert_assets(self, ids, item_id, hrefs):
        # multiple id for assets
        # single id for the items that assets belong to
        data = [(id, item_id ,href) for id, href in zip(ids, hrefs)]
        self.cursor.executemany("INSERT INTO asset VALUES(?,?,?)", data)
        self.connection.commit()