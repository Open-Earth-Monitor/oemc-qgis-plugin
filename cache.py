import sqlite3
import os

class Database:
    """
        class to maintain and interact with db
    """
    def __init__(self, catalog_name) -> None:
        self.connection = self._get_connection(catalog_name)
        

    def create(self) -> None:
        pass
    
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
        
        # check if exist folder called `catalog_name`
        if not os.path.isfile(f'{plugin_dir}/db/{catalog_name}.db'):
            print(f"creating the db for {catalog_name}!")
            connection = sqlite3.connect(f"{plugin_dir}/db/{catalog_name}.db")
            cursor = connection.cursor()
            cursor.execute("CREATE TABLE catalog(id, title, description)")
            cursor.execute("CREATE TABLE collection(id, title, description, sld, qml)")
            cursor.execute("CREATE TABLE item(id, collection_id)")
            cursor.execute("CREATE TABLE asset(id, item_id, href)")
            print("tables are populated")
        else:
            connection = sqlite3.connect(f"{plugin_dir}/db/{catalog_name}.db")
            print("connection is established!")
         
        return connection
        
