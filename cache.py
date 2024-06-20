import sqlite3
import os

class Database:
    """
        class to maintain and interact with db
    """
    def __init__(self, catalog_name) -> None:
        self.db_connection, self.db_cursor = self._get_connection(catalog_name)
        

    def create(self) -> None:
        pass
    
    def read(self) -> None:
        pass

    def update(self) -> None:
        pass

    def delete(self) -> None:
        pass
    
    def _get_connection(cls, catalog_name) -> tuple:
        # check is there is a file called `db`
        plugin_dir = os.path.dirname(__file__)
        
        if not os.path.isdir(f'{plugin_dir}/db'):
            os.mkdir(f'{plugin_dir}/db')
        
        # check if exist folder called `catalog_name`
        if not os.path.isfile(f'{plugin_dir}/db/{catalog_name}.db'):
            connection = sqlite3.connect(f"{plugin_dir}/db/{catalog_name}.db")
            cursor = connection.cursor()
            cursor.execute("CREATE TABLE collection(uuid, id, title, description)")
            cursor.execute("CREATE TABLE item(uuid, id, collecion_id, datetime)")
            cursor.execute("CREATE TABLE asset(uuid, id, item_id, href, title)")
            return (connection, cursor)
        else:
            connection = sqlite3.connect(f"{plugin_dir}/db/{catalog_name}.db")
            cursor = connection.cursor()
            return (connection, cursor)
            