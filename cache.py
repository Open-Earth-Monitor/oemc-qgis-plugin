import sqlite3
import os
from typing import List

class Database:
    """
        class to maintain and interact with db
    """
    def __init__(self, catalog_name) -> None:
        self.connection = self._get_connection(catalog_name)
        self.cursor = self.connection.cursor()
            
    def _get_connection(self, catalog_name) -> tuple:
        """
        Establishes a connection to the local database if it exists; 
        if not, will create one and return the connection of the created database.
        
        Args:
            catalog_name (str): Catalog name
        Returns: 
            (sqlite object): connetion to sql db
        """
        # check is there is a file called `db`
        plugin_dir = os.path.dirname(__file__)
        
        if not os.path.isdir(f'{plugin_dir}/db'):
            os.mkdir(f'{plugin_dir}/db')
        db_path = f"{plugin_dir}/db/{catalog_name}.db"
        
        # check if exist folder called `catalog_name`
        if not os.path.isfile(db_path):
            connection = sqlite3.connect(db_path)
            self._create_db(connection=connection)
            
        else:
            connection = sqlite3.connect(db_path)

        return connection

    def _create_db(self, connection) -> None:
        """
        creates the database file and tables relevat to the STAC
        
        Args: 
            connection (sqlite object)
        Returns:
            None
        """
        self.cursor = connection.cursor()
        self.cursor.execute("CREATE TABLE collection(id INTEGER PRIMARY KEY,objectId UNIQUE, title TEXT)") # , description TEXT
        self.cursor.execute("CREATE TABLE item(id INTEGER PRIMARY KEY, objectId UNIQUE, collection_objectId TEXT)")
        self.cursor.execute("CREATE TABLE asset(id INTEGER PRIMARY KEY, objectId TEXT, item_objectId TEXT, href TEXT, qml TEXT, UNIQUE(objectId, item_objectId))")        
    
    def insert_collection(self, index, title) -> None:
        """
            Inserts a collection to collection table
            
            Args: 
                index (str): collection id
                title (str): collection title 
            Returns :
                None
        """
        self.cursor.execute("INSERT OR IGNORE INTO collection (objectId, title) VALUES (?, ?)",
                            (index, title))
        self.connection.commit()

    def insert_items(self, ids, collection_id) -> None:
        """
            Inserts items into item table
            
            Args:
                ids (list(str)): List of the item ids
                collection_id (str): collection id
            Returns:
                None
        """
        data = [(id, collection_id) for id in ids]
        self.cursor.executemany("INSERT OR IGNORE INTO item (objectId, collection_objectId) VALUES(?,?)", data)
        self.connection.commit()

    def insert_assets(self, asset_data) -> None:
        """
            Inserts assets into asset table 
            
            Args:
                asset_data list(tuple): list of tuples. Tuple stores the data item_id, asset_id, file_href, qml_href, respectively.
            Returns:
                None
        """

        self.cursor.executemany("INSERT OR IGNORE INTO asset (item_objectId, objectId, href, qml) VALUES(?,?,?,?)", asset_data)
        self.connection.commit()

    def get_collection_by_title(self, title) -> str:
        """
            Performs a query to the table of collection to get the id by title
            
            Args:
                title (str): title of the collection
            Returns:
                id of the relevant title
        """
        objectId = self.cursor.execute("SELECT objectId FROM collection WHERE title = ?", (title,)).fetchone()[0]
        return objectId

    def get_all_collection_names(self) -> List[str]:
        """
            Executes a query to get the all collection names
            
            Args:
                None
            Returns:
                list of the collection names in the collection table
        """
        return self.cursor.execute("SELECT title FROM collection ORDER BY title ASC").fetchall()
    
    def get_collection_by_keyword(self, keyword) -> List[str]:
        """
            Performs a query by relying on a keyword that is provided

            Args:
                keyword (str): keyword to check from the database
            Returns:
                list of the titles that have matching characters 
        """
        title_list = [i[0] for i in self.cursor.execute("SELECT title FROM collection WHERE title LIKE ? ORDER BY title ASC",("%"+keyword+"%",)).fetchall()]
        return title_list

    def get_item_by_collection_id(self, collection_id) -> List[str]:
        """
            Gets the item by using the if of the collection

            Args:
                collection_id (str): collection id 
            Returns:
                list of the item ids that is nested under the given collection_id
        """

        return [i[0] for i in self.cursor.execute("SELECT objectId FROM item WHERE collection_objectId = ?",(collection_id,)).fetchall()]
    
    def get_asset_by_item_id(self, item_id) -> List[str]:
        """
            Make query from the db based on item id

            Args:
                item_id list of the the item ids
            Returns:
                list of the asset id
        """
        return [i[0] for i in self.cursor.execute(f"SELECT DISTINCT objectId FROM asset WHERE item_objectId IN ({','.join(['?'] * len(item_id))})", item_id).fetchall()]

    def get_data_from_asset(self, items, assets):
        """
            Makes a query using assets and items from asset table

            Args:
                items list(str): list of item ids
                assets list(str): list of asset ids
            Returns:
                list(tuple) : tuples stores item_id, asset_id href of data and qml of relevant data
        """
        query = f"""
            SELECT item_objectId, objectId, href, qml
            FROM asset
            WHERE objectId IN ({','.join(['?']*len(assets))})
            AND item_objectId IN ({','.join(['?']*len(items))})
            ORDER BY item_objectId ASC
        """
        return self.cursor.execute(query, assets + items).fetchall()