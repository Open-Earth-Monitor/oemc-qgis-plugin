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
            
    def read(self) -> None:
        pass

    def update(self) -> None:
        pass

    def delete(self) -> None:
        pass

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

    # creation of the tables
    def _create_db(self, connection) -> None:
        """
        creates the database file and tables relevat to the STAC
        Args: 
            connection (sqlite object)
        Returns:
            None
        """
        self.cursor = connection.cursor()
        # self.cursor.execute("CREATE TABLE catalog(id INTEGER PRIMARY KEY, objectId UNIQUE, title TEXT, description TEXT)")
        self.cursor.execute("CREATE TABLE collection(id INTEGER PRIMARY KEY,objectId UNIQUE, title TEXT)") # , description TEXT
        self.cursor.execute("CREATE TABLE item(id INTEGER PRIMARY KEY, objectId UNIQUE, collection_objectId TEXT)")
        self.cursor.execute("CREATE TABLE asset(id INTEGER PRIMARY KEY, objectId TEXT, item_objectId TEXT, href TEXT, qml TEXT, UNIQUE(objectId, item_objectId))")        
    
    # single event selection case.
    # def insert_catalog(self, id, title, description):
    #     print("insertion in progress for the catalog named :", title)
    #     self.cursor.execute(f"INSERT INTO catalog VALUES ({id},{title},{description})")
    #     self.connection.commit()

    # Single entry event. The user only allowed to select a collection from the UI
    # when the selection is performed on the UI following function needed to be called. 
    def insert_collection(self, index, title) -> None: # description
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

    # The user allowed to select multiple items which needed to be submit into db 
    def insert_items(self, ids, collection_id) -> None:
        """
            Inserts items into item table
            Args:
                ids (list(str)): List of the item ids
                collection_id (str): collection id
            Returns:
                None
        """
        # multiple id for item
        # single id for collection_id
        data = [(id, collection_id) for id in ids]
        # print(data)
        self.cursor.executemany("INSERT OR IGNORE INTO item (objectId, collection_objectId) VALUES(?,?)", data)
        self.connection.commit()

    def insert_assets(self, asset_data) -> None:
        print("this is a data insertion act")
        """
            Inserts assets into asset table 
            
            Args:
                asset_data list(tuple): list of tuples. Tuple stores the data item_id, asset_id, file_href, qml_href, respectively.
            Returns:
                None
        """

        self.cursor.executemany("INSERT OR IGNORE INTO asset (item_objectId, objectId, href, qml) VALUES(?,?,?,?)", asset_data)
        self.connection.commit()

    # def insert_assets(self, ids, item_id, hrefs):
    #     # multiple id for assets
    #     # single id for the items that assets belong to
    #     data = [(id, item_id, href) for id, href in zip(ids, hrefs)]
    #     self.cursor.executemany("INSERT OR IGNORE INTO asset VALUES(?,?,?)", data)
    #     self.connection.commit()


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
    
    def get_all_collection_objectId(self) -> List[str]:
        """
            Executes a query to get the all collection id from collection table
            Args: 
                None
            Returns:
                list of the collection ids
        """
        return [i[0] for i in self.cursor.execute("SELECT objectId FROM collection").fetchall()]
    
    #for search functionality
    def get_collection_by_keyword(self, keyword) -> List[str]:
        """
            Performs a query by relying on a keyword that is provided
            Args:
                keyword (str): keyword to check from the database
            Returns:
                list of the titles that have matching characters 
        """
        title_list = [i[0] for i in self.cursor.execute("SELECT title FROM collection WHERE title LIKE ?",("%"+keyword+"%",)).fetchall()]
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
        # return self.cursor.execute("SELECT objectId FROM asset item_objectId = ?", (item_id,)).fetchall()

    def get_collections(self) -> tuple:
        """
            Performs query to db and gets the title and id of 
            the collection in ascending order respect to the title

            Args: 
                None
            Returns: 
                Tuple of the sets that contains title and id of the collections 
        """
        return self.cursor.execute("SELECT title, objectId FROM collection ORDER BY title ASC").fetchall()
    
    def get_collection_titles_ordered(self) -> List[str]:
        """
            returns the ordered titles of the collection 
        """
        return [i[0] for i in self.get_collections()]
    
    def get_collection_ids_ordered(self) -> List[str]:
        """
            returns the ordered ids of the collections
            
            Args:
                None
            Returns:
                id of the collections in a list
        """
        return [i[1] for i in self.get_collections()]
    
    def get_value_from_table(self, tablename, fieldname, fieldvalue) -> List[str]:
        """
            Performs a query of a table by specifing the field name
            Args:
                tablename (str): name of the table one of the following collection/item/asset
                fieldname (str): name of the column exist for the table like objectId, title, href, or qml
                fieldvalue (str): the desired value of the fieldname to filtered
            Returns:
                list of strings as a result of the search
        """
        return [i[0] for i in self.cursor.execute(f"SELECT {fieldname} FROM {tablename} WHERE {fieldname} = ?", (fieldvalue,)).fetchall()]
    
    def delete_value_from_table(self, tablename, fieldname, fieldvalue) -> None:
        """
            Removes the entries that is specified/filtered
            Args:
                tablename (str): name of the table one of the following collection/item/asset
                fieldname (str): name of the column exist for the table like objectId, title, href, or qml
                fieldvalue (str): the desired value of the fieldname to filtered

        """
        print(tablename, fieldname, fieldvalue)
        print('this message is from the delete funcion in cache')
        if type(fieldvalue) is list:
            for fv in fieldvalue:
                self.cursor.execute(f"DELETE FROM {tablename} WHERE {fieldname} = ?", (fv,))
            else:
                self.cursor.execute(f"DELETE FROM {tablename} WHERE {fieldname} = ?", (fieldvalue,))
        self.connection.commit()

    def flush_collection(self) -> None:
        """
            Deletes the all entries from the collection table
            Args:
                None
            Returns:
                None
        """
        self.cursor.execute("DELETE FROM collection")

    def flush_item(self) -> None:
        """
            Deletes the all entries from the item table
            Args:
                None
            Returns:
                None
        """
        self.cursor.execute("DELETE FROM item")

    def flush_asset(self) -> None:
        """
            Deletes the all entries from the asset table
            Args:
                None
            Returns:
                None
        """
        self.cursor.execute("DELETE FROM asset")

    def flush_all(self):
        """
            Deletes the all entries from the all tables
            Args:
                None
            Returns:
                None
        """
        self.flush_collection()
        self.flush_item()
        self.flush_asset()