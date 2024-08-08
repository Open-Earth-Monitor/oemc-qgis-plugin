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
        print("establishing a db connection ...")
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
    def _create_db(self, connection):
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
    def insert_collection(self, index, title): # description
        self.cursor.execute("INSERT OR IGNORE INTO collection (objectId, title) VALUES (?, ?)",
                            (index, title))
        self.connection.commit()

    # The user allowed to select multiple items which needed to be submit into db 
    def insert_items(self, ids, collection_id):
        # multiple id for item
        # single id for collection_id
        data = [(id, collection_id) for id in ids]
        # print(data)
        self.cursor.executemany("INSERT OR IGNORE INTO item (objectId, collection_objectId) VALUES(?,?)", data)
        self.connection.commit()

    def insert_assets(self, asset_ids, item_ids):
        data = []
        for item_id in item_ids:
            for asset_id in asset_ids:
                data.append((item_id, asset_id))
        self.cursor.executemany("INSERT OR IGNORE INTO asset (item_objectId, objectId) VALUES(?,?)", data)
        self.connection.commit()

    # def insert_assets(self, ids, item_id, hrefs):
    #     # multiple id for assets
    #     # single id for the items that assets belong to
    #     data = [(id, item_id, href) for id, href in zip(ids, hrefs)]
    #     self.cursor.executemany("INSERT OR IGNORE INTO asset VALUES(?,?,?)", data)
    #     self.connection.commit()


    def get_collection_by_title(self, title):
        objectId = self.cursor.execute("SELECT objectId FROM collection WHERE title = ?", (title,)).fetchone()[0]
        return objectId

    def get_all_collection_names(self):
        return [i[0] for i in self.cursor.execute("SELECT title FROM collection ORDER BY title ASC").fetchall()]
    
    def get_all_collection_objectId(self):
        return [i[0] for i in self.cursor.execute("SELECT objectId FROM collection").fetchall()]
    
    #for search functionality
    def get_collection_by_keyword(self, keyword):
        title_list = [i[0] for i in self.cursor.execute("SELECT title FROM collection WHERE title LIKE ?",("%"+keyword+"%",)).fetchall()]
        return title_list

    def get_item_by_collection_id(self, collection_id):
        return [i[0] for i in self.cursor.execute("SELECT objectId FROM item WHERE collection_objectId = ?",(collection_id,)).fetchall()]
    
    def get_asset_by_item_id(self, item_id):
        #return []
        return [i[0] for i in self.cursor.execute(f"SELECT objectId FROM asset WHERE item_objectId IN ({','.join(['?'] * len(item_id))})", item_id).fetchall()]
        # return self.cursor.execute("SELECT objectId FROM asset item_objectId = ?", (item_id,)).fetchall()

    def get_collections(self):
        return self.cursor.execute("SELECT title, objectId FROM collection ORDER BY title ASC").fetchall()
    
    def get_collection_titles_ordered(self):
        return [i[0] for i in self.get_collections()]
    
    def get_collection_ids_ordered(self):
        return [i[1] for i in self.get_collections()]
    
    def get_value_from_table(self, tablename, fieldname, fieldvalue):
        return [i[0] for i in self.cursor.execute(f"SELECT {fieldname} FROM {tablename} WHERE {fieldname} = ?", (fieldvalue,)).fetchall()]
    
    def delete_value_from_table(self, tablename, fieldname, fieldvalue):
        print(tablename, fieldname, fieldvalue)
        print('this message is from the delete funcion in cache')
        if type(fieldvalue) is list:
            for fv in fieldvalue:
                self.cursor.execute(f"DELETE FROM {tablename} WHERE {fieldname} = ?", (fv,))
            else:
                self.cursor.execute(f"DELETE FROM {tablename} WHERE {fieldname} = ?", (fieldvalue,))
        self.connection.commit()

    def flush_collection(self):
        self.cursor.execute("DELETE FROM collection")

    def flush_item(self):
        self.cursor.execute("DELETE FROM item")

    def flush_asset(self):
        self.cursor.execute("DELETE FROM asset")

    def flush_all(self):
        self.flush_collection()
        self.flush_item()
        self.flush_asset()