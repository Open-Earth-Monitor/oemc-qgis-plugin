# -*- coding: utf-8 -*-
"""
/***************************************************************************
 OemcStac
                                 A QGIS plugin
 This plugin provides easy access to OEMC STAC catalog
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                              -------------------
        begin                : 2023-11-07
        git sha              : $Format:%H$
        copyright            : (C) 2023 by OpenGeoHub
        email                : murat.sahin@opengeohub.org
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction

# Initialize Qt resources from file resources.py
from .resources import *
# Import the code for the dialog
from .oemc_plugin_dialog import OemcStacDialog
import os.path

# iniital settings for the PYSTAC and STAC_CLIENT
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parents[0])+'/src') # findable lib path
from pystac_client.client import Client

#importing the QT libs to control ui
from qgis.core import QgsProject, QgsRasterLayer, QgsTask, QgsApplication
from qgis.PyQt.QtCore import QRunnable, Qt, QThreadPool

from qgis.PyQt.QtCore import Qt, pyqtSignal
from qgis.PyQt.QtXml import QDomDocument
from qgis.PyQt.QtWidgets import QListWidget
# to access the qml files on the fly
from urllib.request import urlopen


class OemcStac:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        # initialize locale
        locale = QSettings().value('locale/userLocale')[0:2]
        locale_path = os.path.join(
            self.plugin_dir,
            'i18n',
            'OemcStac_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

        # Declare instance attributes
        self.actions = []
        self.menu = self.tr(u'&OEMC Plugin')

        # Check if plugin was started the first time in current QGIS session
        # Must be set in initGui() to survive plugin reloads
        self.first_start = None

        ############################################
        # tapping on the project structure to use it
        self.project_tree = QgsProject.instance().layerTreeRoot() # QgsLayerTree()
        # saving the stac names and catalog urls as a variable
        self.main_url = None
        self.oemc_stacs = dict(
            OpenLandMap = "https://s3.eu-central-1.wasabisys.com/stac/openlandmap/catalog.json",
            EcoDataCube = "https://s3.eu-central-1.wasabisys.com/stac/odse/catalog.json"
        )

    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate('OemcStac', message)


    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        """Add a toolbar icon to the toolbar.

        :param icon_path: Path to the icon for this action. Can be a resource
            path (e.g. ':/plugins/foo/bar.png') or a normal file system path.
        :type icon_path: str

        :param text: Text that should be shown in menu items for this action.
        :type text: str

        :param callback: Function to be called when the action is triggered.
        :type callback: function

        :param enabled_flag: A flag indicating if the action should be enabled
            by default. Defaults to True.
        :type enabled_flag: bool

        :param add_to_menu: Flag indicating whether the action should also
            be added to the menu. Defaults to True.
        :type add_to_menu: bool

        :param add_to_toolbar: Flag indicating whether the action should also
            be added to the toolbar. Defaults to True.
        :type add_to_toolbar: bool

        :param status_tip: Optional text to show in a popup when mouse pointer
            hovers over the action.
        :type status_tip: str

        :param parent: Parent widget for the new action. Defaults None.
        :type parent: QWidget

        :param whats_this: Optional text to show in the status bar when the
            mouse pointer hovers over the action.

        :returns: The action that was created. Note that the action is also
            added to self.actions list.
        :rtype: QAction
        """

        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            # Adds plugin icon to Plugins toolbar
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)

        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""

        icon_path = ':/plugins/oemc_plugin/icon.png'
        self.add_action(
            icon_path,
            text=self.tr(u'OEMC Plugin'),
            callback=self.run,
            parent=self.iface.mainWindow())

        # will be set False in run()
        self.first_start = True

    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""
        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr(u'&OEMC Plugin'),
                action)
            self.iface.removeToolBarIcon(action)

    def run(self):
        """Run method that performs all the real work"""

        # Create the dialog with elements (after translation) and keep reference
        # Only create GUI ONCE in callback, so that it will only load when the plugin is started
        if self.first_start == True:
            self.first_start = False
            self.dlg = OemcStacDialog()

            # creating some variable to handle the state of the plugin
            self._collection_meta = None
            self._catalog = None,
            self._all_items = None
            self._colors = None
            self._a_collection = None
            self._query_keys = dict()
            self._inserted = dict()
            self.thread_pool = QThreadPool().globalInstance()
            self.thread_pool.setMaxThreadCount(int(self.thread_pool.maxThreadCount()/2))

            # defining settings for the ui elements on the start
            self.dlg.listCatalog.view().setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            # self.dlg.addStrategy.view().setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
            self.dlg.listItems.setSelectionMode(QListWidget.ExtendedSelection)
            self.dlg.listAssets.setSelectionMode(QListWidget.ExtendedSelection)
            # adding the stac names from oemc_stac variable
            self.dlg.listCatalog.addItem("") # extra space for visual concerns
            self.dlg.listCatalog.addItems(list(self.oemc_stacs.keys()))
            self.dlg.addLayers.setEnabled(False)

        # functionalities
        # change on the selection of the catalog will update the
        # listCatalog and fills it with the collection names
        self.dlg.listCatalog.currentIndexChanged.connect(self.catalog_task_handler)
        # based on the selection from collections this will trigered
        # following the selection this will fills the listItems
        self.dlg.listCollection.itemClicked.connect(self.taskhandler_items) 
        # this will fills the listAssets with unique assets
        self.dlg.listItems.itemClicked.connect(self.asset_task_handler)
        # this will set selected variable for seleceted assets
        self.dlg.listAssets.itemClicked.connect(self.selecting_assets)
        # this will fills the strategies wit predefined add layer strategies
        # self.dlg.addStrategy.addItems(self.strategies)
        # finally some one is going to push the addLayers button

        self.dlg.addLayers.clicked.connect(self.add_layers_in_parallel) # add_layers) # #
        self.dlg.progressBar.reset()
        self.dlg.progressBar.setTextVisible(True)
        # show the dialog
        self.dlg.show()
        # Run the dialog event loop
        # result = self.dlg.exec_()
        # See if OK was pressed
        # if result:
        #     # Do something useful here - delete the line containing pass and
        #     # substitute with your code.
        #     pass
    
    def catalog_task_handler(self, index):
        self.dlg.listCollection.clear()
        self.dlg.listItems.clear()
        self.dlg.listAssets.clear()
        self.dlg.addLayers.setEnabled(False)

        self.main_url = list(self.oemc_stacs.values())[index-1]


        globals()["access_catalog"] = taskCatalog(self.main_url)
        QgsApplication.taskManager().addTask(globals()["access_catalog"])
        globals()["access_catalog"].result.connect(self.listing_collection)
        globals()["access_catalog"].catalog.connect(self.handling_catalog)

    def handling_catalog(self, catalog_object):
        self._catalog = catalog_object

    def listing_collection(self, algo_out):
        self._collection_meta = algo_out
        self.dlg.listCollection.addItems(sorted(algo_out['title']))

    def taskhandler_items(self):
        self.dlg.listItems.clear()
        self.dlg.listAssets.clear()
        self.dlg.addLayers.setEnabled(False)
        globals()['select_collection'] = listSelection(self.dlg.listCollection)
        QgsApplication.taskManager().addTask(globals()['select_collection'])
        globals()['select_collection'].result.connect(self.listing_items)

    def listing_items(self, arg):
        ind = self._collection_meta['title'].index(
            sorted(self._collection_meta['title'])[arg[0]]
        )
        self._a_collection = self._collection_meta['index'][ind]

        globals()["listing_items"] = taskItemListing(ind, self._collection_meta, self._catalog)
        QgsApplication.taskManager().addTask(globals()["listing_items"])
        globals()["listing_items"].result.connect(self.listhandler_items)

    def listhandler_items(self, namelist):
        self.dlg.listItems.addItems(namelist)
        self._all_items = namelist

    def asset_task_handler(self):
        self.dlg.listAssets.clear()
        self.dlg.addLayers.setEnabled(False)
        globals()['selecting_items'] = listSelection(self.dlg.listItems)
        QgsApplication.taskManager().addTask(globals()['selecting_items'])
        globals()['selecting_items'].result.connect(self.listing_assets)
        globals()['selecting_items'].result.connect(self.handle_styles)


    def listing_assets(self, selectedItems):
        self.dlg.listAssets.clear()
        globals()['listing_assets'] = taskAssetListing(
            self._catalog,
            self._a_collection,
            self._all_items,
            selectedItems
        )
        QgsApplication.taskManager().addTask(globals()['listing_assets'])
        globals()['listing_assets'].result.connect(self.listhandler_assets)
        

    def handle_styles(self, selectedItems):
        globals()['resolve_style'] = taskStyleResolver(
            self._catalog,
            self._a_collection,
            self._all_items,
            selectedItems
        )
        QgsApplication.taskManager().addTask(globals()["resolve_style"])
        globals()['resolve_style'].result.connect(self.handling_colors)

    def listhandler_assets(self,givenlist):
        self.dlg.listAssets.addItems(givenlist)
        self._all_assets = givenlist

    def handling_colors(self, given_cc):
        self._colors = given_cc

    def selecting_assets(self):
        self.dlg.addLayers.setEnabled(True)

    
    def add_layers_in_parallel(self):
        def get_selected_asset(passedArg):
            self._query_keys['assets'] = [self._all_assets[i] for i in passedArg]

        def get_selected_items(passedArg):
            self._query_keys["items"] =  [self._all_items[i] for i in passedArg]

            total_len = len(self._query_keys['assets']) * len(self._query_keys['items'])
            self.dlg.progressBar.setMaximum(total_len)

            call_parallel_implementation()

            self.dlg.progressBar.reset()


        globals()['asset_selection'] = listSelection(self.dlg.listAssets)
        QgsApplication.taskManager().addTask(globals()['asset_selection'])
        globals()['asset_selection'].result.connect(get_selected_asset)

        globals()['item_selection'] = listSelection(self.dlg.listItems)
        QgsApplication.taskManager().addTask(globals()['item_selection'])
        globals()['item_selection'].result.connect(get_selected_items)

        def call_parallel_implementation():
            total_count = len(self._query_keys['assets']) * len(self._query_keys['items'])
            count = 0
            self.dlg.progressBar.setMaximum(total_count)
            print(total_count)
            for asset in self._query_keys['assets']:
                for pos_item, item in enumerate(self._query_keys['items']):
                    col_name = self._catalog.get_collection(self._a_collection).title
                    col_tree = QgsProject.instance().layerTreeRoot().findGroup(col_name)
                    if col_tree:
                        item_tree = col_tree.findGroup(item)
                        
                        if item_tree:
                            if asset not in self._inserted[self._a_collection][item]:
                                self._inserted[self._a_collection][item].append(asset)
                                runnable = addRasterParalel(asset, item, self._a_collection,self._catalog, item_tree, self._colors.get(self._a_collection))
                                self.thread_pool.start(runnable)
                        else:
                            item_tree = col_tree.addGroup(item)
                            self._inserted[self._a_collection][item] = [asset]
                            
                            runnable = addRasterParalel(asset, item, self._a_collection,self._catalog, item_tree, self._colors.get(self._a_collection))
                            self.thread_pool.start(runnable)
                        count += 1
                        self.dlg.progressBar.setValue(count)
                    else:
                        col_tree = QgsProject.instance().layerTreeRoot().addGroup(col_name)
                        item_tree = col_tree.addGroup(item)
                        self._inserted[self._a_collection] = dict()
                        self._inserted[self._a_collection][item] = [asset]
                        
                        runnable = addRasterParalel(asset, item, self._a_collection,self._catalog, item_tree, self._colors.get(self._a_collection))
                        self.thread_pool.start(runnable)
                        count += 1
                        self.dlg.progressBar.setValue(count)
                    
                    
                    item_tree.setExpanded(False)
                    if pos_item != 0:
                        item_tree.setItemVisibilityChecked(False)
            col_tree.setExpanded(False)

class taskCatalog(QgsTask):
    result = pyqtSignal(dict)
    catalog = pyqtSignal(object)

    def __init__(self, given_url):
        super().__init__('OEMC STAC: listing collections', QgsTask.CanCancel)
        self.url = given_url
        self.title = []
        self.index = []

    def run(self):
        self.cat = Client.open(self.url)
        for i in self.cat.get_collections():
            self.title.append(i.title)
            self.index.append(i.id)
        return True

    def finished(self, result):
        if result:
            self.result.emit(dict(title=self.title, index=self.index))
            self.catalog.emit(self.cat)

    def cancel(self):
        super().cancel()

class listSelection(QgsTask):
    result = pyqtSignal(list)

    def __init__(self, ui_element):
        super().__init__("OEMC STAC: listing items part-i", QgsTask.CanCancel)
        self.ind = []
        self.ui = ui_element

    def run(self):
        for i in self.ui.selectedItems():
            self.ind.append(self.ui.indexFromItem(i).row())
        return True

    def finished(self,result):
        if result:
            self.result.emit(self.ind)

    def cancel(self): super().cancel()

class taskItemListing(QgsTask):
    result = pyqtSignal(list)
    def __init__(self, index, collection_meta, catalog):
        super().__init__("OEMC STAC: listing items part-ii")
        self.index = index
        self.cllct_meta = collection_meta
        self.catalog = catalog
        self._items = None
    def run(self):
        _items = self.catalog.get_collection(
            self.cllct_meta['index'][self.index]
        ).get_items()
        self._items = [i.id for i in _items]
        return True
    def finished(self, result):
        if result:
            self.result.emit(self._items)
    def cancel(self):
        super().cancel()

class taskAssetListing(QgsTask):
    result = pyqtSignal(list)
    def __init__(self, catalog, collectionid, item_id, selecteditems):
        super().__init__('OEMC STAC: listing assets')
        self.catalog = catalog
        self.collection=collectionid
        self.item_id = item_id
        self.selecteditems = selecteditems
        self.unique = []
        self.ccodes = dict()

    def run(self):
        for i in self.selecteditems:
            item = self.item_id[i]
            # accessing to the assets
            memo = self.catalog.get_collection(self.collection).get_item(item).to_dict()['assets']
            for k in memo.keys():
                if not  ((k.endswith('view')) or (k.endswith('nail')) or (k.endswith('sld')) or (k.endswith('qml'))):
                    if k not in self.unique:
                        self.unique.append(k)
        return True
    def finished(self,result):
        if result:
            self.result.emit(self.unique)

    def cancel(self): super().cancel()

class taskStyleResolver(QgsTask):
    result = pyqtSignal(dict)
    def __init__(self, catalog, collectionid, item_id, selecteditems):
        super().__init__('OEMC STAC: resolving styles')
        self.catalog = catalog
        self.collection=collectionid
        self.item_id = item_id
        self.selecteditems = selecteditems
        self.unique = []
        self.ccodes = dict()

    def run(self):
        for i in self.selecteditems:
            item = self.item_id[i]
            # accessing to the assets
            memo = self.catalog.get_collection(self.collection).get_item(item).to_dict()['assets']
            for k in memo.keys():
                if (k.endswith('qml')):
                    style_url = memo['qml']['href']
                    if self.collection not in self.ccodes.keys():
                        r_file = urlopen(style_url)
                        stylebytes = r_file.read()
                        doc = QDomDocument()
                        doc.setContent(stylebytes)
                        self.ccodes[self.collection] = doc
        return True

    def finished(self,result):
        if result:
            self.result.emit(self.ccodes)

    def cancel(self): super().cancel()

class addRasterParalel(QRunnable):
    result = pyqtSignal()
    def __init__(self, asset_id, item_id, collection_id, catalog_object, place, colors):
        super().__init__()
        self.asset_id = asset_id
        self.item_id = item_id
        self.collection_id = collection_id
        self.catalog = catalog_object
        self.place = place
        self.colorschema = colors

    def run(self):
        raster_remote = '/vsicurl/' + self.catalog.get_collection(self.collection_id).get_item(self.item_id).to_dict()['assets'][self.asset_id]['href']
        raster_layer = QgsRasterLayer(raster_remote, baseName=self.asset_id)

        if self.colorschema:
            raster_layer.importNamedStyle(self.colorschema)
        QgsProject.instance().addMapLayer(mapLayer=raster_layer, addToLegend=False)
        self.place.addLayer(raster_layer)

    def finished(self, result):
        if result:
            self.result.emit()