import logging
import os
import json
import io

class SectorizedStorageManager:
    '''
    This class encapsulates logic to store data in sectors.
    '''
    def __init__(self, folder:str, description:str, index_for_item):
        self.logger = logging.getLogger("SectorizedStorage")

        #: str: the folder where extrinsics of a given module and call are stored.
        self._folder = folder
        #: str: the description of the stored items. Used to produce debug messages
        self._description = description
        #: function: a function to retrieve the index of an item
        self._index_for_item = index_for_item
        #: str: the name of the currently loaded sector
        self._sector_name = None
        #: list: the currently loaded sector
        self._sector = None
        #: bool: a dirty flag that keeps track of unsaved items
        self._dirty = False
        #: int: the number of digits of blocks a sector takes, e.g. 4 -> 1e4 blocks per sector
        self._digits_per_sector = 4

        self._clear_sector_state()

        # make sure the older exists
        if not os.path.exists(self._folder):
            os.makedirs(self._folder)

    def _clear_sector_state(self):
        assert(not self._dirty)
        # remove references to existing sectors
        self._sector = None
        self._sector_name = None

    def _sector_file_path(self, sector):
        return self._folder + sector +  ".json"

    def write_item(self, item):
        """
        The method assumes that consumers go through sorted block lists.
        It will load a sector from disk when it becomes active
        and unloads it once a new one becomes active.

        :param item: The item as dict
        :type item: dict
        """
        # determine the sector
        index = self._index_for_item(item)
        sector = index.split("-")[0]
        suffix = "x" * self._digits_per_sector
        if(len(sector) > self._digits_per_sector):
            sector = sector[:-self._digits_per_sector] + suffix
        else:
            sector = suffix

        # did we change sector?
        if self._sector_name != sector:
            # make sure previous data is saved
            self.flush_sector()
            self._clear_sector_state()

            # load sector file or create empty dict
            self._sector_name = sector
            self._sector = self._load_sector(sector)
            self.logger.info(f"{self._description}: Switched to sector {self._sector_name}. {len(self._sector)} active entries")

        # do we already know this extrinsic? 
        if index in self._sector:
            return False

        self._dirty = True
        self._sector[index] = item

        return True

    def flush_sector(self):
        if not self._dirty:
            return

        file_path = self._sector_file_path(self._sector_name)
        self.logger.info(f"Writing {len(self._sector)} entries")
        payload = json.dumps(self._sector)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._dirty = False

    def _load_sector(self, sector):
        file_path = self._sector_file_path(sector)
        if os.path.exists(file_path):
            file = io.open(file_path)
            file_payload = file.read()
            return json.loads(file_payload)
        else:
            return {}

    def get_iter(self):
        class SectorizedItemIter:
            def __init__(self, folder, file_list):
                self.folder = folder
                self.file_list = file_list
                self.file_index = 0
                self.sector_items = None
                self.sector_index = None

            def __iter__(self):
                return self

            def __next__(self):
                load_next_file = False
                if self.sector_index == None:
                    load_next_file = True
                elif self.sector_index >= len(self.sector_items):
                    load_next_file = True

                if load_next_file:
                    if self.file_index >= len(self.file_list):
                        raise StopIteration

                    file_path = self.file_list[self.file_index]
                    file = io.open(self.folder + file_path)
                    file_payload = file.read()
                    self.sector_items = json.loads(file_payload)
                    self.keys = list(self.sector_items.keys())

                    self.sector_index = 0
                    self.file_index += 1
                
                key = self.keys[self.sector_index]
                result = self.sector_items[key]
                self.sector_index += 1

                return key, result

        file_list = os.listdir(self._folder)
        if(len(file_list) == 0):
            self.logger.warning("Empty file list in get_iter(). Did you use the correct config?")
        return SectorizedItemIter(self._folder, file_list)
