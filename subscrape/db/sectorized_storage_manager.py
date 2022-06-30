import logging
import os
import json
import io

class SectorizedStorageManager:
    '''
    This class encapsulates logic to store data in sectors.
    '''
    def __init__(self, path, parachain, call_module, call_name):
        self.logger = logging.getLogger("SectorizedStorage")

        #: str: the name of the parachain. Used to produce debug messages
        self._parachain = parachain
        #: str: the name of the currently loaded extrinsics
        self._extrinsics_name = f"{call_module}_{call_name}"
        #: str: the folder where extrinsics of a given module and call are stored.
        self._extrinsics_folder = f"{path}extrinsics_{self._extrinsics_name}/"
        #: str: the name of the currently loaded extrinsics sector
        self._extrinsics_sector_name = None
        #: list: the currently loaded extrinsics sector
        self._extrinsics = None
        #: bool: a dirty flag that keeps track of unsaved extrinsics
        self._extrinsics_dirty = False
        #: int: the number of digits of blocks a sector takes, e.g. 4 -> 1e4 blocks per sector
        self.extrinsics_digits_per_sector = 4

        self._clear_extrinsics_state()

        # make sure the older exists
        if not os.path.exists(self._extrinsics_folder):
            os.makedirs(self._extrinsics_folder)

    def _clear_extrinsics_state(self):
        assert(not self._extrinsics_dirty)
        # remove references to existing sectors
        self._extrinsics = None
        self._extrinsics_sector_name = None

    def _extrinsics_sector_file_path(self, sector):
        return self._extrinsics_folder + sector +  ".json"

    def write_extrinsic(self, extrinsic):
        """
        The method assumes that consumers go through sorted block lists.
        It will load a sector from disk when it becomes active
        and unloads it once a new one becomes active.

        :param extrinsic: The extrinsic as dict
        :type extrinsic: dict
        """

        # determine the sector
        index = extrinsic["extrinsic_index"]
        sector = index.split("-")[0]
        suffix = "x" * self.extrinsics_digits_per_sector
        if(len(sector) > self.extrinsics_digits_per_sector):
            sector = sector[:-self.extrinsics_digits_per_sector] + suffix
        else:
            sector = suffix

        # did we change sector?
        if self._extrinsics_sector_name != sector:
            # make sure previous data is saved
            self.flush_extrinsics()
            self._clear_extrinsics_state()

            # load sector file or create empty dict
            self._extrinsics_sector_name = sector
            self._extrinsics = self._load_extrinsics_sector(sector)
            self.logger.info(f"{self._parachain} {self._extrinsics_name}: Switched to sector {self._extrinsics_sector_name}. {len(self._extrinsics)} active entries")

        # do we already know this extrinsic? 
        if index in self._extrinsics:
            return False

        self._extrinsics_dirty = True
        self._extrinsics[index] = extrinsic

        return True

    def flush_extrinsics(self):
        if not self._extrinsics_dirty:
            return

        file_path = self._extrinsics_sector_file_path(self._extrinsics_sector_name)
        self.logger.info(f"Writing {len(self._extrinsics)} entries")
        payload = json.dumps(self._extrinsics)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._extrinsics_dirty = False

    def _load_extrinsics_sector(self, sector):
        file_path = self._extrinsics_sector_file_path(sector)
        if os.path.exists(file_path):
            file = io.open(file_path)
            file_payload = file.read()
            return json.loads(file_payload)
        else:
            return {}

    def extrinsics_iter(self):
        class ExtrinsicsIter:
            def __init__(self, folder, file_list):
                self.folder = folder
                self.file_list = file_list
                self.file_index = 0
                self.extrinsics = None
                self.extrinsics_index = None

            def __iter__(self):
                return self

            def __next__(self):
                load_next_file = False
                if self.extrinsics_index == None:
                    load_next_file = True
                elif self.extrinsics_index >= len(self.extrinsics):
                    load_next_file = True

                if load_next_file:
                    if self.file_index >= len(self.file_list):
                        raise StopIteration

                    file_path = self.file_list[self.file_index]
                    file = io.open(self.folder + file_path)
                    file_payload = file.read()
                    self.extrinsics = json.loads(file_payload)
                    self.extrinsics_keys = list(self.extrinsics.keys())

                    self.extrinsics_index = 0
                    self.file_index += 1
                
                key = self.extrinsics_keys[self.extrinsics_index]
                result = self.extrinsics[key]
                self.extrinsics_index += 1

                return key, result

        file_list = os.listdir(self._extrinsics_folder)
        if(len(file_list) == 0):
            self.logger.warning("Empty file list in extrinsics_iter(). Did you use the correct config?")
        return ExtrinsicsIter(self._extrinsics_folder, file_list)
