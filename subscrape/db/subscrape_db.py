import os
import io
import json
import logging


# one DB per Parachain
class SubscrapeDB:

    def __init__(self, path):
        self.logger = logging.getLogger("SubscrapeDB")
        self._path = path
        # the name of the currently loaded extrinsics
        self._extrinsics_name = None
        # the name of the currently loaded extrinsics sector
        self._extrinsics_sector_name = None
        # the currently loaded extrinsics sector
        self._extrinsics = None
        # a dirty flag that keeps track of unsaved changes
        self._dirty = False
        # store information from batched calls by setting a dimension
        self._dimension = ""
        self.digits_per_sector = 6

    @property
    def dimension(self):
        return self._dimension
    
    @dimension.setter
    def dimension(self, value):
        self.flush_extrinsics()
        self._dimension = value
        self._clean_state()

    def _extrinsics_folder(self, name):
        return f"{self._path}extrinsics_{name}/"

    def _extrinsics_sector_file_path(self, extrinsic, sector, dimension):
        path = self._extrinsics_folder(extrinsic) + sector
        if self.dimension != "":
            path += "_" + dimension
        path += ".json"
        return  path

    def _clean_state(self):
        assert(not self._dirty)
        # remove references to existing sectors
        self._extrinsics = None
        self._extrinsics_sector_name = None

    def warmup_extrinsics(self, name):
        self._clean_state()

        self._extrinsics_name = name
        # make sure folder exists
        folder_path = self._extrinsics_folder(name)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

    # the method assumes that consumers go through sorted block lists
    # it will load a sector from disk when it becomes active
    # and unloads it once a new one becomes active
    def set_extrinsic(self, name, index, payload):
        assert(name == self._extrinsics_name)

        # determine the sector
        sector = index.split("-")[0]
        suffix = "x" * self.digits_per_sector
        if(len(sector) > self.digits_per_sector):
            sector = sector[:-self.digits_per_sector] + suffix
        else:
            sector = suffix

        # did we change sector?
        if self._extrinsics_sector_name != sector:
            # make sure previous data is saved
            self.flush_extrinsics()
            self._clean_state()

            # load sector file or create empty dict
            self._extrinsics_sector_name = sector
            file_path = self._extrinsics_sector_file_path(name, sector, self._dimension)
            if os.path.exists(file_path):
                file = io.open(file_path)
                file_payload = file.read()
                self._extrinsics = json.loads(file_payload)
            else:
                self._extrinsics = {}
            self.logger.info(f"Switched to sector {self._extrinsics_sector_name}. {len(self._extrinsics)} active entries")
        

        # do we already know this extrinsic? 
        if index in self._extrinsics:
            return False

        self._dirty = True
        self._extrinsics[index] = payload

        return True

    def flush_extrinsics(self):
        if not self._dirty:
            return

        file_path = self._extrinsics_sector_file_path(self._extrinsics_name, self._extrinsics_sector_name, self._dimension)
        self.logger.info(f"Writing {len(self._extrinsics)} entries")
        payload = json.dumps(self._extrinsics)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._dirty = False

#        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
#        if os.path.exists(file_path):
#            self.logger.warn(f"{file_path} already exists. Skipping.")
#            return
