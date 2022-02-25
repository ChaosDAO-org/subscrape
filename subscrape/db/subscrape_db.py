import os
import io
import json

_DECIMALS_PER_SECTOR_FILE = 6

# one DB per Parachain
class SubscrapeDB:

    def __init__(self, path):
        self.path = path
        # the name of the currently loaded extrinsics
        self.extrinsics_name = None
        # the name of the currently loaded extrinsics sector
        self.extrinsics_sector_name = None
        # the currently loaded extrinsics sector
        self.extrinsics = None
        # a dirty flag that keeps track of unsaved changes
        self.dirty = False

    def _extrinsics_folder(self, name):
        return f"{self.path}extrinsics_{name}/"

    def _extrinsics_sector_file_path(self, extrinsic, sector):
        return self._extrinsics_folder(extrinsic) + sector + ".json"

    def warmup_extrinsics(self, name):
        # remove references to existing sectors
        assert(not self.dirty)
        self.extrinsics = None
        self.extrinsics_sector_name = None

        self.extrinsics_name = name
        # make sure folder exists
        folder_path = self._extrinsics_folder(name)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)


    # the method assumes that consumers go through sorted block lists
    # it will load a sector from disk when it becomes active
    # and unloads it once it becomes inactive
    def set_extrinsic(self, name, index, payload):
        assert(name == self.extrinsics_name)

        # determine the sector
        sector = index.split("-")[0]
        suffix = "x" * _DECIMALS_PER_SECTOR_FILE
        if(len(sector) > _DECIMALS_PER_SECTOR_FILE):
            sector = sector[:-_DECIMALS_PER_SECTOR_FILE] + suffix
        else:
            sector = suffix

        # did we change sector?
        if self.extrinsics_sector_name != sector:
            # make sure previous data is saved
            self.flush_extrinsics()

            # load sector file or create empty dict
            self.extrinsics_sector_name = sector
            file_path = self._extrinsics_sector_file_path(name, sector)
            if os.path.exists(file_path):
                file = io.open(file_path)
                payload = file.read()
                self.extrinsics = json.loads(payload)
            else:
                self.extrinsics = {}
        
        # if the extrinsic we are about to store already exists,
        # we issue a stop signal
        if index in self.extrinsics:
            return False

        self.dirty = True
        self.extrinsics[index] = payload

        return True

    def flush_extrinsics(self):
        if not self.dirty:
            return

        file_path = self._extrinsics_sector_file_path(self.extrinsics_name, self.extrinsics_sector_name)
        payload = json.dumps(self.extrinsics)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self.dirty = False

#        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
#        if os.path.exists(file_path):
#            self.logger.warn(f"{file_path} already exists. Skipping.")
#            return
