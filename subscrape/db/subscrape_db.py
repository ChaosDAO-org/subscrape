import os
import io
import json
import logging


# one DB per Parachain
class SubscrapeDB:

    def __init__(self, name):
        self.logger = logging.getLogger("SubscrapeDB")
        self._path = f"data/parachains/{name}_"
        # the name of the currently loaded extrinsics
        self._extrinsics_name = None
        # the name of the currently loaded extrinsics sector
        self._extrinsics_sector_name = None
        # the currently loaded extrinsics sector
        self._extrinsics = None
        # a dirty flag that keeps track of unsaved changes
        self._dirty = False
        self.digits_per_sector = 6

    def _extrinsics_folder(self, name):
        return f"{self._path}extrinsics_{name}/"

    def _extrinsics_sector_file_path(self, extrinsic, sector):
        return self._extrinsics_folder(extrinsic) + sector +  ".json"

    def _clean_state(self):
        assert(not self._dirty)
        # remove references to existing sectors
        self._extrinsics = None
        self._extrinsics_sector_name = None

    def set_active_extrinsics_call(self, call_module, call_name):
        self._clean_state()
        call_string = f"{call_module}_{call_name}"

        self._extrinsics_name = call_string
        # make sure folder exists
        folder_path = self._extrinsics_folder(call_string)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

    # the method assumes that consumers go through sorted block lists
    # it will load a sector from disk when it becomes active
    # and unloads it once a new one becomes active
    def write_extrinsic(self, extrinsic):
        # determine the sector
        index = extrinsic["extrinsic_index"]
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
            self._extrinsics = self._load_extrinsics_sector(self._extrinsics_name, sector)
            self.logger.info(f"Switched to sector {self._extrinsics_sector_name}. {len(self._extrinsics)} active entries")
        

        # do we already know this extrinsic? 
        if index in self._extrinsics:
            return False

        self._dirty = True
        payload = json.dumps(extrinsic)
        self._extrinsics[index] = payload

        return True

    def flush_extrinsics(self):
        if not self._dirty:
            return

        file_path = self._extrinsics_sector_file_path(self._extrinsics_name, self._extrinsics_sector_name)
        self.logger.info(f"Writing {len(self._extrinsics)} entries")
        payload = json.dumps(self._extrinsics)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._dirty = False
        
    def _load_extrinsics_sector(self, name, sector):
        file_path = self._extrinsics_sector_file_path(name, sector)
        if os.path.exists(file_path):
            file = io.open(file_path)
            file_payload = file.read()
            return json.loads(file_payload)
        else:
            return {}
            
    def extrinsics_iter(self, call_module, call_name):
        self.set_active_extrinsics_call(call_module, call_name)
        call_string = f"{call_module}_{call_name}"
        
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
                    file = io.open(folder + file_path)
                    file_payload = file.read()
                    self.extrinsics = json.loads(file_payload)
                    self.extrinsics_keys = list(self.extrinsics.keys())

                    self.extrinsics_index = 0
                    self.file_index += 1
                
                key = self.extrinsics_keys[self.extrinsics_index]
                result = self.extrinsics[key]
                self.extrinsics_index += 1

                return key, result

        folder = self._extrinsics_folder(call_string)
        file_list = os.listdir(folder)
        return ExtrinsicsIter(folder, file_list)
