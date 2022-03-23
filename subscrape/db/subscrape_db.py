import os
import io
import json
import logging


# one DB per Parachain
class SubscrapeDB:

    def __init__(self, parachain):
        self.logger = logging.getLogger("SubscrapeDB")
        self._path = f"data/parachains/{parachain}_"
        self._parachain = parachain
        # the name of the currently loaded extrinsics
        self._extrinsics_name = None
        # the name of the currently loaded extrinsics sector
        self._extrinsics_sector_name = None
        # the currently loaded extrinsics sector
        self._extrinsics = None
        # a dirty flag that keeps track of unsaved changes
        self._extrinsics_dirty = False
        self.digits_per_sector = 4

        self._transfers_account = None
        self._transfers = None
        self._transfers_dirty = False


    # Extrinsics

    def _extrinsics_folder(self, module_call):
        """
        Returns the folder where extrinsics of a given module and call are stored.

        :param module_call: The module and call
        :type module_call: str
        """
        return f"{self._path}extrinsics_{module_call}/"


    def _extrinsics_sector_file_path(self, extrinsic, sector):
        return self._extrinsics_folder(extrinsic) + sector +  ".json"


    def _clear_extrinsics_state(self):
        assert(not self._extrinsics_dirty)
        # remove references to existing sectors
        self._extrinsics_name = None
        self._extrinsics = None
        self._extrinsics_sector_name = None


    def set_active_extrinsics_call(self, call_module, call_name):
        self._clear_extrinsics_state()
        call_string = f"{call_module}_{call_name}"

        self._extrinsics_name = call_string
        # make sure folder exists
        folder_path = self._extrinsics_folder(call_string)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)


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
        suffix = "x" * self.digits_per_sector
        if(len(sector) > self.digits_per_sector):
            sector = sector[:-self.digits_per_sector] + suffix
        else:
            sector = suffix

        # did we change sector?
        if self._extrinsics_sector_name != sector:
            # make sure previous data is saved
            self.flush_extrinsics()
            self._clear_extrinsics_state()

            # load sector file or create empty dict
            self._extrinsics_sector_name = sector
            self._extrinsics = self._load_extrinsics_sector(self._extrinsics_name, sector)
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

        file_path = self._extrinsics_sector_file_path(self._extrinsics_name, self._extrinsics_sector_name)
        self.logger.info(f"Writing {len(self._extrinsics)} entries")
        payload = json.dumps(self._extrinsics)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._extrinsics_dirty = False


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

    # Transfers

    def _transfers_folder(self):
        """
        Returns the folder where transfers are stored.
        """
        return f"{self._path}transfers/"

    def _clear_transfers_state(self):
        assert(not self._transfers_dirty)
        self._transfers_account = None
        self._transfers = None


    def set_active_transfers_account(self, account):
        self._clear_transfers_state()
        self._transfers_account = account
        self._transfers = []

        # make sure folder exists
        folder_path = self._transfers_folder()
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)


    def write_transfers(self, transfer):
        self._transfers.append(transfer)
        self._transfers_dirty = True
        return True


    def flush_transfers(self):
        if not self._transfers_dirty:
            return

        payload = json.dumps(self._transfers)

        file_path = self._transfers_folder() + self._transfers_account + ".json"
        self.logger.info(f"Writing {len(self._transfers)} entries")
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._transfers_dirty = False


    def transfers_iter(self, account):
        file_path = self._transfers_folder() + account + ".json"
        if os.path.exists(file_path):
            file = io.open(file_path)
            file_payload = file.read()
            return json.loads(file_payload)
        else:
            self.logger.warn(f"transfers from account {account} have been requested but do not exist on disk.")
            return None
