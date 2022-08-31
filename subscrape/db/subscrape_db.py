__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import io
import json
import logging
from substrateinterface.utils import ss58
from subscrape.db.sqlitedict_wrapper import SqliteDictWrapper
from sqlitedict import SqliteDict

# one DB per Parachain
class SubscrapeDB:
    """
    This class is used to support online scraping of various types of data.
    The write_<type>() methods are used as callbacks for the scraper that constantly feeds new data from web responses.
    To accommodate this behavior, before scraping begins the DB object must be parameterized by calling
    set_active_<type>().
    At the end of the process, flush_<type>() is called to make sure the state is properly saved.
    """

    def __init__(self, parachain):
        self.logger = logging.getLogger("SubscrapeDB")

        # make sure casing is correct
        parachain = parachain.lower()

        # make sure the parachains folder exists
        folder_path = f"{os.getcwd()}/data/parachains/"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        #: str: the root path to this db
        self._path = f"data/parachains/{parachain}_"
        #: str: the name of the chain this db represents
        self._parachain = parachain
        self._extrinsics_meta_index = SqliteDict(f"{self._path}extrinsics_meta_index.sqlite", autocommit=True)

    """ # Extrinsics """

    def storage_manager_for_extrinsics_call(self, module, call):
        """
        returns a `SectorizedStorageManager` to store and retrieve extrinsics
        """
        path = f"{self._path}extrinsics_{module}_{call}.sqlite"
        log_description = f"{self._parachain}.{module}.{call}"
        index_for_item = lambda item: item["extrinsic_index"]
        return SqliteDictWrapper(path, log_description, index_for_item)

    def write_extrinsic(self, data):
        """
        Write a single extrinsic to the storage.
        This might be done expensive when done with a lot of extrinsics,
        but is the intended approach when the module and call of extrinsics being scraped is unknown
        ahead of scraping.

        This method will determine the module and call, instantiate a new storage manager for the extrinsic,
        and write the extrinsic to the storage.
        """

        # determine the module and call of the extrinsic
        module = data["call_module"]
        call = data["call_module_function"]
        extrinsic_index = data["extrinsic_index"]

        # instantiate a new storage manager for the extrinsic
        storage_manager = self.storage_manager_for_extrinsics_call(module, call)
        storage_manager.write_item(data)
        storage_manager.flush()

        self._extrinsics_meta_index[extrinsic_index] = {
            "module": module,
            "call": call
        }

    def read_extrinsic(self, extrinsic_index):
        """
        Reads an extrinsic with a given index from the database.
        """
        obj = self._extrinsics_meta_index[extrinsic_index]
        module = obj["module"]
        call = obj["call"]
        storage_manager = self.storage_manager_for_extrinsics_call(module, call)
        return storage_manager.read_item(extrinsic_index)

    """ # Events """

    def storage_manager_for_events_call(self, module, event):
        path = f"{self._path}events_{module}_{event}.sqlite"
        log_description = f"{self._parachain}.{module}.{event}"
        index_for_item = lambda item: f"{item['block_num']}-{item['event_idx']}"
        return SqliteDictWrapper(path, log_description, index_for_item)


    # Transfers

    def _transfers_folder(self):
        """
        Returns the folder where transfers are stored.
        """
        return f"{self._path}transfers/"

    def _transfers_account_file(self, account):
        """
        Will return the storage path for an account.
        The address is normalized to the Substrate address format.

        :param account: The account
        :type account: str
        """
        public_key = ss58.ss58_decode(account)
        substrate_address = ss58.ss58_encode(public_key, ss58_format=42)
        file_path = self._transfers_folder() + substrate_address + ".json"
        return file_path

    def _clear_transfers_state(self):
        """
        Clears the internal state of a transfers
        """
        assert(not self._transfers_dirty)
        self._transfers_account = None
        self._transfers = None

    def set_active_transfers_account(self, account):
        """
        Sets the active transfers account for the upcoming scrape.

        :param account: The account we are about to scrape
        :type account: str
        """
        self._clear_transfers_state()
        self._transfers_account = account
        self._transfers = []

        # make sure folder exists
        folder_path = self._transfers_folder()
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    def write_transfer(self, transfer):
        """
        Write a new transfer object to the state.

        :param transfer: The transfer to write
        :type transfer: dict
        """
        self._transfers.append(transfer)
        self._transfers_dirty = True
        return True

    def flush_transfers(self):
        """
        Flushes the unsaved transfers to disk.
        """
        if not self._transfers_dirty:
            return

        payload = json.dumps(self._transfers)

        file_path = self._transfers_account_file(self._transfers_account)
        self.logger.info(f"Writing {len(self._transfers)} entries")
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

        self._transfers_dirty = False

    def transfers_iter(self, account):
        """
        Returns an iterable object of transfers for the given account.

        :param account: The account to read transfers for
        :type account: str
        """
        file_path = self._transfers_account_file(account)
        if os.path.exists(file_path):
            file = io.open(file_path)
            file_payload = file.read()
            return json.loads(file_payload)
        else:
            self.logger.warning(f"transfers from account {account} have been requested but do not exist on disk.")
            return None
