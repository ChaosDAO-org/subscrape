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
        #: SqliteDict: the index of all extrinsics
        self._extrinsics_storage = SqliteDictWrapper(self._path + "extrinsics.sqlite", f"{parachain}.extrinsics")
        
        self._extrinsics_index_managers = {}


    """ # Extrinsics """

    def storage_manager_for_extrinsics_call(self, module, call):
        """
        returns a SqliteDictWrapper to store and retrieve extrinsics
        """
        name = f"{self._parachain}.{module}.{call}"
        if name in self._extrinsics_index_managers:
            return self._extrinsics_index_managers[name]

        path = f"{self._path}extrinsics_index_{module}_{call}.sqlite"
        sm = SqliteDictWrapper(path, name)
        self._extrinsics_index_managers[name] = sm
        return sm

    def storage_manager_for_extrinsics(self):
        return self._extrinsics_storage

    def write_extrinsic(self, index, extrinsic) -> bool:
        """
        Write hydrated extrinsics to the database.

        :param index: The index of the extrinsic
        :type index: str
        :param extrinsic: The extrinsic to write
        :type extrinsic: dict
        """
                
        was_new_element = self._extrinsics_storage.write_item(index, extrinsic)
        
        if was_new_element:
            self.logger.info(f"Extrinsic {index} already exists in the database. Overwriting.")

        return was_new_element

    def flush_extrinsics(self):
        """
        Flush the extrinsics to the database.
        """
        self._extrinsics_storage.flush()

    def read_extrinsic(self, extrinsic_index):
        """
        Reads an extrinsic with a given index from the database.

        :param extrinsic_index: The index of the extrinsic to read, e.g. "123456-12"
        :return: The extrinsic
        """
        return self._extrinsics_storage.read_item(extrinsic_index)

    """ # Events """

    def storage_manager_for_events_call(self, module, event):
        path = f"{self._path}events_{module}_{event}.sqlite"
        log_description = f"{self._parachain}.{module}.{event}"
        return SqliteDictWrapper(path, log_description)


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
