__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import io
import json
import logging
from substrateinterface.utils import ss58
from subscrape.db.sectorized_storage_manager import SectorizedStorageManager

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

        #: str: the root path to this db
        self._path = f"data/parachains/{parachain}_"
        #: str: the name of the chain this db represents
        self._parachain = parachain

        #: str: the account transfers are being store for
        self._transfers_account = None
        #: list: the list of transfers we are storing
        self._transfers = None
        #: bool: a dirty flag that keeps track of unsaved transfers
        self._transfers_dirty = False

    def storage_manager_for_extrinsics_call(self, call_module, call_name):
        """
        returns a `SectorizedStorageManager` to store and retrieve extrinsics
        """
        folder = f"{self._path}extrinsics_{call_module}_{call_name}/"
        description = f"{self._parachain} {call_module}.{call_name}"
        index_for_item = lambda item: item["extrinsic_index"]
        return SectorizedStorageManager(folder, description, index_for_item)

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
