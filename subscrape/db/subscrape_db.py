__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import io
import json
import logging
from substrateinterface.utils import ss58
from subscrape.db.sqlitedict_wrapper import SqliteDictWrapper
import sqlite3

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
        #: SqliteDict: the index of all events
        self._events_storage = SqliteDictWrapper(self._path + "events.sqlite", f"{parachain}.events")

        self._extrinsics_index_managers = {}
        self._events_index_managers = {}
        self._transfers_index_managers = {}


    """ # Extrinsics """

    def storage_manager_for_extrinsics_call(self, module, call):
        """
        returns a SqliteDictWrapper to store and retrieve extrinsics
        """
        module = module.lower()
        call = call.lower()
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
        Write extrinsic to the database.

        :param index: The index of the extrinsic
        :type index: str
        :param extrinsic: The extrinsic to write
        :type extrinsic: dict
        """
                
        was_new_element = self._extrinsics_storage.write_item(index, extrinsic)
        
        if not was_new_element:
            self.logger.warning(f"Extrinsic {index} already exists in the database. This should be prevented by the scraper by checking `has_extrinsic`.")

        return was_new_element

    def flush_extrinsics(self):
        """
        Flush the extrinsics to the database.
        """
        self._extrinsics_storage.flush()

    def has_extrinsic(self, extrinsic_index):
        """
        Returns true if the extrinsic with the given index is in the database.
        """
        return extrinsic_index in self._extrinsics_storage

    def read_extrinsic(self, extrinsic_index):
        """
        Reads an extrinsic with a given index from the database.

        :param extrinsic_index: The index of the extrinsic to read, e.g. "123456-12"
        :return: The extrinsic
        """
        return self._extrinsics_storage.read_item(extrinsic_index)

    """ # Events """

    def storage_manager_for_events_call(self, module, event):
        """
        returns a SqliteDictWrapper to store and retrieve events
        """
        module = module.lower()
        event = event.lower()
        name = f"{self._parachain}.{module}.{event}"
        if name in self._events_index_managers:
            return self._events_index_managers[name]

        path = f"{self._path}events_index_{module}_{event}.sqlite"
        sm = SqliteDictWrapper(path, name)
        self._events_index_managers[name] = sm
        return sm

    def write_event(self, index, event) -> bool:
        """
        Write event to the database.

        :param index: The index of the event
        :type index: str
        :param event: The event to write
        :type event: dict
        """
        was_new_element = self._events_storage.write_item(index, event)

        if not was_new_element:
            self.logger.warning(f"Event {index} already exists in the database. This should be prevented by the scraper by checking `has_event`.")

        return was_new_element

    def flush_events(self):
        """
        Flush the events to the database.
        """
        self._events_storage.flush()

    def has_event(self, event_index):
        """
        Returns true if the event with the given index is in the database.
        """
        return event_index in self._events_storage

    def read_event(self, event_index):
        """
        Reads an event with a given index from the database.

        :param event_index: The index of the event to read, e.g. "123456-12"
        :return: The event
        """
        return self._events_storage.read_item(event_index)
    
    """ # Transfers """

    def storage_manager_for_transfers(self, address):
        """
        returns a SqliteDictWrapper to store and retrieve transfers
        """
        address = ss58.ss58_decode(address)
        address = ss58.ss58_encode(address, 42)
        name = f"{self._parachain}.transfers.{address}"
        if name in self._transfers_index_managers:
            return self._transfers_index_managers[name]

        path = f"{self._path}transfers_index_{address}.sqlite"
        sm = SqliteDictWrapper(path, name)
        self._transfers_index_managers[name] = sm
        return sm

    def write_transfer(self, index, transfer) -> bool:
        """
        Write transfer to the database.

        :param index: The index of the transfer
        :type index: str
        :param transfer: The transfer to write
        :type transfer: dict
        """
        sm = self.storage_manager_for_transfers(transfer["address"])
        was_new_element = sm.write_item(index, transfer)

        if not was_new_element:
            self.logger.warning(f"Transfer {index} already exists in the database.")

        return was_new_element

    def flush_transfers(self):
        """
        Flush the transfers to the database.
        """
        for sm in self._transfers_index_managers.values():
            sm.flush()

    def has_transfer(self, transfer_index):
        """
        Returns true if the transfer with the given index is in the database.
        """
        return transfer_index in self._transfers_storage

    def read_transfer(self, transfer_index):
        """
        Reads an transfer with a given index from the database.

        :param transfer_index: The index of the transfer to read, e.g. "123456-12"
        :return: The transfer
        """
        return self._transfers_storage.read_item(transfer_index)

