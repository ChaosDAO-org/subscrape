__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import io
import json
import logging
from substrateinterface.utils import ss58
import sqlite3

class SubscrapeDB:
    """
    This class is used to support online scraping of various types of data.
    The write_<type>() methods are used as callbacks for the scraper that constantly feeds new data from web responses.
    To accommodate this behavior, before scraping begins the DB object must be parameterized by calling
    set_active_<type>().
    At the end of the process, flush_<type>() is called to make sure the state is properly saved.
    """

    def __init__(self, path):
        self.logger = logging.getLogger("SubscrapeDB")

        # make sure the parachains folder exists
        folder_path = f"{os.getcwd()}/{path}"
        # remove the file name
        folder_path = os.path.dirname(folder_path)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        #: str: the root path to this db
        self._path = path
        self._connection = sqlite3.connect(self._path)

    """ # Extrinsics """

    def flush(self):
        """
        Flush the extrinsics to the database.
        """
        raise NotImplementedError()

    def write_extrinsic(self, index, extrinsic) -> bool:
        """
        Write extrinsic to the database.

        :param index: The index of the extrinsic
        :type index: str
        :param extrinsic: The extrinsic to write
        :type extrinsic: dict
        """
        raise NotImplementedError()
        was_new_element = self._extrinsics_storage.write_item(index, extrinsic)
        
        if not was_new_element:
            self.logger.warning(f"Extrinsic {index} already exists in the database. This should be prevented by the scraper by checking `has_extrinsic`.")

        return was_new_element

    def read_extrinsics(self):
        """
        Reads all extrinsics from the database.

        :return: The extrinsics
        """
        raise NotImplementedError()


    def has_extrinsic(self, extrinsic_index):
        """
        Returns true if the extrinsic with the given index is in the database.
        """
        raise NotImplementedError()

    def read_extrinsic(self, extrinsic_index):
        """
        Reads an extrinsic with a given index from the database.

        :param extrinsic_index: The index of the extrinsic to read, e.g. "123456-12"
        :return: The extrinsic
        """
        raise NotImplementedError()

    """ # Events """

    def write_event(self, index, event) -> bool:
        """
        Write event to the database.

        :param index: The index of the event
        :type index: str
        :param event: The event to write
        :type event: dict
        """

        raise NotImplementedError()

        was_new_element = self._events_storage.write_item(index, event)

        if not was_new_element:
            self.logger.warning(f"Event {index} already exists in the database. This should be prevented by the scraper by checking `has_event`.")

        return was_new_element

    def read_events(self):
        """
        Reads all events from the database.

        :return: The events
        """
        raise NotImplementedError()

    def has_event(self, event_index):
        """
        Returns true if the event with the given index is in the database.
        """
        raise NotImplementedError()

    def read_event(self, event_index):
        """
        Reads an event with a given index from the database.

        :param event_index: The index of the event to read, e.g. "123456-12"
        :return: The event
        """
        raise NotImplementedError()
    
    """ # Transfers """

    def write_transfer(self, index, transfer) -> bool:
        """
        Write transfer to the database.

        :param index: The index of the transfer
        :type index: str
        :param transfer: The transfer to write
        :type transfer: dict
        """

        raise NotImplementedError()

        sm = self.storage_manager_for_transfers(transfer["address"])
        was_new_element = sm.write_item(index, transfer)

        if not was_new_element:
            self.logger.warning(f"Transfer {index} already exists in the database.")

        return was_new_element

    def read_transfers(self):
        """
        Reads all transfers from the database.

        :return: The transfers
        """
        raise NotImplementedError()

    def has_transfer(self, transfer_index):
        """
        Returns true if the transfer with the given index is in the database.
        """
        raise NotImplementedError()

    def read_transfer(self, transfer_index):
        """
        Reads an transfer with a given index from the database.

        :param transfer_index: The index of the transfer to read, e.g. "123456-12"
        :return: The transfer
        """
        raise NotImplementedError()

