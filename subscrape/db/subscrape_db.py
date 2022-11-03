__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import logging
from substrateinterface.utils import ss58
from sqlalchemy import create_engine, Table, Column, Integer, String, Boolean, JSON, DateTime, ForeignKey
from sqlalchemy.orm import Session	
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

class Block(Base):
    __tablename__ = "blocks"
    block_number = Column(Integer, unique=True, primary_key=True)

class Extrinsic(Base):
    __tablename__ = 'extrinsics'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    index = Column(Integer)

class Event(Base):
    __tablename__ = 'events'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    event_index = Column(Integer)
    extrinsic_index = Column(Integer)
    module = Column(String(100))
    event = Column(String(100))
    params = Column(JSON)
    finalized = Column(Boolean)

class SubscrapeDB:
    """
    This class is used to support online scraping of various types of data.
    The write_<type>() methods are used as callbacks for the scraper that constantly feeds new data from web responses.
    To accommodate this behavior, before scraping begins the DB object must be parameterized by calling
    set_active_<type>().
    At the end of the process, flush_<type>() is called to make sure the state is properly saved.
    """

    def __init__(self, connection_string):
        self.logger = logging.getLogger("SubscrapeDB")
        self._engine = create_engine(connection_string)

        if not database_exists(self._engine.url):
            # ensure that the folder exists
            os.makedirs(os.path.dirname(connection_string.replace("sqlite:///", "")), exist_ok=True)
            create_database(self._engine.url)
            self._setup_db()

        self._session = Session(bind=self._engine)

    def _setup_db(self):
        """
        Creates the database tables if they do not exist.
        """
        Base.metadata.create_all(self._engine)
        

    """ # Extrinsics """

    def flush(self):
        """
        Flush the extrinsics to the database.
        """
        self._session.commit()

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

    def write_event(self, event: Event):
        """
        Write event to the database.

        :param index: The index of the event
        :type index: str
        :param event: The event to write
        :type event: Event
        """
        self._session.add(event)

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

    def missing_events_from_index_list(self, index_list):
        """
        Returns a list of all events that are not in the database.
        """
        matches = self._session.query(Event).filter(Event.id.in_(index_list)).all()
        # find all indices that are not in the matches
        return [index for index in index_list if index not in [match.id for match in matches]]

    def read_event(self, event_id) -> Event:
        """
        Reads an event with a given id from the database.

        :param event_id: The id of the event to read, e.g. "123456-12"
        :return: The event
        """
        result = self._session.query(Event).get(event_id)
        return result
    
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

