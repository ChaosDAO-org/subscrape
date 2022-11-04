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

class ExtrinsicMetadata(Base):
    __tablename__ = 'extrinsics_metadata'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    index = Column(Integer)

class Extrinsic(Base):
    __tablename__ = 'extrinsics'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    index = Column(Integer)

class EventMetadata(Base):
    __tablename__ = 'events_metadata'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    extrinsic_id = Column(String(20), ForeignKey('extrinsics.id'))
    module = Column(String(100))
    event = Column(String(100))
    finalized = Column(Boolean)

class Event(Base):
    __tablename__ = 'events'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    extrinsic_id = Column(Integer)
    module = Column(String(100))
    event = Column(String(100))
    # In the Subscan API, `params` is only delivered in the `events call of
    # API v1 and in the `event` call. When using the `events` call of API v2,
    # we need to make sure we hydrate them by calling `event` for each event.
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

    def __init__(self, connection_string="sqlite:///data/cache/default.db"):
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
        

    def flush(self):
        """
        Flush the extrinsics to the database.
        """
        self._session.commit()

    def close(self):
        """
        Close the database connection.
        """
        self._session.close()

    def write_item(self, item: Base):
        """
        Write this item to the database.

        :param item: The item to write
        :type item: Base
        """
        self._session.add(item)


    """ # Extrinsics """

    def extrinsics_query(self, module=None, call=None):
        """
        Returns a query object for extrinsics.

        :param module: The module to filter for
        :type module: str
        :param call: The call to filter for
        :type call: str
        """
        query = self._session.query(Extrinsic)
        if module is not None:
            query = query.filter(Extrinsic.module == module)
        if call is not None:
            query = query.filter(Extrinsic.call == call)
        return query

    def missing_extrinsics_from_index_list(self, index_list):
        """
        Returns a list of all extrinsics that are not in the database.
        """
        matches = self._session.query(ExtrinsicMetadata).filter(ExtrinsicMetadata.id.in_(index_list)).all()
        # find all indices that are not in the matches
        return [index for index in index_list if index not in [match.id for match in matches]]

    """ # Events """

    def events_query(self, module=None, event=None):        
        """
        Returns a query object for events.

        :param module: The module to filter for
        :type module: str
        :param event: The event to filter for
        :type event: str
        """
        query = self._session.query(EventMetadata)
        if module is not None:
            query = query.filter(EventMetadata.module == module)
        if event is not None:
            query = query.filter(EventMetadata.event == event)
        return query

    def read_events(self):
        """
        Reads all events from the database.

        :return: The events
        """
        raise NotImplementedError()

    def missing_events_from_index_list(self, index_list):
        """
        Returns a list of all events that are not in the database.
        """
        matches = self._session.query(EventMetadata).filter(EventMetadata.id.in_(index_list)).all()
        # find all indices that are not in the matches
        return [index for index in index_list if index not in [match.id for match in matches]]

    def read_event_metadata(self, event_id) -> EventMetadata:
        """
        Reads an event with a given id from the database.

        :param event_id: The id of the event to read, e.g. "123456-12"
        :return: The event
        """
        result = self._session.query(EventMetadata).get(event_id)
        return result

    def read_event(self, event_id) -> EventMetadata:
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

