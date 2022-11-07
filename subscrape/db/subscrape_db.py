__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import logging
from substrateinterface.utils import ss58
from sqlalchemy import create_engine, Table, Column, Integer, String, Boolean, JSON, DateTime, ForeignKey
from sqlalchemy.orm import Session, Query
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
    module = Column(String(100))
    call = Column(String(100))
    address = Column(String(100))
    nonce = Column(Integer)
    extrinsic_hash = Column(String(100))
    success = Column(Boolean)
    params = Column(JSON)
    # event
    # event_count
    fee = Column(Integer)
    fee_used = Column(Integer)
    error = Column(JSON)
    finalized = Column(Boolean)
    tip = Column(Integer)

class Event(Base):
    __tablename__ = 'events'
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    extrinsic_id = Column(Integer)
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

    def extrinsics_query(self, module: str = None, call: str = None, extrinsic_ids: list = None) -> Query:
        """
        Returns a query object for extrinsics.

        :param module: The module to filter for
        :type module: str
        :param call: The call to filter for
        :type call: str
        :return: The query object
        :rtype: Query
        """
        query = self._session.query(Extrinsic)
        if module is not None:
            query = query.filter(Extrinsic.module == module)
        if call is not None:
            query = query.filter(Extrinsic.call == call)
        if extrinsic_ids is not None:
            query = query.filter(Extrinsic.id.in_(extrinsic_ids))

        return query

    def read_extrinsic(self, extrinsic_id) -> Extrinsic:
        """
        Returns the extrinsic with the given id.

        :param extrinsic_id: The id of the extrinsic
        :type extrinsic_id: str
        :return: The extrinsic
        :rtype: Extrinsic
        """
        return self._session.query(Extrinsic).get(extrinsic_id)

    """ # Events """

    def events_query(self, module=None, event=None, event_ids: list = None) -> Query:       
        """
        Returns a query object for events.

        :param module: The module to filter for
        :type module: str
        :param event: The event to filter for
        :type event: str
        :param event_ids: The ids of the events to filter for
        :type event_ids: list
        :return: The query object
        :rtype: Query
        """
        query = self._session.query(Event)
        if module is not None:
            query = query.filter(Event.module == module)
        if event is not None:
            query = query.filter(Event.event == event)
        if event_ids is not None:
            query = query.filter(Event.id.in_(event_ids))
        return query

    def read_event(self, event_id) -> Event:
        """
        Reads an event with a given id from the database.

        :param event_id: The id of the event to read, e.g. "123456-12"
        :type event_id: str
        :return: The event
        :rtype: Event
        """
        result = self._session.query(Event).get(event_id)
        return result

