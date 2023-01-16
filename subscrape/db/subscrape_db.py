__author__ = 'Tommi Enenkel @alice_und_bob'

import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Boolean, JSON, DateTime, ForeignKey, ForeignKeyConstraint
from sqlalchemy.orm import Session, Query, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

class Block(Base):
    __tablename__ = "blocks"
    block_number = Column(Integer, unique=True, primary_key=True)

class Extrinsic(Base):
    __tablename__ = 'extrinsics'
    chain = Column(String(50), primary_key=True)
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    block_timestamp = Column(DateTime)
    module = Column(String(100))
    call = Column(String(100))
    origin_address = Column(String(100))
    origin_public_key = Column(String(100))
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

    events = relationship("Event", back_populates="extrinsic")


class Event(Base):
    __tablename__ = 'events'
    chain = Column(String(50), primary_key=True)
    id = Column(String(20), primary_key=True)
    block_number = Column(Integer, ForeignKey('blocks.block_number'))
    block_timestamp = Column(DateTime) # only present in the Subscan metadata
    extrinsic_id = Column(String(20))
    module = Column(String(100))
    event = Column(String(100))
    params = Column(JSON)
    finalized = Column(Boolean)

    __table_args__ = (
        ForeignKeyConstraint([extrinsic_id, chain],
                             [Extrinsic.id, Extrinsic.chain]),
    )

    extrinsic = relationship("Extrinsic", back_populates="events")


class SubscrapeDB:
    """
    This class is used to support online scraping of various types of data.
    The write_<type>() methods are used as callbacks for the scraper that constantly feeds new data from web responses.
    To accommodate this behavior, before scraping begins the DB object must be parameterized by calling
    set_active_<type>().
    At the end of the process, flush_<type>() is called to make sure the state is properly saved.
    """

    def __init__(self, connection_string="sqlite:///data/cache/default.db"):
        self.logger = logging.getLogger(__name__)
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
        self._extrinsics_storage_managers = {}

    """ # Extrinsics """

    def query_extrinsics(self, chain: str = None, module: str = None, call: str = None, extrinsic_ids: list = None) -> Query:
        """
        Returns a query object for extrinsics.

        :param chain: The chain to filter for
        :type chain: str
        :param module: The module to filter for
        :type module: str
        :param call: The call to filter for
        :type call: str
        :return: The query object
        :rtype: Query
        """
        query = self._session.query(Extrinsic)
        if chain is not None:
            query = query.filter(Extrinsic.chain == chain)
        if module is not None:
            query = query.filter(Extrinsic.module == module)
        if call is not None:
            query = query.filter(Extrinsic.call == call)
        if extrinsic_ids is not None:
            query = query.filter(Extrinsic.id.in_(extrinsic_ids))

        return query

    def query_extrinsic(self, chain: str, extrinsic_id: str) -> Extrinsic:
        """
        Returns the extrinsic with the given id.

        :param chain: The chain to filter for
        :type chain: str
        :param extrinsic_id: The id of the extrinsic
        :type extrinsic_id: str
        :return: The extrinsic
        :rtype: Extrinsic
        """
        return self._session.query(Extrinsic).get((chain, extrinsic_id))

    """ # Events """

    def query_events(self, chain: str = None, module: str = None, event: str = None, event_ids: list = None) -> Query:       
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
        if chain is not None:
            query = query.filter(Event.chain == chain)
        if module is not None:
            query = query.filter(Event.module == module)
        if event is not None:
            query = query.filter(Event.event == event)
        if event_ids is not None:
            query = query.filter(Event.id.in_(event_ids))
        return query

    def query_event(self, chain: str, event_id: str) -> Event:
        """
        Reads an event with a given id from the database.

        :param chain: The chain to filter for
        :type chain: str
        :param event_id: The id of the event to read, e.g. "123456-12"
        :type event_id: str
        :return: The event
        :rtype: Event
        """
        result = self._session.query(Event).get((chain, event_id))
        return result

