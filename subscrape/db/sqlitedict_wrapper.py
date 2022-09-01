import logging
from sqlitedict import SqliteDict

class SqliteDictWrapper:
    """
    A wrapper for the sqlitedict library.
    """

    def __init__(self, path, log_description):
        """
        :param path: str: the path to the sqlite database
        :param log_description: str: a description of the specific database for logging purposes
        """
        self.logger = logging.getLogger(__name__)
        self.db = SqliteDict(path)
        self._write_count = 0
        self._log_description = log_description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def __contains__(self, index):
        return index in self.db

    def write_item(self, index, data) -> bool:
        """
        Write a single item to the database.
        :param index: str: the index of the item to write
        :param data: dict: the data to write
        :return: bool: True if the item was new, False if it was already in the database
        """
        was_new_element = index not in self.db
        self.db[index] = data

        self._write_count += 1
        if self._write_count % 1000 == 0:
            self.db.commit()
            self.logger.info(f"{self._log_description} - wrote {self._write_count} entries")

        return was_new_element
    
    def flush(self):
        self.db.commit()

    def read_item(self, index: str) -> dict:
        """
        Read a single item from the database.
        :param index: str: the index of the item to read
        :return: dict: the data at the index
        """
        return self.db[index]

    def get_iter(self):
        return self.db.iteritems()