import logging
from sqlitedict import SqliteDict


class SqliteDictWrapper:
    """
    A wrapper for the sqlitedict library.
    """

    def __init__(self, path, log_description, index_for_item):
        """
        :param path: str: the path to the sqlite database
        :param index_for_item: function: a function that returns the index of an item
        :param log_description: str: a description of the specific database for logging purposes
        """
        self.logger = logging.getLogger(__name__)
        self._index_for_item = index_for_item
        self.db = SqliteDict(path)
        self._write_count = 0
        self._log_description = log_description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def write_item(self, data):
        index = self._index_for_item(data)
        was_new_element = index not in self.db
        self.db[index] = data

        self._write_count += 1
        if self._write_count % 1000 == 0:
            self.db.commit()
            self.logger.info(f"{self._log_description} - wrote {self._write_count} entries")

        return was_new_element

    def flush(self):
        self.db.commit()

    def read_item(self, index):
        return self.db[index]

    def get_iter(self):
        return self.db.iteritems()
