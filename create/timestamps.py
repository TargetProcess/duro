from datetime import datetime as dt


class Timestamps:
    __slots__ = ('start', 'connect', 'select',
                 'create_temp', 'process', 'csv',
                 's3', 'insert', 'clean_csv',
                 'tests', 'replace_old', 'drop_old')

    def log(self, event: str):
        setattr(self, event, int(dt.now().timestamp()))

    @property
    def events(self):
        return self.__slots__

    @property
    def values(self):
        return [getattr(self, event, None) for event in self.__slots__]