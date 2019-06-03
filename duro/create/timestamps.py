from typing import List, Optional

import arrow

events = {
    "start": "Started",
    "connect": "Connected to Redshift",
    "select": "Selected data from Redshift",
    "create_temp": "Created temporary table",
    "process": "Processed selected data",
    "csv": "Exported processed data to CSV",
    "s3": "Uploaded processed data to S3",
    "insert": "Uploaded processed data to Redshift",
    "clean_csv": "Removed CSV files",
    "tests": "Run tests",
    "replace_old": "Replaced old table",
    "drop_old": "Dropped old table",
    "make_snapshot": "Made snapshot"
}


# pylint: disable=attribute-defined-outside-init
# noinspection PyAttributeOutsideInit
class Timestamps:
    __slots__ = list(events.keys()) + ["finish"]

    def log(self, event: str):
        setattr(self, event, arrow.now().timestamp)
        if event in ("drop_old", "insert"):
            self.finish = getattr(self, event)

    @property
    def events(self) -> List:
        return self.__slots__

    @property
    def values(self) -> List:
        return [getattr(self, event, None) for event in self.__slots__]

    # pylint: disable=no-member
    # noinspection PyUnresolvedReferences
    @property
    def duration(self) -> Optional[int]:
        if getattr(self, "finish", None):
            return self.finish - self.start
        return None
