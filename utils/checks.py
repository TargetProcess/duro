from abc import ABC, abstractmethod
from typing import Optional, Iterable

from utils.file_utils import (load_query, list_processors,
                              load_select_query, list_tests,
                              test_postfix, load_ddl_query)


class Check(ABC):
    def __init__(self, views_path: str):
        self.views_path = views_path

    @abstractmethod
    def check(self, table):
        pass

    @abstractmethod
    def _list_tables(self) -> Iterable[str]:
        pass

    @property
    @abstractmethod
    def message(self) -> str:
        pass

    def run(self) -> Optional[str]:
        tables = self._list_tables()
        failures = []
        for table in tables:
            try:
                check_result = self.check(table)
            except (OSError, ValueError):
                failures.append(table)
                continue

            if not check_result:
                failures.append(table)

        if not failures:
            return None

        return f'{self.message}: {", ".join(failures)}.'


class TestsWithoutQuery(Check):
    def _list_tables(self):
        tests = list_tests(self.views_path)
        return (t.replace(f'{test_postfix}', '')
                for t in tests)

    def check(self, table):
        return load_query(self.views_path, table)

    @property
    def message(self) -> str:
        return 'Some tables have tests, but not a SELECT query'


class ProcessorsWithoutSelect(Check):
    def _list_tables(self) -> Iterable[str]:
        return list_processors(self.views_path)

    def check(self, table):
        return load_select_query(self.views_path, table)

    @property
    def message(self) -> str:
        return 'Some processors don’t have a SELECT query'


class ProcessorsWithoutDDL(Check):
    def _list_tables(self) -> Iterable[str]:
        return list_processors(self.views_path)

    def check(self, table):
        return load_ddl_query(self.views_path, table)

    @property
    def message(self) -> str:
        return 'Some processors don’t have a CREATE TABLE query'


enabled_checks = (TestsWithoutQuery,
                  ProcessorsWithoutSelect,
                  ProcessorsWithoutDDL)


def find_tables_with_missing_files(views_path: str) -> Optional[str]:
    check_results = [check(views_path).run()
                     for check in enabled_checks]

    failed = (result for result in check_results if result)

    return '\n'.join(failed)
