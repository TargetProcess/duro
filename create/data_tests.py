import os
from logging import Logger
from typing import Tuple, List, Optional

from utils.file_utils import read_file


def load_tests(table: str, path: str, logger: Logger) -> str:
    logger.info(f'Loading tests for {table}')
    folder, file = table.split('.')
    tests_file = os.path.join(path, folder, f'{file}_test.sql')
    if os.path.isfile(tests_file):
        return read_file(tests_file).replace(f'{table}', f'{table}_temp')
    else:
        tests_file = os.path.join(path, f'{table}_test.sql')
        if os.path.isfile(tests_file):
            return read_file(tests_file).replace(f'{table}', f'{table}_temp')
    logger.info(f'No tests for {table}')
    return ''


def run_tests(tests_queries: str, connection, logger: Logger) -> Tuple[bool, Optional[List]]:
    if len(tests_queries) == 0:
        return True, None

    logger.info(f'Running tests')
    with connection.cursor() as cursor:
        queries = (q for q in tests_queries.split(';') if len(q) > 0)
        results = []
        for query in queries:
            cursor.execute(query)
            results.append((cursor.description[0].name,
                            cursor.fetchone()[0]))

        passed = all((result[1] for result in results))
        if not passed:
            failed_columns = [result[0] for result in results if not result[1]]
            logger.info(f'Failed tests: {failed_columns}')
            return passed, failed_columns

        return passed, None
