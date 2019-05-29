import os
from typing import Tuple, List, Optional

from duro.utils.file_utils import read_file
from duro.utils.logger import log_action, setup_logger
from duro.utils.table import temp_postfix

TestResults = Tuple[bool, Optional[List]]

logger = setup_logger()


def load_tests(table: str, path: str) -> str:
    has_tests, tests_file = find_tests(table, path)
    if not has_tests:
        logger.info(f"No tests for {table}")
        return ""

    logger.info(f"Tests file found for {table}")
    return read_file(tests_file).replace(f"{table}", f"{table}{temp_postfix}")


def find_tests(table: str, path: str) -> Tuple[bool, str]:
    folder, file = table.split(".")

    inside_folder_file = os.path.join(path, folder, f"{file}_test.sql")
    if os.path.isfile(inside_folder_file):
        return True, inside_folder_file

    outside_folder_file = os.path.join(path, f"{table}_test.sql")
    if os.path.isfile(outside_folder_file):
        return True, outside_folder_file

    return False, ""


@log_action("run tests")
def run_tests(tests_queries: str, connection) -> TestResults:
    if not tests_queries:
        return True, None

    with connection.cursor() as cursor:
        queries = (q for q in tests_queries.split(";") if len(q) > 0)
        results = []
        for query in queries:
            cursor.execute(query)
            results.append((cursor.description[0].name, cursor.fetchone()[0]))

    passed, failed_columns = parse_tests_results(results)
    if failed_columns:
        logger.info(f"Failed tests: {failed_columns}")
    return passed, failed_columns


def parse_tests_results(results) -> TestResults:
    passed = all((result[1] for result in results))
    if not passed:
        failed_columns = [result[0] for result in results if not result[1]]

        return passed, failed_columns

    return passed, None
