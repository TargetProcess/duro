import os
import csv

from create.process import run_processor
from utils.file_utils import find_processor


def test_run_processor(views_path):
    table = "first.countries"
    processor = find_processor(views_path, table)
    selected = "first.countries_selected.csv"
    processed = "first.countries_processed.csv"

    with open(selected, "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows([["a", "b"], [1, 2], [3, 4]])

    run_processor(views_path, processor, table, selected, processed)

    with open(processed) as csvfile:
        reader = csv.reader(csvfile)
        processing_result = list(reader)
        assert processing_result == [["a", "b"], ["3", "7"]]

    os.remove(selected)
    os.remove(processed)
