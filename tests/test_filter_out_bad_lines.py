import tempfile
import unittest
import argparse
import gzip
from unittest.mock import patch

from c4_dataset_script.Arabic.filter_out_bad_lines import main, process_file
from c4_dataset_script.json_file_manager import JsonlFileManager


@patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(
    badwords_filepath="../badwords/ar",
    output_bad_lines="./bad_lines.jsonl",
    bad_words_ratio=0.05,
    files_head_path="./download-docs/*/part-*"
))
def test_main():
    main()


def test_process_file():
    class Object(object):
        pass
    manager = JsonlFileManager(tempfile.gettempdir())
    mock_args = Object()
    mock_args.output_bad_lines = tempfile.gettempdir()+'/bad_lines.jsonl'
    mock_args.badwords_filepath = '../c4_dataset_script/badwords/ar'
    mock_args.bad_words_ratio = 0.005
    process_file('./sample_files/part-00001', mock_args, manager)

