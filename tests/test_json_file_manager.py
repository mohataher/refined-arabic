import tempfile
import unittest

from c4_dataset_script.json_file_manager import JsonlFileManager


class TestJsonlFileManager(unittest.TestCase):

    def test_save_dict(self):
        directory = tempfile.gettempdir()
        print(directory)
        manager = JsonlFileManager(directory)
        for i in range(10003):
            manager.save_dict('{}')
        manager.close()
        self.assertTrue(True)

