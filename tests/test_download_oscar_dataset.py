import os
import json
import tempfile
import unittest
from datasets import Dataset
from c4_dataset_script.Arabic.download_oscar_dataset import OscarDownloader


class TestDownloadOscarDataset(unittest.TestCase):

    def setUp(self):
        # Define a custom dataset to use in the tests
        self.custom_dataset = Dataset.from_dict({
            "train": [
                {
                    "text": "Sample text 1",
                    "meta": json.dumps({
                        "warc-date": "2023-08-30",
                        "warc-target-uri": "https://example.com/page1"
                    })
                },
                {
                    "text": "Sample text 2",
                    "meta": json.dumps({
                        "warc-date": "2023-08-31",
                        "warc-target-uri": "https://example.com/page2"
                    })
                },
            ]
        })

        # Create a temporary folder for testing
        self.output_folder = tempfile.mkdtemp()
        os.makedirs(self.output_folder, exist_ok=True)

    def tearDown(self):
        # Remove the temporary folder after testing
        for filename in os.listdir(self.output_folder):
            os.remove(os.path.join(self.output_folder, filename))
        os.rmdir(self.output_folder)

    def test_convert_oscar_to_json(self):

        #oscar_downloader = OscarDownloader('nthngdy/oscar-mini', 'unshuffled_deduplicated_ar', self.output_folder, 2)
        oscar_downloader = OscarDownloader('', '', self.output_folder, 1)
        oscar_downloader.dataset = self.custom_dataset
        oscar_downloader.convert_oscar_to_json()
        # Verify that JSON files were created in the output folder
        files_created = os.listdir(self.output_folder)
        self.assertTrue(len(files_created) > 0)

        # Check the content of the created JSON files
        for filename in files_created:
            with open(os.path.join(self.output_folder, filename), "r") as json_file:
                commoncrawl_json = json.load(json_file)
                self.assertTrue("text" in commoncrawl_json)
                self.assertTrue("url" in commoncrawl_json)
                self.assertTrue("timestamp" in commoncrawl_json)

if __name__ == "__main__":
    unittest.main()
