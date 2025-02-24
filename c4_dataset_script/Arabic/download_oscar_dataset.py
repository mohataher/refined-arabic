# Copyright (c) 2023 Mohamed Taher Alrefaie
import argparse
import os
import json

from datasets import load_dataset

from c4_dataset_script.json_file_manager import JsonlFileManager


class OscarDownloader:
    def __init__(self, dataset_name, dataset_name_subset, output_dir, num_proc):
        self.dataset = None
        self.dataset_name = dataset_name
        self.dataset_name_subset = dataset_name_subset
        self.output_dir = output_dir
        self.num_proc = num_proc
        self.saver = JsonlFileManager(output_dir)

    def download_oscar(self, oscar='oscar-corpus/OSCAR-2301', oscar_subset='ar'):
        return load_dataset(oscar, oscar_subset, num_proc=self.num_proc)
        pass

    def convert_to_commoncrawl_json(self, row):
        row = row['train']
        metadata = json.loads(row["meta"])
        timestamp = metadata.get("warc-date", "")
        url = metadata.get("warc-target-uri", "")
        text = row["text"]
        commoncrawl_json = {
            "text": text,
            "url": url,
            "timestamp": timestamp
        }
        return commoncrawl_json

    def save_as_json(self, row):
        commoncrawl_json = self.convert_to_commoncrawl_json(row)
        # Generate a unique filename based on the URL
        self.saver.save_dict(commoncrawl_json)

    def convert_oscar_to_json(self):
        if not hasattr(self, 'dataset') or self.dataset is None:
            self.dataset = self.download_oscar(self.dataset_name, self.dataset_name_subset)
        os.makedirs(self.output_dir, exist_ok=True)
        self.dataset.map(self.save_as_json, num_proc=self.num_proc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--oscar-dataset", required=True)
    parser.add_argument("--oscar-dataset-subset", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--num-processors", default=2, type=int, required=False)

    args = parser.parse_args()

    print('downloading dataset', args.oscar_dataset, 'with subset',
          args.oscar_dataset_subset, 'with processors', args.num_processors,
          'on folder', args.output_dir)

    oscar_downloader = OscarDownloader(args.oscar_dataset, args.oscar_dataset_subset,
                                       args.output_dir, num_proc=args.num_processors)
    oscar_downloader.convert_oscar_to_json()


if __name__ == "__main__":
    main()
