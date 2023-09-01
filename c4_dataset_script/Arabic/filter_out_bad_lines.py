"""This script filter out non-sentence lines and toxic text.

```bash
cat docs.jsonl | python filter_out_bad_lines.py --badwords_filepath ../badwords/zh > clean_docs.jsonl
```
"""

import argparse
import glob
import json
import re
import gzip
from multiprocessing import Pool

from tqdm import tqdm

from c4_dataset_script.json_file_manager import JsonlFileManager


def parse_args():
    parser = argparse.ArgumentParser("Filter out bad lines.")
    parser.add_argument("--badwords_filepath", default=None,
                        help="The file path of the toxic word dictionary, if you set this "
                             "argument, the program will filter out which document has over limit of"
                             " toxic word count. The format of the dictionary file is one word per"
                             "line."
                        )
    parser.add_argument("--output_bad_lines", default="bad_lines.jsonl.zst",
                        help="output file for bad lines")
    parser.add_argument("--bad_words_ratio", default=0.05, type=float,
                        help="Document filtering conditions, when the number of bad words in the document exceeds this ratio, it will be screened out.")

    args = parser.parse_args()

    return args


def is_bad_line(line):

    if len(line) < 5:
        return True

    if len(line.strip().split())<=2:
        return True

    return False


def is_bad_doc(args, doc, badwords_filepath):
    bad_words_character_count = 0
    for bad_word in open(badwords_filepath):
        bad_word = bad_word.strip()
        doc_words = set(doc.strip().split())
        if bad_word in doc_words:
            bad_words_character_count += doc.count(bad_word) * len(bad_word)

    if bad_words_character_count / len(doc) > args.bad_words_ratio:
        return True

    return False


def process_file(file, args, manager):
    bad_lines_file = open(args.output_bad_lines, "wt")
    with open(file, 'r') as file:
        # Iterate through each line in the file
        for row in file:
            try:
                j = json.loads(row)
            except Exception as e:
                print(e)
                return

            if args.badwords_filepath is not None:
                if not is_bad_doc(args, j["text"], args.badwords_filepath):
                    manager.save_dict(json.dumps(j, ensure_ascii=False))
                    return

            output = []
            bad_lines = []
            for line in j["text"].splitlines():
                line = line.strip()
                if is_bad_line(line):
                    bad_lines.append(line)
                else:
                    output.append(line)

            if len(output) > 5:
                j["text"] = '\n'.join(output)
                manager.save_dict(json.dumps(j, ensure_ascii=False))
            else:
                bad_lines += output

            if len(bad_lines) > 0:
                j["text"] = '\n'.join(bad_lines)
                print(json.dumps(j, ensure_ascii=False))


def main():
    args = parse_args()

    # Find all files matching './download-docs/*/part-*' pattern
    file_paths = glob.glob(args.files_head_path)

    manager = JsonlFileManager("data")

    #for line in tqdm(file_paths):
    #    process_file(line, args, manager)

    # Use multiprocessing Pool to process files in parallel
    with Pool() as pool:
        pool.starmap(process_file, [(file_path, args, manager) for file_path in file_paths])


if __name__ == "__main__":
    main()
