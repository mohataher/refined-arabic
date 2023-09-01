import os
import json
import multiprocessing


class JsonlFileManager:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.current_folder = -1
        self.current_file = -1
        self.buffer = []
        self.current_file_count = 0
        self.current_folder_count = 0
        self.lock = multiprocessing.Lock()
        self.ensure_folder_exists()

    def ensure_folder_exists(self):
        while not os.path.exists(self.get_folder_path()):
            os.makedirs(self.get_folder_path())

    def get_folder_path(self):
        return os.path.join(self.base_dir, str(self.current_folder))

    def get_file_path(self):
        return os.path.join(self.get_folder_path(), f"{self.current_file}.jsonl")

    def save_dict(self, data):
        with self.lock:
            self.buffer.append(data)
            self.current_file_count += 1
            if self.current_file_count >= 10000:
                self.flush_buffer()

    def flush_buffer(self):
        with self.lock:
            if not os.path.exists(self.get_folder_path()):
                os.makedirs(self.get_folder_path())
            self.current_file += 1
            self.current_file_count = 0
            with open(self.get_file_path(), 'w') as f:
                for item in self.buffer:
                    f.write(json.dumps(item) + '\n')
                print('file', f, 'is saved')
            self.buffer = []

            if self.current_file >= 100:
                self.current_file = 0
                self.current_folder += 1
                self.current_folder_count += 1
                self.ensure_folder_exists()

    def close(self):
        self.flush_buffer()
