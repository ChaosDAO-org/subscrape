import io
import json

class FileSerializer:
    def __init__(self, path):
        self.path = path

    def save_extrinsics(self, name, payload):
        file_path = self.path + f"extrinsics_{name}.json"
        payload = json.dumps(payload)
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()

#        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
#        if os.path.exists(file_path):
#            self.logger.warn(f"{file_path} already exists. Skipping.")
#            return
