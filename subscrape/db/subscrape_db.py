import io
import json


class SubscrapeDB:
    def __init__(self, path):
        self.path = path
        self.addresses = []
        self.extrinsics = {}

    def warmup_extrinsic_index(self, name):
        self.extrinsics[name] = {}

    def set_extrinsic(self, name, index, payload):
        self.extrinsics[name][index] = payload

    def save_extrinsics(self, name):
        file_path = self.path + f"extrinsics_{name}.json"
        payload = json.dumps(self.extrinsics[name])
        file = io.open(file_path, "w")
        file.write(payload)
        file.close()
        return True

#        file_path = self.db_path + f"extrinsics_{call_module}_{call_name}.json"
#        if os.path.exists(file_path):
#            self.logger.warn(f"{file_path} already exists. Skipping.")
#            return
