import os
import json

DATABASE_DIR = './databases/'
DELETE_MARKER = -1

def get_hash_index(db_path):
    try:
        with open(os.path.join(db_path, 'hash_index.json'), 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Database {db_path} does not exist")
    
def save_hash_index(db_path, hash_index):
    try:
        with open(os.path.join(db_path, 'hash_index.json'), 'w') as f:
            json.dump(hash_index, f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Database {db_path} does not exist")

def add_hash_index(db_path, primary_key, offset):
    try:
        hash_index = get_hash_index(db_path)
        hash_index[str(primary_key)] = offset
        save_hash_index(db_path, hash_index)
        return True
    except FileNotFoundError as e:
        return False

def remove_hash_index(db_path, primary_key):
    try:
        # Retrieve the current hash index.
        hash_index = get_hash_index(db_path)
        # Check if the primary key exists in the hash index.
        if str(primary_key) not in hash_index:
            raise KeyError(f"Primary key {primary_key} does not exist in the hash index")
        # Mark the record as deleted.
        hash_index[str(primary_key)] = DELETE_MARKER
        # Save the modified hash index.
        save_hash_index(db_path, hash_index)
        return True  # Indicate success.
    
    except (FileNotFoundError, KeyError) as e:
        print(f"Error removing hash index: {str(e)}")
        return False  # Indicate failure.

def append_to_ndjson(db_path, data):
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'a') as f:
            offset = f.tell()
            json.dump(data, f)
            f.write('\n')
        return offset
    except FileNotFoundError as e:
        return False

def get_primary_key(db_path):
    with open(os.path.join(db_path, 'metadata.json'), 'r') as f:
        metadata = json.load(f)
    return metadata.get('primary_key')

def delete_from_ndjson(db_path, offset):
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r+') as f:
            f.seek(offset)
            original_line = f.readline()
            original_length = len(original_line)
            deletion_marker = {"delete": True, "val": ""}
            deletion_marker_str = json.dumps(deletion_marker)
            val_length = original_length - len(deletion_marker_str) - 1
            if val_length < 0:
                raise ValueError("Original record too short for the deletion marker")
            deletion_marker["val"] = 'x' * val_length
            deletion_marker_str = json.dumps(deletion_marker) + '\n'
            f.seek(offset)
            f.write(deletion_marker_str)
        return True
    except FileNotFoundError:
        print("File not found")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
    
def get_record_from_ndjson(db_path, offset):
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r') as f:
            f.seek(offset)
            record = json.loads(f.readline().strip())
        return record
    except FileNotFoundError as e:
        return False

# Functions for Chunk Processing

def load_chunk_data(db_path, chunk_index, chunk_size):
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r') as f:
            data = []
            for _ in range(chunk_index * chunk_size):
                line = f.readline()
                if not line:
                    break
            for _ in range(chunk_size):
                line = f.readline()
                if not line:
                    break
                item = json.loads(line.strip())
                data.append(item)
        return data
    except FileNotFoundError as e:
        return False

def unload_chunk_data(data):
    # Simply remove the reference to the chunk to free memory.
    del data
