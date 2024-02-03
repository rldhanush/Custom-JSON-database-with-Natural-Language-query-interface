import os
import json
import shutil
import traceback

DATABASE_DIR = './databases/'

def get_nested_value(data, key_path):
    try:
        for key in key_path:
            data = data[key]
        return data
    except (TypeError, KeyError):
        return None

def delete_database(name):
    path = os.path.join(DATABASE_DIR, name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No database named '{name}' found.")
    try:
        shutil.rmtree(path)
        print(f"Document '{name}' deleted successfully!")
    except OSError as e:
        raise OSError(f"Error deleting database: {str(e)}")

def list_databases():
    try:
        databases = [db for db in os.listdir(DATABASE_DIR) if not db.startswith('.')]
        if databases:
            return databases
    except FileNotFoundError:
        print(f"No databases found. Ensure the {DATABASE_DIR} directory exists.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    return []


def create_metadata(array_data, path, primary_key):
    """
    Creates metadata for a dataset based on the provided array data.
    
    :param array_data: List[Dict] - The data from which to extract metadata.
    :param path: str - Path where metadata file should be saved.
    :param primary_key: str - The primary key for the data records.
    """
    # Gather all attributes from all records in array_data.
    attributes = set()
    for record in array_data:
        attributes.update(record.keys())
    
    # Metadata will store the primary key and all attributes.
    metadata = {
        "primary_key": primary_key,
        "attributes": list(attributes)
    }
    
    metadata_path = os.path.join(path, "metadata.json")
    with open(metadata_path, "w") as meta_file:
        json.dump(metadata, meta_file)

def create_database(name=None):
    if name is None:
        raise ValueError("A database name must be provided.")
    
    path = os.path.join(DATABASE_DIR, name)
    if os.path.exists(path):
        raise FileExistsError(f"Database '{name}' already exists.")
    os.makedirs(path, exist_ok=True)
    
    while True:
        data_path = input("Enter path to dataset: ")
        if os.path.exists(data_path):
            break
        else:
            print(f"File not found: {data_path}. Please try again.")
    
    partition_key = input("Enter the keys to partition data, in the nested order, separated by commas: ").split(',')
    
    try:
        hash_index = {}
        ndjson_path = os.path.join(path, "data.ndjson")
        
        with open(data_path, "r") as infile, open(ndjson_path, "w") as outfile:
            data = json.load(infile)
            array_data = get_nested_value(data, partition_key[:-1]) 

            # Creating metadata.
            create_metadata(array_data, path, partition_key[-1])

            for item in array_data:
                nested_value = get_nested_value(item, [partition_key[-1]])

                if nested_value is None:
                    raise KeyError(f"Invalid key path: {' -> '.join(partition_key)}")

                offset = outfile.tell()
                hash_index[str(nested_value)] = offset
                
                json.dump(item, outfile)
                outfile.write("\n")

        index_path = os.path.join(path, "hash_index.json")
        with open(index_path, "w") as index_file:
            json.dump(hash_index, index_file)
        
        print(f"Database {name} created successfully!")
    
    except (KeyError, ValueError) as e:
        shutil.rmtree(path)
        traceback.print_exc()
        raise RuntimeError(f"Database creation aborted: {str(e)}")
    except Exception as e:
        shutil.rmtree(path)
        raise Exception(f"An unexpected error occurred: {str(e)}")