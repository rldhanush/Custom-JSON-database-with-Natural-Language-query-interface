# src/data_storage/data_manager.py

import os
import json
import shutil

DATABASE_DIR = './databases/'

def create_database(name = None):
    if name is None:
        raise ValueError("A database name must be provided.")
    
    path = os.path.join(DATABASE_DIR, name)
    os.makedirs(path, exist_ok=True)
    
    while True:
        data_path = input("Enter path to dataset: ")
        if os.path.exists(data_path):
            break
        else:
            print(f"File not found: {data_path}. Please try again.")
    
    partition_key = input("Enter the keys to partition data, in the nested order, separated by commas: ").split(',')
    
    try:
        with open(data_path, "r") as file:
            data = json.load(file)
        
        # Note: Importing here to avoid circular imports
        from src.data_storage.data_partition import partition_data
        
        partitioned_data = partition_data(data, partition_key)
        save_partitioned_data(partitioned_data, path)
        print(f"Database {name} created successfully!")

    except (KeyError, ValueError) as e:
        raise RuntimeError(f"Database creation aborted: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")

def save_partitioned_data(partitioned_data, path):
    """
    Saves partitioned data into separate JSON files.

    :param partitioned_data: Data partitioned by partition key.
    :param path: Path to save partitioned data.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Path {path} does not exist.")
    
    try:
        for key, items in partitioned_data.items():
            with open(f"{path}/{key}.json", "w") as file:
                json.dump(items, file)
    except Exception as e:
        raise IOError(f"An error occurred while saving data: {str(e)}")


def list_databases():
    try:
        return os.listdir(DATABASE_DIR)
    except FileNotFoundError:
        print(f"No databases found. Ensure the {DATABASE_DIR} directory exists.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")


def delete_database(name):
    """
    Delete a database directory and all its contents.

    :param name: The name of the database/directory to delete.
    """
    path = os.path.join(DATABASE_DIR, name)

    if not os.path.exists(path):
        raise FileNotFoundError(f"No database named '{name}' found.")

    try:
        shutil.rmtree(path)
        print(f"Database '{name}' deleted successfully!")
    except OSError as e:
        raise OSError(f"Error deleting database: {str(e)}")