# src/data_storage/data_partition.py
import json

def get_nested_value(data, key_path):
    """
    Retrieves the nested value by navigating through the key path.

    :param data: Dictionary to retrieve value from.
    :param key_path: List of keys representing the path to the value.
    :return: Nested value or None if not found.
    """
    try:
        for key in key_path:
            data = data[key]
        return data
    except (TypeError, KeyError):
        return None

def validate_key_path_exists(data_sample, key_path):
    """
    Validates if the provided key path exists in the data sample.

    :param data_sample: Sample data to validate key path.
    :param key_path: List of keys representing the path to validate.
    :return: True if key path exists, False otherwise.
    """
    return get_nested_value(data_sample, key_path) is not None

def partition_data(data, partition_key):
    """
    Partitions data based on the given key.

    :param data: Original data.
    :param partition_key: Key on which to partition data.
    :return: Dictionary with partitioned data.
    """
    partitioned_data = {}
    try:
        # Ensure the provided key path exists in data
        if not validate_key_path_exists(data[partition_key[0]][0], partition_key[1:]):
            raise KeyError(f"Invalid key path: {' -> '.join(partition_key)}")
            
        
        # Iterate over data and partition it
        for item in data[partition_key[0]]:
            key_value = get_nested_value(item, partition_key[1:])
            if key_value is not None:
                key_value_str = str(key_value)
                if key_value_str not in partitioned_data:
                    partitioned_data[key_value_str] = []
                partitioned_data[key_value_str].append(item)
                
    except Exception as e:
       raise ValueError(f"Error during data partitioning: {str(e)}")
    
    return partitioned_data