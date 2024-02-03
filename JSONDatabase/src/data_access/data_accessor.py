from collections import defaultdict
import os
import json
from src.utils import utils
import traceback
import heapq
from itertools import islice
import pprint
import glob
import uuid

DATABASE_DIR = './databases/'
DELETE_MARKER = -1

def get_pk(conditions, primary_key):
    primary_key_condition = next((condition[primary_key] for condition in conditions if primary_key in condition), None)
    if not primary_key_condition:
        #need to implement a search for record later when PK is not given
        raise ValueError(f"Conditions must contain primary key: {primary_key}")
    if primary_key_condition['operator'] != '=':
        raise ValueError(f"Primary key condition must use '=' operator")
    else:
        primary_key_value = primary_key_condition['value']
        return primary_key_value

def insert_data(data_to_insert, active_database):
    db_path = os.path.join(DATABASE_DIR, active_database)
    primary_key = utils.get_primary_key(db_path)
    primary_key_value = data_to_insert.get(primary_key)

    if not primary_key_value:
        raise ValueError(f"Data to insert must contain primary key: {primary_key}")
    
    hash_index = utils.get_hash_index(db_path)
    if str(primary_key_value) in hash_index and hash_index[str(primary_key_value)] != DELETE_MARKER:
        # Handle duplicate primary key scenario
        raise ValueError(f"Duplicate primary key: {primary_key_value}")
    
    offset = utils.append_to_ndjson(db_path, data_to_insert)
    res = utils.add_hash_index(db_path, primary_key_value, offset)
    if res == True:
        print('Successfully inserted the data')
    else:
        print('Failed to insert the data')
    
def update_data(data, conditions, active_database):
    db_path = os.path.join(DATABASE_DIR, active_database)
    hash_index = utils.get_hash_index(db_path)
    primary_key = utils.get_primary_key(db_path)
    
    primary_key_value = get_pk(conditions, primary_key)
    if str(primary_key_value) not in hash_index or hash_index[str(primary_key_value)] == DELETE_MARKER:
        raise ValueError(f"Data with primary key: {primary_key_value} does not exist.")
    
    data[primary_key] = primary_key_value
    # Retrieve existing data
    offset = hash_index[str(primary_key_value)]
    
    with open(os.path.join(db_path, 'data.ndjson'), 'r') as f:
        f.seek(offset)
        existing_data = json.loads(f.readline().strip())

    # Update existing data with new data
    existing_data.update(data)
    
    #delete the data from datastore
    utils.delete_from_ndjson(db_path, offset)

    #delete old data of this record and append the updated data
    utils.remove_hash_index(db_path, primary_key_value)
    new_offset = utils.append_to_ndjson(db_path, existing_data)
    response = utils.add_hash_index(db_path, primary_key_value, new_offset)
    if response:
        print('Successfully updated the data')
    else:
        print('Failed to update the data')

def delete_data(condition, active_database):
    db_path = os.path.join(DATABASE_DIR, active_database)
    primary_key = utils.get_primary_key(db_path)
    primary_key_value = get_pk(condition, primary_key)
    
    if not primary_key_value:
        raise ValueError(f"Data to delete must contain primary key: {primary_key}")
    
    hash_index = utils.get_hash_index(db_path)
    index_val = hash_index[str(primary_key_value)]
    if str(primary_key_value) in hash_index and index_val != DELETE_MARKER:
        response = utils.remove_hash_index(db_path, primary_key_value)
        if response:
            utils.delete_from_ndjson(db_path, index_val)
            return 'Successfully deleted the data'
    elif str(primary_key_value) in hash_index and hash_index[str(primary_key_value)] == DELETE_MARKER:
        return 'Data is already deleted'
    elif str(primary_key_value) not in hash_index:
        return 'Data record does not exist'
    else:
        return 'Failed to delete the data'
    
def delete_records(conditions, active_database, chunk_size=1024):
    db_path = os.path.join(DATABASE_DIR, active_database)
    primary_key = utils.get_primary_key(db_path)
    hash_index = utils.get_hash_index(db_path)
    and_flag = False
    if conditions!= None and len(conditions) != 0:
        if conditions[-1]['and'] == True:
            and_flag = True
        conditions.pop()
    records_to_delete = []
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r') as db_file:
            for chunk in read_in_chunks(db_file, chunk_size):
                partial_records = process_chunk(chunk, conditions, primary_key, hash_index, and_flag, ['all'])
                for record in partial_records:
                    primary_key_value = record[primary_key]
                    if str(primary_key_value) in hash_index and hash_index[str(primary_key_value)] != DELETE_MARKER:
                        records_to_delete.append(primary_key_value)

        # # Perform deletion
        with open(os.path.join(db_path, 'data.ndjson'), 'r+') as db_file:
            for primary_key_value in records_to_delete:
                index_val = hash_index[str(primary_key_value)]
                utils.delete_from_ndjson(db_path, index_val)
                utils.remove_hash_index(db_path, primary_key_value)
        
        print(f"Suceessfully deleted {len(records_to_delete)} records.")
    
    except FileNotFoundError as e:
        print(f"An error occurred: {e}") 
        traceback.print_exc()
    except Exception as e:
        print(f"An error occurred: {e}") 
        traceback.print_exc()

def check_condition(record, condition):
    """
    Check if a record satisfies a single condition
    """
    for key, value in condition.items():
        # Check if the record's value for the key satisfies the condition
        operator = value['operator']
        expected_value = value['value']
        actual_value = record.get(key)
        if actual_value is None:
            return False
        if operator == '=' and actual_value != expected_value:
            return False
        elif operator == '!=' and actual_value == expected_value:
            return False
        elif operator == '>' and actual_value <= expected_value:
            return False
        elif operator == '>=' and actual_value < expected_value:
            return False
        elif operator == '<' and actual_value >= expected_value:
            return False
        elif operator == '<=' and actual_value > expected_value:
            return False      
    
    return True

'''
    Find records given the condition - will be used in select query and update and delete conditions

'''

def read_in_chunks(file_object, chunk_size):
    """ Generator function to read a file line by line and accumulate lines in chunks, 
    skipping lines marked for deletion. """
    current_chunk = ''
    for line in file_object:
        try:
            # Check if line is marked for deletion
            record = json.loads(line)
            if record.get('delete', False):
                continue  # Skip this line
        except json.JSONDecodeError:
            print("JSON parsing error in line: ", line)
            continue  # Skip line if there's a JSON parsing error

        # Check if adding this line exceeds chunk size
        if len(current_chunk) + len(line) > chunk_size:
            if current_chunk:  # Yield non-empty current chunk
                yield current_chunk
                current_chunk = line
            else:  # If the line itself is larger than chunk size, yield the line
                yield line
        else:
            current_chunk += line

    # Yield any remaining text in the current chunk
    if current_chunk:
        yield current_chunk

def output_result(filename, chunk_size = 4*1024):
    print('\n','------------ Result ------------')
    records_count = 0
    try:
        with open(filename, 'r') as f:
            for chunk in read_in_chunks(f, chunk_size):
                print('\n')
                print('Current Chunk length: ',len(chunk))
                for line in chunk.splitlines():
                    json_line = json.loads(line)
                    pprint.pprint(json_line, indent=4)
                    records_count += 1
        if records_count == 0:
            print("No records found for the given query")
        else:
            print('\n')
            print(f"Total records : {records_count}")
        print('\n')
        os.remove(filename)
    except FileNotFoundError:
        print("Database file not found.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
              
def process_chunk(chunk, conditions, primary_key, hash_index, and_flag, project):
    records = []
    if conditions != []:
        print('Procesing Chunk: of length: ',len(chunk))
        print(chunk)
    for line in chunk.splitlines():
        try:
            record = json.loads(line)
            if record.get('delete',False):
                continue
            else:
                if hash_index.get(str(record[primary_key])) != DELETE_MARKER and record.get(primary_key):
                    if conditions == []:
                        if project == ['all']:
                            records.append(record)
                        else:
                            filtered_record = {k: v for k, v in record.items() if k in project}
                            records.append(filtered_record)
                    
                    elif (and_flag and all(check_condition(record, condition) for condition in conditions)) or \
                    (not and_flag and any(check_condition(record, condition) for condition in conditions)):
                        if project == ['all']:
                            records.append(record)
                        else:
                            filtered_record = {k: v for k, v in record.items() if k in project}
                            records.append(filtered_record)

        except json.JSONDecodeError:
            print("JSON parsing error in line: ", line)
            traceback.print_exc()
    return records

def find_records(conditions, project, sort, active_database, chunk_size = 4 * 1024):
    db_path = os.path.join(DATABASE_DIR, active_database)
    primary_key = utils.get_primary_key(db_path)
    hash_index = utils.get_hash_index(db_path)
    records = []
    and_flag = False
    chunk_size = int(input("Enter chunk size in bytes: ") or chunk_size)
    if conditions!= None and len(conditions) != 0:
        if conditions[-1]['and'] == True:
            and_flag = True
        conditions.pop()
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r') as f:
            for chunk in read_in_chunks(f, chunk_size):
                partial_records = process_chunk(chunk, conditions, primary_key, hash_index, and_flag, project)
                records.extend(partial_records)
    except FileNotFoundError:
        return "Database file not found."
    except Exception as e:
        return f"An error occurred: {e}"
    
    if sort['sort_flag'] == True:
        sort_dir_path = os.path.join(db_path, 'sorted_temp_files')
        os.makedirs(sort_dir_path, exist_ok=True)
        temp_file_path = db_path  + '/temp_file.ndjson'
        with open(temp_file_path, 'w') as temp_file:
            for record in records:
                temp_file.write(json.dumps(record) + '\n')
        
        sort_key = sort['sort_key']
        order = sort['order']

        sorted_files = sort_data(temp_file_path, sort_key, order, chunk_size,sort_dir_path)
        final_file = k_way_merge(sorted_files, sort['sort_key'], sort['order'], chunk_size, db_path)
        os.remove(temp_file_path)
        output_result(final_file,chunk_size)
    else:
        final_file = db_path + '/find_unsorted.ndjson'
        with open(final_file, 'w') as temp_file:
            for record in records:
                temp_file.write(json.dumps(record) + '\n')
        output_result(final_file,chunk_size)
        
def read_records_from_file(file_path):
    with open(file_path, 'r') as file:
        return [json.loads(line) for line in file]


''' GROUP BY METHODS '''

def aggregate_data(temp_dir_path, agg_functions, chunk_size):
    if len(agg_functions) != 1:
        raise ValueError("Only one aggregation function should be provided.")

    agg_function, field = agg_functions[0]['function'], agg_functions[0]['field']
    
    # Validating the aggregation function
    valid_functions = ['max', 'min', 'sum', 'mean']
    if agg_function not in valid_functions and not (agg_function == 'count' and field == '*'):
        raise ValueError("Invalid aggregation function.")

    dict_res = {}  # Dictionary to store interim results
    base_file_name = 'aggregation_result'
    unique_id = uuid.uuid4().hex
    result_file_path = os.path.join(temp_dir_path, f"{base_file_name}_{unique_id}.ndjson")


    def process_chunk(file, group_key):
        nonlocal records_sum, records_count, records_max, records_min
        total_size = 0
        while total_size < chunk_size:
            line = file.readline()
            if not line:
                break  # End of file
            # Decode bytes to string
            line = line.decode('utf-8')
            total_size += len(line)
            if total_size > chunk_size and total_size != len(line):
                file.seek(-len(line.encode('utf-8')), os.SEEK_CUR)
                break

            try:
                record = json.loads(line)
                if field in record and isinstance(record[field], (int, float)):
                    value = record[field]
                    records_sum += value
                    records_count += 1
                    records_max = max(records_max, value) if records_max is not None else value
                    records_min = min(records_min, value) if records_min is not None else value
            except json.JSONDecodeError:
                print(f"JSON parsing error in line: {line}")
                traceback.print_exc()

    if agg_function == 'count' and field == '*':
        for file_path in glob.glob(os.path.join(temp_dir_path, '*.ndjson')):
            if 'aggregation_result_' in os.path.basename(file_path):
                continue
            group_key = os.path.basename(file_path).replace('.ndjson', '')
            with open(file_path, 'r') as file:
                count = sum(1 for _ in file)
                dict_res[group_key] = {'count': count}
    else:
        for file_path in glob.glob(os.path.join(temp_dir_path, '*.ndjson')):
            if 'aggregation_result_' in os.path.basename(file_path):
                continue
            group_key = os.path.basename(file_path).replace('.ndjson', '')
            records_sum = 0
            records_count = 0
            records_max = None
            records_min = None

            with open(file_path, 'rb') as file:
                process_chunk(file, group_key)

            # Storing results based on the aggregation function
            if agg_function == 'mean':
                key = 'mean of ' + field
                dict_res[group_key] = {key: records_sum / records_count if records_count > 0 else 0}
            elif agg_function == 'max':
                key = 'max of ' + field
                dict_res[group_key] = {key: records_max}
            elif agg_function == 'min':
                key = 'min of ' + field
                dict_res[group_key] = {key: records_min}
            elif agg_function == 'sum':
                key= 'sum of ' + field
                dict_res[group_key] = {key: records_sum}
            else:
                raise ValueError("Invalid aggregation function.")

    with open(result_file_path, 'w') as result_file:
        json.dump(dict_res, result_file)

    return result_file_path

def group_data_by_field_and_write(records, group_by_field, temp_files_dir):
    if isinstance(records, str):
        records = [json.loads(line) for line in records.splitlines() if line.strip()]

    for record in records:
        key = record.get(group_by_field)
        if key:  # Ensure the key exists
            record['group_key'] = key  # Adding 'group_key' attribute
            temp_file_path = os.path.join(temp_files_dir, f'{key}.ndjson')
            try:
                with open(temp_file_path, 'a') as file:
                    file.write(json.dumps(record) + '\n')
            except IOError as e:
                print(f"Error writing to file {temp_file_path}: {e}")

def clean_up_temp_directory(temp_dir):
    if os.path.exists(temp_dir) and os.path.isdir(temp_dir):
        # Remove each file in the directory
        for filename in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

        # Remove the directory itself
        os.rmdir(temp_dir)

def groupby_records(conditions, aggregate, group_by_field, active_database, chunk_size=4 * 1024):
    aggregated_result = {}
    db_path = os.path.join(DATABASE_DIR, active_database)
    primary_key = utils.get_primary_key(db_path)
    hash_index = utils.get_hash_index(db_path)
    and_flag = False
    if conditions!= None and len(conditions) != 0:
        if conditions[-1]['and'] == True:
            and_flag = True
        conditions.pop()

    chunk_size = int(input("Enter chunk size in bytes: ") or chunk_size)
    temp_dir = os.path.join(db_path, "temp_group_key_files")
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    try:
        with open(os.path.join(db_path, 'data.ndjson'), 'r') as f:
            for chunk in read_in_chunks(f, chunk_size):
                if conditions:
                    data = process_chunk(chunk, conditions, primary_key, hash_index, and_flag, ['all'])
                else:
                    print('\n')
                    print('Current Chunk length: ',len(chunk))
                    print('Processing Chunk: ')
                    for line in chunk.splitlines():
                        try:
                            json_line = json.loads(line)
                            print(json_line)
                        except json.JSONDecodeError:
                            continue
                    data = chunk
                group_data_by_field_and_write(data, group_by_field, temp_dir)

        res_files = [aggregate_data(temp_dir, [aggregate[i]], chunk_size) for i in range(len(aggregate))]
        for res_file in res_files:
            try:
                with open(res_file, 'r') as file:
                    file_data = json.load(file)
                    for key, value in file_data.items():
                        if key in aggregated_result:
                            aggregated_result[key].update(value)
                        else:
                            aggregated_result[key] = value
            except IOError as e:
                print(f"Error reading file {res_file}: {e}")
                traceback.print_exc()
    
    except Exception as e:
        traceback.print_exc()
        print(f"An error occurred in grouping and aggregating: {e}")
    finally:
        clean_up_temp_directory(temp_dir)
    
    final_file = os.path.join(db_path, 'groupby_result.ndjson')
    with open(final_file, 'w') as file:
        json.dump(aggregated_result, file)
    output_result(final_file,chunk_size)

''' SORT METHODS '''

def sort_data(temp_file_path, sort_key, order, chunk_size, output_dir):
    sorted_files = []
    with open(temp_file_path, 'r') as file:
        current_chunk = ''
        for line in file:
            current_chunk += line
            if len(current_chunk.encode('utf-8')) >= chunk_size:
                sorted_chunk_file = sort_and_write_chunk(current_chunk, sort_key, order, output_dir)
                sorted_files.append(sorted_chunk_file)
                current_chunk = ''
        if current_chunk:
            sorted_chunk_file = sort_and_write_chunk(current_chunk, sort_key, order, output_dir)
            sorted_files.append(sorted_chunk_file)
    return sorted_files

def sort_and_write_chunk(chunk_data, sort_key, order, output_dir):
    chunk_records = [json.loads(line) for line in chunk_data.strip().splitlines()]
    chunk_records.sort(key=lambda x: x[sort_key], reverse=(order == 'desc'))

    temp_file_path = os.path.join(output_dir, f"temp_{os.urandom(4).hex()}.json")
    with open(temp_file_path, 'w') as temp_file:
        for record in chunk_records:
            temp_file.write(json.dumps(record) + '\n')
    return temp_file_path

def k_way_merge(sorted_files, sort_key, order, chunk_size, db_path):
    final_sorted_file = os.path.join(db_path, 'final_sorted_output.json')
    output_handle = open(final_sorted_file, 'w')

    sample_record_size = estimate_average_record_size(sorted_files[0])
    records_per_buffer = max(1, chunk_size // (sample_record_size * len(sorted_files)))

    min_heap = []
    buffers = {}
    file_handles = {}

    for file_index, file_name in enumerate(sorted_files):
        file_handle = open(file_name, 'r')
        file_handles[file_index] = file_handle
        buffer = [json.loads(next(file_handle, '{}')) for _ in range(records_per_buffer)]
        buffers[file_index] = buffer
        if buffer and sort_key in buffer[0]:
            try:
                adjusted_key = adjust_key_for_order(buffer[0][sort_key], order)
                heapq.heappush(min_heap, (adjusted_key, file_index))
            except KeyError as e:
                print(f"KeyError occurred. Record: {buffer[0]}, Error: {e}")
        elif buffer:
            print(f"Record without sort key: {buffer[0]}")

    while min_heap:
        _, file_index = heapq.heappop(min_heap)

        # Check if buffer is not empty before popping
        if buffers[file_index]:
            record = buffers[file_index].pop(0)
            output_handle.write(json.dumps(record) + '\n')

        # Refill buffer if empty
        if not buffers[file_index]:
            buffer = []
            while len(buffer) < records_per_buffer:
                line = next(file_handles[file_index], None)
                if line is None:  # End of file
                    break
                buffer.append(json.loads(line))
            buffers[file_index] = buffer

        # Push next record from buffer to heap
        if buffers[file_index]:
            next_item = buffers[file_index][0]
            # Check if sort key exists in next_item
            if sort_key in next_item:
                adjusted_key = adjust_key_for_order(next_item[sort_key], order)
                heapq.heappush(min_heap, (adjusted_key, file_index))
            else:
                print(f"Missing sort key '{sort_key}' in record during merge: {next_item}")

    output_handle.close()

    # Close all open file handles
    for file_handle in file_handles.values():
        file_handle.close()

    # Delete sorted chunk files
    for file_name in sorted_files:
        if os.path.exists(file_name):
            os.remove(file_name)

    # Check if the directory where sorted chunk files were stored is empty,
    # and if so, remove it
    sorted_chunk_dir = os.path.dirname(sorted_files[0])
    if os.path.exists(sorted_chunk_dir) and not os.listdir(sorted_chunk_dir):
        os.rmdir(sorted_chunk_dir)

    return final_sorted_file # return the path of the sorted output file

def adjust_key_for_order(key, order):
    if isinstance(key, (int, float)):  # For numeric types
        return -key if order == 'desc' else key
    else:  # For strings and other types
        return key
    
def estimate_average_record_size(file_name):
    with open(file_name, 'r') as file:
        first_record = json.loads(next(file, '{}'))
    return len(json.dumps(first_record))

'''One to Many : JOIN METHODS'''

def create_index(file_path, join_key):
    index = defaultdict(list)
    with open(file_path, 'r') as file:
        while True:
            offset = file.tell()
            line = file.readline()
            if not line:
                break
            try:
                record = json.loads(line)
                key_value = record.get(join_key)
                if key_value is not None:
                    index[key_value].append(offset)
            except json.JSONDecodeError:
                print(f"JSON decoding error at offset: {offset}")
    return index

def join_records(dbs, join_conditions, db_project):
    try:
        db_first, db_second = dbs
        db_first_path = os.path.join(DATABASE_DIR, db_first, 'data.ndjson')
        db_second_path = os.path.join(DATABASE_DIR, db_second, 'data.ndjson')
        db_first_condition = join_conditions[0].split('.')[1]
        db_second_condition = join_conditions[1].split('.')[1]

        project = {'db_first': [], 'db_second': []}
        db_names = {'db_first': db_first, 'db_second': db_second}
        
        for item in db_project:
            db_name, sub_key = item.split('.')
            db_key = 'db_first' if db_name == db_first else 'db_second'
            project[db_key].append(sub_key)
        
        chunk_size = int(input("Enter chunk size in bytes: ") or (4 * 1024))
        temp_output_path = os.path.join(DATABASE_DIR, 'temp_join_output.ndjson')
        second_db_index = create_index(db_second_path, db_second_condition)

        with open(db_first_path, 'r') as first_db_file, open(temp_output_path, 'w') as temp_output:
            for chunk in read_in_chunks(first_db_file, chunk_size):
                for line in chunk.splitlines():
                    try:
                        first_db_record = json.loads(line)
                        join_value = first_db_record.get(db_first_condition)
                        if join_value in second_db_index:
                            # print("First DB Record: ", first_db_record)
                            # print("Join value: ", join_value)
                            for second_chunk in buffered_read(db_second_path, second_db_index[join_value], chunk_size):
                                # print(second_chunk)
                                for second_db_record in second_chunk:
                                    # print("Second DB Record: ", second_db_record)
                                    joined_record = combine_records(first_db_record, second_db_record, project, db_names)
                                    temp_output.write(json.dumps(joined_record) + '\n')
                    except json.JSONDecodeError:
                        print("JSON decoding error in line: ", line)
                    except Exception as e:
                        print("An error occurred while processing join: ", e)
                        traceback.print_exc()
    except Exception as e:
        print("An error occurred in join_records function: ", e)
        traceback.print_exc()
    #  output_result(temp_output_path, chunk_size)

def buffered_read(file_path, offsets, buffer_size):
    # print(file_path, offsets, buffer_size)
    try:
        with open(file_path, 'r') as file:
            buffer = []
            for offset in offsets:
                file.seek(offset)
                line = file.readline()
                try:
                    buffer.append(json.loads(line))
                    if len(buffer) >= buffer_size:
                        yield buffer
                        buffer = []
                except json.JSONDecodeError:
                    print("JSON decoding error at offset: ", offset)
            if buffer:
                yield buffer
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"An error occurred in buffered_read function: {e}")
        traceback.print_exc()

def combine_records(first_record, second_record, project, db_names):
    try:
        combined_record = {}
        for db_key, fields in project.items():
            record = first_record if db_key == 'db_first' else second_record
            prefix = db_names[db_key] + "_"
            for field in fields:
                combined_field_name = prefix + field
                combined_record[combined_field_name] = record.get(field, None)
        return combined_record
    except Exception as e:
        print("An error occurred in combine_records function: ", e)
        traceback.print_exc()


# -------- INSERT --------------------[Add]
# Add new [ENTITY] with [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]

# TEST : Find all records with Name=Pandharpur
# Add new city with Name=Pandharpur CountryCode=IND District=Pune Population=200000
# TEST : Find all records with Name=Pandharpur

# Add new city with Name='Navi Mumbai' CountryCode=IND District=Maharashtra Population=5000000
# TEST : Find all records with Name='Navi Mumbai'

# -------- UPDATE -------------------- [Modify]
# Modify [ENTITY/record] with [ATTRIBUTE]=[VALUE] set [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
# ONLY 'WITH' no usage of 'WHERE'

# TEST : Find all records with Name=Mangalore
# Modify city with Name=Mangalore set Population=1000000, District=DK
# TEST : Find all records with Name=Mangalore

# TEST : Find all records with Name=Pandharpur
# Modify city with Name=Pandharpur set District=Satara
# TEST : Find all records with Name=Pandharpur

# TEST : Find all records with Name=Jaipur
# Modify city with Name=Jaipur set Population=12341234
# TEST : Find all records with Name=Jaipur

# -------- DELETE -------------------- 
# Delete [ENTITY/record] with [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
# ONLY 'WITH' no usage of 'WHERE'
# No AND

# TEST : Find all records with Name=Pandharpur
# Delete city with Name=Pandharpur
# TEST : Find all records with Name=Pandharpur

# TEST : Find all records with Population>1000000 and Population<2000000
# Delete city with Population>1000000, Population<2000000
# TEST : Find all records with Population>1000000 and Population<2000000

# -------- FIND ----------------------
# Find all records with CountryCode=IND
# Find all records with CountryCode=IND and District=Karnataka

# Find Name, IndepYear, Continent, Region, Population, GNP of records where GNP>10000 and Continent!=Europe
# Find Name, IndepYear, Continent, Region, Population, GNP of records where GNP>100000 and Continent!=Europe and Population<10000000


# --------- GROUP BY AGGREGATIONS --------------------
# Find average(GNP) of records categorize_by Continent
# Find maximum(GNP) of records categorize_by Continent
# Find minimum(GNP) of records categorize_by Continent
# Find total_sum(GNP) of records categorize_by Continent
# Find count(*), total_sum(GNP) of records categorize_by Continent


# --------- SORT --------------------
# Find Name, GNP in records with Continent=Asia sort_by GNP desc

# --------- JOIN --------------------
# Join country and city where country.country_code=city.CountryCode display country.Name, city.Name, city.Population country.Continent, country.Population
