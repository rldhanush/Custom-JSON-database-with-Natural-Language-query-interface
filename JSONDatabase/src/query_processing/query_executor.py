from src.data_access import data_accessor

def execute_query(parsed_query, active_database):
    operation = parsed_query.get("action")
    if operation == "add":
        data = parsed_query.get("data")
        data_accessor.insert_data(data, active_database)

    elif operation == "modify":
        data = parsed_query.get("data")
        conditions = parsed_query.get("conditions")
        data_accessor.update_data(data,conditions,active_database)
        
    elif operation == "delete":
        conditions = parsed_query.get("conditions")
        data_accessor.delete_records(conditions,active_database)
    
    elif operation == "find" or operation == "show" or operation == "select":
        conditions = parsed_query.get("conditions")
        project = parsed_query.get("project")
        sort = parsed_query.get("sort")
        data_accessor.find_records(conditions,project,sort,active_database)
    
    elif operation == "groupby":
        conditions = parsed_query.get("conditions")
        aggregate =  parsed_query.get("aggregate")
        groupby = parsed_query.get("groupby")
        data_accessor.groupby_records(conditions,aggregate,groupby,active_database)
    
    elif operation == "join":
        dbs = parsed_query.get("dbs")
        join_conditions = parsed_query.get("join_conditions")
        project = parsed_query.get("project")
        data_accessor.join_records(dbs,join_conditions,project)
        
    else:
        print(Exception("Unsupported operation. Please provide a valid operation (INSERT, UPDATE, DELETE, SELECT)."))
    