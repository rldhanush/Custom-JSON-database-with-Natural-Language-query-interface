import nltk
import re
import json,ast

operators = ['<=', '>=', '==', '!=', '=', '<', '>']

def get_type(value):
    try:
        value = int(value)
        return value
    except ValueError:
        pass

    try:
        value = float(value)
        return value
    except ValueError:
        pass

    try:
        value = ast.literal_eval(value)
        return value
    except ValueError: 
        pass

    return value

def split_outside_quotes(s):
    return re.split(r' (?=(?:[^\'"]|\'[^\']*\'|"[^"]*")*$)', s)

def parse_conditions(query, start_index):
    conditions = []
    for i in range(start_index, len(query)):
        condition = {}
        op = next((operator for operator in operators if operator in query[i]), None)
        if op:
            key, val = query[i].split(op)
            condition[key.strip()] = {"operator": op, "value": get_type(val.strip())}
        conditions.append(condition)
    return conditions

def parse_aggregate_functions(query):
    agg_functions = []
    agg_keyword_map = {
        'maximum': 'max',
        'minimum': 'min',
        'average': 'mean',
        'total_sum': 'sum',
        'count(*)': 'count'
    }
    for item in query:
        for agg_keyword, py_func in agg_keyword_map.items():
            if item.startswith(agg_keyword):
                agg_func, agg_field = item.split('(')
                agg_field = agg_field.strip(')')
                agg_functions.append({'function': py_func, 'field': agg_field})
                break
    return agg_functions

def parse_join(query):
    action = 'join'
    dbs = [query[1],query[3]]
    join_conditions = query[query.index('where') + 1].split('=')
    project = []
    for i in range(query.index('display') + 1,len(query)):
        project.append(query[i])
    
    return {'action': action, 'dbs': dbs, 'join_conditions': join_conditions, 'project': project}

def parse_query(query):
    pattern = re.compile(r',(?![^\[\{]*[\}\]])')
    query = pattern.sub('',query)
    query = split_outside_quotes(query)
    action = query[0].lower()

    if action in ['join','merge','combine','lookup']:
        return parse_join(query)
    
    query_part, categorize_part, sort_and_limit_part = split_query(query)    
    
    if action == 'add':
        #Add new [ENTITY] with [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
        data = {}
        ind_with = query.index('with') + 1
        for i in range(ind_with,len(query)):
            var = query[i].split('=')
            key = var[0]
            val = get_type(var[1])
            data[key] = val
        return {'action': action, 'data': data}
        
    elif action == 'modify':
        #Modify [ENTITY/record] with [ATTRIBUTE]=[VALUE] set [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE], [ATTRIBUTE]=[VALUE]
        #condition is the attribute=value ; ONLY OPERATOR allower are as follows: <,>,<=,>=,==,!=
        ind_condition = query.index('with') + 1
        ind_data = query.index('set') + 1
        data = {}
        for i in range(ind_data,len(query)):
            var = query[i].split('=')
            key = var[0]
            val = get_type(var[1])
            data[key] = val
    
        conditions = []
        for i in range(ind_condition,ind_data-1):
            condition = {}
            op = next((operator for operator in operators if operator in query[i]), None)
            if op:
                key,val = query[i].split(op)
                condition[key.strip()] = {"operator": op, "value": get_type(val.strip())}
            conditions.append(condition)

        return {'action': action, 'data': data, 'conditions': conditions}
    
    elif action == 'delete':
        ind_cond = query.index('with') + 1 if 'with' in query else query.index('where') + 1 if 'where' in query else 0
        
        conditions = []
        if ind_cond != 0:
            and_flag = False
            if 'and' in query:
                and_flag = True
                query.remove('and')
            conditions = parse_conditions(query, ind_cond)
            conditions.append({'and': and_flag})
            
        return {'action': action, 'conditions': conditions}

    elif action in ['find','show','get','display','select']:
        result = None
        if 'categorize_by' in query:
            result =  parse_aggregate_find(query_part)
            if len(categorize_part) == 1:
                return ('Invalid Query - Give proper group by command')
                
            elif len(categorize_part) > 1:
                result['groupby'] = categorize_part[1]
            if len(result['aggregate']) == 0:
                return ('Invalid Query - Group by can be used only with aggregate functions')
        else:
            result = parse_regular_find(action,query_part)
            sort_flag = False
            sort_key = None
            order = None
            if len(sort_and_limit_part) == 3:
                sort_flag = True
                sort_key = sort_and_limit_part[1]
                order = sort_and_limit_part[2]
            elif len(sort_and_limit_part) > 0 and len(sort_and_limit_part) < 3:
                return ('Invalid Query - Give proper sort by command')
                
            result['sort'] = {'sort_flag': sort_flag, 'sort_key': sort_key, 'order': order}
            
        return result

def parse_aggregate_find(query):
    action = 'groupby'
    agg_functions = []
    agg_functions = parse_aggregate_functions(query)

    ind_cond = None
    if 'where' in query:
            ind_cond = query.index('where') + 1
    elif 'with' in query:
            ind_cond = query.index('with') + 1
    
    conditions = []
    if ind_cond != None:
        and_flag = False
        if 'and' in query:
            and_flag = True
            query.remove('and')
        conditions = parse_conditions(query, ind_cond)
        conditions.append({'and': and_flag})

    return {'action': action, 'conditions': conditions,'aggregate': agg_functions}

def split_query(query):
    # Define the keywords to split the list
    keywords = ['categorize_by', 'sort_by', 'limit']

    # Initialize empty lists for the splits
    query_part = []
    categorize_part = []
    sort_and_limit_part = []

    # Temporary list to hold the current segment
    current_part = query_part

    for item in query:
        if item in keywords:
            # If we encounter a keyword, we switch to the corresponding list
            if item == 'categorize_by':
                current_part = categorize_part
            elif item == 'sort_by' or item == 'limit':
                current_part = sort_and_limit_part
        current_part.append(item)

    return query_part, categorize_part, sort_and_limit_part

def parse_regular_find(action,query):
    if 'where' in query:
            ind_cond = query.index('where') + 1
    elif 'with' in query:
            ind_cond = query.index('with') + 1
    else:
        ind_cond = 0
    
    proj = []
    if query[1] == 'all':
        proj.append('all')
    else:
        if 'of' in query:
            ind_of = query.index('of')
        elif 'in' in query:
            ind_of = query.index('in')
        else:
            print('Invalid Query - Give proper select command')
            return
        for i in range(1,ind_of):
            proj.append(query[i])
    
    conditions = []
    if ind_cond != 0:
        and_flag = False
        if 'and' in query:
            and_flag = True
            query.remove('and')
        conditions = parse_conditions(query, ind_cond)
        conditions.append({'and': and_flag})
    return {'action': action, 'conditions': conditions,'project': proj}


# print(parse_query('display count(*) of records where artsits=Adele categorize_by artists'))
