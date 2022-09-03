from pprint import pprint
import pymongo
import re


TRIM = lambda x : x.strip()

def create_db(mydb):

    mycol = mydb["customers"]

    mylist = [
    #   { "_id": 100, "name": "John", "address": "Highway 37", 'num': 5},
    #   { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
    #   { "_id": 3, "name": "Amy", "address": "Apple st 652"},
    #   { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
    #   { "_id": 5, "name": "Michael", "address": "Valley 345"},
    #   { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
      
      { "_id": 2000, "name": "Peter", "address": "Lowstreet 27", 'num': 36},
      { "_id": 3000, "name": "Amy", "address": "Apple st 652", 'num': 18},
      { "_id": 4000, "name": "Hannah", "address": "Mountain 21", 'num': 69},
      { "_id": 5000, "name": "Michael", "address": "Valley 345", 'num': 90},
      { "_id": 6000, "name": "Sandy", "address": "Ocean blvd 2", 'num': 35},

      { "_id": 700, "name": "Betty", "address": "Green Grass 1", 'num': 100},
      { "_id": 800, "name": "Richard", "address": "Sky st 331", 'num': 500},
      { "_id": 900, "name": "Susan", "address": "One way 98", 'num': 700},
      { "_id": 10000, "name": "Vicky", "address": "Yellow Garden 2", 'num': 60},
      { "_id": 1100, "name": "Ben", "address": "Park Lane 38", 'num': 20},
      { "_id": 1200, "name": "William", "address": "Central st 954", 'num': 1},
      { "_id": 1300, "name": "Chuck", "address": "Main Road 989", 'num': 9},
      { "_id": 1400, "name": "Viola", "address": "Sideway 1633", 'num': 50}
    ]
    print([mycol.insert_one(doc).inserted_id for doc in mylist])
    return mycol


def extract_nested_conditinals(seq, operator):
    if seq.startswith('('):
        seq = seq[1:]
    if seq.endswith(')'):
        seq = seq[:-1]
    print(seq)
    cond1, cond2 = seq.split(operator)
    cond1 = TRIM(cond1)
    cond2 = TRIM(cond2)

    return str('$'+operator), *[cond.split(' ') for cond in [cond1, cond2]]


def check_nested(checked_list):
    brackets_indexes = []
    
    for i, e in enumerate(checked_list):
        if e == '(' or e == ')':
            brackets_indexes.append(i)
    if len(brackets_indexes) > 0:
        half = len(brackets_indexes) / 2
        start_indexes = brackets_indexes[:half]
        end_indexes = brackets_indexes[half:]
        return [start_indexes.reverse(), end_indexes]
    return None


def exist_and_strip(element):
    element = TRIM(element)
    if '[' in element:
        element = element.strip('[')
    if '(' in element:
        element = element.strip('(')
    if ',' in element:
        element = element.strip(',')
    if ']' in element:
        element = element.strip(']')
    if ')' in element:
        element = element.strip(')')
    if '\'' in element:
        element = element.strip('\'')
    
    return TRIM(element)


def parser_list(elements):
    ll = []
    for e in elements:
        if e == '(' or e == ')' or e =='[' or e == ']':
            continue
        
        element = exist_and_strip(e)

        if element.isnumeric():
            element = int(element)
        
        ll.append(element)
    return ll
        

def translate_symbol(symbol):
    if symbol == '=': return '$eq'
    if symbol == '<': return '$lt'
    if symbol == '<=': return '$lte'
    if symbol == '>': return '$gt'
    if symbol == '>=': return '$gte'
    if symbol == '!=': return '$ne'
    if symbol == 'not': return '$not'
    if symbol == 'like': return '$regex'
    if symbol == 'in': return '$in'
    

def find_all_occurances(sub, sql_query):
    return [sub.start() for sub in re.finditer(sub.lower(), sql_query.lower())]

def create_one_struct(start_index, end_index, sql_query):
    query_sec = sql_query[start_index:end_index]
    print(query_sec)
    one_query = {
        'select':None,
        'delete':None,
        'update':None,
        'from': None,
        'where': None,
        'having': None,
        'group by': None
    }
    select_index = find_all_occurances('select', query_sec)
    delete_index = find_all_occurances('delete', query_sec)
    update_index = find_all_occurances('update', query_sec)
    # if len(select_index) > 0:
    #     # found select statemnt
    # elif len(delete_index) > 0:
    #     # found delete
    # else:
    #     # fpund update

    distinct_index = find_all_occurances('DISTINCT', query_sec)
    
    
    from_index  = find_all_occurances('from', query_sec)
    where_index = find_all_occurances('where', query_sec)
    having_index = find_all_occurances('having', query_sec)
    group_by_index = find_all_occurances('group by', query_sec)

    if len(group_by_index) >= 1:
        start_index = group_by_index[0] + len('group by')  
        one_query['group by'] = TRIM(query_sec[start_index:end_index])
        end_index = group_by_index[0]
    
    if len(having_index) >= 1:
        start_index = having_index[0] + len('having') 
        one_query['having'] = TRIM(query_sec[start_index:end_index])
        end_index = having_index[0]
    
    if len(where_index) >= 1:
        start_index = where_index[0] + len('where') 
        one_query['where'] = TRIM(query_sec[start_index:end_index])
        end_index = where_index[0]

    if len(from_index) >= 1:
        start_index = from_index[0] + len('from') 
        one_query['from'] = TRIM(query_sec[start_index:end_index])
        end_index = from_index[0]

    if len(select_index) >= 1:
        start_index = select_index[0] + len('select') 
        one_query['select'] = {}
        if distinct_index:
            one_query['select']['distinct'] = True
            start_index += len('distinct') +2
        else:
            one_query['select']['distinct'] = False

        select_statment = TRIM(query_sec[start_index:end_index])
        if ',' in select_statment:
            one_query['select']['elements'] = [TRIM(elemnt) for elemnt in select_statment.split(',')]
        else:
            one_query['select']['elements'] = select_statment
        end_index = select_index[0]
    
    if len(delete_index) >= 1:

        start_index = delete_index[0] + len('delete') 
        one_query['delete'] = True

    if len(update_index) >= 1:
        start_index = update_index[0] + len('update') 
        one_query['update'] = True
        
    
    return one_query



def SelectQue(sql_dict):
    que = {
        'conditionals': {},
        'present': {}
    }
    if not sql_dict['select']['elements'] == "*":
        
        hold = sql_dict['select']['elements']
        if "_id" not in hold:
            que['present']['_id'] = 0
        if type(hold) == list:
            for x in hold:
                que['present'][x] = 1 
        else:
            que['present'][hold] = 1 
    
    if sql_dict['where']:
        que['conditionals'] = parser_where(sql_dict['where'])
    return que

    


def check_math_logics(elements):
    to_stop = False
    not_flag = False

    
    for i, e in enumerate(elements):
        for c in ['<', '>', '=', '!', 'not', 'like', 'in']:
            if c in e:
                if c == 'not':
                     not_flag = True
                     continue
                
                elif c == 'in':
                    tmp_dict = {translate_symbol(e): parser_list(elements[i+1: len(elements)])} 
                    to_stop = True
                    break

                else:
                    concate = ' '.join(elements[i+1: len(elements)])
                    concate = TRIM(concate).strip('\'')
                    t = concate
                    t = t.replace(')', '')
                    
                    if t.isnumeric():
                        t = int(t)
                    
                    elif not_flag:
                        t = t.replace('not', '')
                    
                    tmp_dict = {translate_symbol(e): t}                         
                    to_stop = True
                    break
        if to_stop:
            break
    
    
    if not_flag:
        tmp_dict = {'$not': tmp_dict}
        elements.remove('not')

    if i-1 > 0:
        concate = ' '.join(elements[0: i-1])
        key_name = TRIM(concate).strip('\'')
    else:
        key_name = elements[i-1]
    if to_stop:
        key_name = key_name.replace('(', '')
        return {key_name: tmp_dict}
    else: 
        return {}




def get_new_values(query):
    first_index = query.find('set') + 3
    last_index = query.find('where')
    get_segment = query[first_index:last_index]
    split = get_segment.split(',')
    size_split = len(split)
    count = 0
    que = {'$set':{}}
    
    for seg in split:
        split_second = seg.split('=')
        que['$set'][TRIM(split_second[0])] = exist_and_strip(TRIM(split_second[1]))
    return que

def parser_where(where_sec):
    print(where_sec)
    if 'and' in where_sec or 'or' in where_sec:
        if 'and' in where_sec and 'or' in where_sec:
            and_index  = find_all_occurances('and', where_sec)[0]
            or_index  = find_all_occurances('or', where_sec)[0]

            if and_index < or_index:
                major1, major2 = [TRIM(major) for major in where_sec.split('and')][:2]

                minor_op, minor1, minor2 = extract_nested_conditinals(major2, 'or')
                minor_cond = {
                    minor_op:[
                        check_math_logics(minor1),
                        check_math_logics(minor2)
                    ]
                }

                temp = {
                    '$and':[
                            check_math_logics(major1.split(' ')),
                            minor_cond
                    ]
                }
                return temp
            else:
                major1, major2 = [TRIM(major) for major in where_sec.split('or')][:2]

                minor_op, minor1, minor2 = extract_nested_conditinals(major2, 'and')
                minor_cond = {
                    minor_op:[
                        check_math_logics(minor1),
                        check_math_logics(minor2)
                    ]
                }

                temp = {
                    '$or':[
                            check_math_logics(major1.split(' ')),
                            minor_cond
                    ]
                }

                return temp
            
            
        else:
            if 'and' in where_sec:
                op, cond1, cond2 = extract_nested_conditinals(where_sec, 'and')
            else: # or in sentence
                op, cond1, cond2 = extract_nested_conditinals(where_sec, 'or')
            
            temp = {
                op:[
                    check_math_logics(cond1),
                    check_math_logics(cond2)
                ]
            }

        return temp
    
    else:
        list_where_elements = where_sec.split(' ')
        translated_logics = check_math_logics(list_where_elements)
        return translated_logics

def check_aggregation(element):
    start = find_all_occurances('\(', element)
    end = find_all_occurances('\)', element)
    if len(start) == 0:
        return None

    start = start[0]
    end = end[0]

    operation = element[:start]
    op_on = element[start+1:end]
    op_on = f'${op_on}'
    

    if end+1 == len(element):
        name = operation
    elif 'as' in element:
        print(element)
        print(TRIM(TRIM(element[end+1:]).strip('as')))
        name = exist_and_strip(TRIM(TRIM(element[end+1:]).strip('as')))
    else:
        name = exist_and_strip(TRIM(element[end+1:]))

    if operation == 'count':
        operation = 'sum'
        op_on = 1
    
    t = {
        name:{
            f'${operation}': op_on
        }
    }

    if name == 'count':
        pprint(t)

    return t



def multi_group_by(names, conditionals):
    match_dict = {
        '$match': conditionals
    }

    temp ={
        '$group':{'_id': {}}
    }

    disp_names = []
    aggrate_names = []
    for name in names:
        res = check_aggregation(name)
        if not res:
            temp['$group']['_id'][name] = f'${name}'
            disp_names.append(name)
        else:
            name = list(res.keys())[0]

            # temp['$group']['_id'][name] = res[name]
            temp['$group'][name] = res[name]
            aggrate_names.append(name)
        
    pprint(temp)
    temp1 = {
        '$project': {name:f'$_id.{name}' for name in disp_names}
    }

    for name in aggrate_names:
        temp1['$project'][name] = f'${name}'

    temp1['$project']['_id'] = 0
    # temp1['$project']['$sort'] = {"$_id.count":1}
    pprint(temp1)
    
    return [match_dict, temp, temp1]
    


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
collections = mydb.list_collection_names()
mycol = mydb["customers"]

    # nested_indexes = check_nested(list_where_elements)
# DISTINCT
# s = "Select name, address, count(num) from customers where num = 5 or name = 'Amy'"   
# s = "Select name, max(num), min(num) from customers"   
#s = "delete from customers where name = 'Viola' and _id > 1000"   
#s = "select * from customers"
#s = "delete from customers where name = 'yoav'"
#s = "update customers set name = 'yoav', address = 'meow' where name = 'matan'"

# s = "Select name, address, sum(num) from customers where name like '^J' or (name not like 'y$' and address not like '^S')"
# s = "Select name, address from customers where num = 5 or name not in ['John', 'Chuck', 'Susan']"
# s = "Select name, address from customers where num != 4 or (name = 'Amy' and address = 'Apple st 652')"
# s = "Select name, address from customers where num = 5 and (name = 'John' or name = 'Amy')"
# s = "Select name, address from customers where num = 5 or name = 'Amy'"

sql_dict = create_one_struct(start_index=0, end_index=len(s), sql_query=s)

if sql_dict['delete']:
    pprint(sql_dict)
    if sql_dict['where']:
        cond = parser_where(sql_dict['where'])
    
    if sql_dict['from'] in collections:
        mycol = mydb[sql_dict['from']]
    
    x = mycol.delete_many(cond)

    print(x.deleted_count, " documents updated.") 

elif sql_dict['update']:
    pprint(sql_dict)
    if sql_dict['where']:
            filter = parser_where(sql_dict['where'])

    mycol = mydb["customers"]

    newValues = get_new_values(s)
    print(newValues)
    print(filter)
    x = mycol.update_one(filter,newValues)

    #print(x.updated_count, " documents deleted.") 

else: # Select Statement
    pprint(sql_dict)
    que = SelectQue(sql_dict)

    if sql_dict['from'] in collections:
        mycol = mydb[sql_dict['from']]
    else:
        print(f"{sql_dict['from']} collection not found")

    distinct_names = [TRIM(str(key)) for key, val in que['present'].items() if val ==1]
    if sql_dict['select']['distinct']:

        if len(distinct_names) == 1:
            for x in mycol.distinct(distinct_names[0], que['conditionals']):
                print(x)
        else:
            for x in mycol.aggregate(multi_group_by(distinct_names, que['conditionals'])):
                print(x)

    else:
        to_stop = False
        for name in distinct_names:
            for agr in ['min', 'max', 'sum', 'avg', 'count']:
                if agr in name:
                    for x in mycol.aggregate(multi_group_by(distinct_names, que['conditionals'])):
                        print(x)
                    to_stop = True
                    break

            if to_stop:
                break
                
        if not to_stop:

            for x in mycol.find(que['conditionals'], que['present']):
                print(x)