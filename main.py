from pprint import pprint
import pymongo
import re


TRIM = lambda x : x.strip()

def create_db(mydb):

    mycol = mydb["telephones"]

    mylist = [
    #   { "_id": 100, "name": "John", "address": "Highway 37", 'num': 5},
    #   { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
    #   { "_id": 3, "name": "Amy", "address": "Apple st 652"},
    #   { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
    #   { "_id": 5, "name": "Michael", "address": "Valley 345"},
    #   { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
      
      { "_id": 2000, "number": 11111},
      { "_id": 3000, "number": 222222},
      { "_id": 4000, "number": 333333},
      { "_id": 5000, "number": 4444444},
      { "_id": 6000, "number": 5555555},

      { "_id": 700, "number": 6666666},
      { "_id": 800, "number": 7777777},
      { "_id": 900, "number": 8888888},
      { "_id": 10000, "number": 999999999},
      { "_id": 1100, "number": 1010101010},
      { "_id": 1200, "number": 1111111111111},
      { "_id": 1300, "number": 12121212},
      { "_id": 1400, "number": 13131313} 
    ]
    print([mycol.insert_one(doc).inserted_id for doc in mylist])
    return mycol


def extract_nested_conditinals(seq, operator):
    if seq.startswith('('):
        seq = seq[1:]
    if seq.endswith(')'):
        seq = seq[:-1]
    cond1, cond2 = seq.split(operator)
    cond1 = TRIM(cond1)
    cond2 = TRIM(cond2)

    return str('$'+operator), *[cond.split(' ') for cond in [cond1, cond2]]


# def check_nested(checked_list):
#     brackets_indexes = []
    
#     for i, e in enumerate(checked_list):
#         if e == '(' or e == ')':
#             brackets_indexes.append(i)
#     if len(brackets_indexes) > 0:
#         half = len(brackets_indexes) / 2
#         start_indexes = brackets_indexes[:half]
#         end_indexes = brackets_indexes[half:]
#         return [start_indexes.reverse(), end_indexes]
#     return None


TRIM = lambda x : x.strip()

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
        

def create_one_struct(start_index, end_index, sql_query):
    query_sec = sql_query[start_index:end_index]
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
        one_query['from'] = {}
        lookup, collection = parser_from(TRIM(query_sec[start_index:end_index]), one_query['where'], TRIM(query_sec[:from_index[0]]))
        if not lookup:
            one_query['from']['lookup'] = None
            one_query['from']['collection'] = TRIM(query_sec[start_index:end_index])
        else:
            one_query['from']['lookup'] = lookup
            one_query['from']['collection'] = collection


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


def parser_from(from_seq, where_seq, select_seq, lookup_result_name='lookup_result'):
    left_join_index = find_all_occurances('left join', from_seq)
    if len(left_join_index) == 0:
        return None, None
    from_collection = TRIM(from_seq[:left_join_index[0]])
    left_join_index_end = left_join_index[0]+len('left join ')
    on_index = find_all_occurances(' on ', from_seq)
    to_collection = from_seq[left_join_index_end:on_index[0]]
    
    on_index_end = on_index[0] + len('on ')
    from_id,to_id = from_seq[on_index_end:].split('=')
    lookup_builder = {
        'from_collection': exist_and_strip(TRIM(from_collection)),
        'to_collection': exist_and_strip(TRIM(to_collection)),
        'from_id': exist_and_strip(TRIM(from_id.split('.')[1])),
        'to_id': exist_and_strip(TRIM(to_id.split('.')[1]))
    }
    # print(where_seq, not where_seq)
    if where_seq is None:
        conditionals = {}
    else:
        conditionals = parser_where(where_seq)
    
    select_seq = select_seq[len('select'):]
    
    project_dict = {'_id':0}
        

    for disp_name in select_seq.split(','):
        disp_name = exist_and_strip(TRIM(disp_name))
      
        if '.' in disp_name:
            collection_name, var_name = disp_name.split('.')

            if collection_name == lookup_builder['to_collection']:
                project_dict[var_name] = f'${lookup_result_name}.{var_name}'
        else:
            project_dict[disp_name] = 1

    lookup = [{
        '$lookup': {
            'from': f'{lookup_builder["to_collection"]}', 
            'localField': f'{lookup_builder["from_id"]}', 
            'foreignField': f'{lookup_builder["to_id"]}', 
            'as': f'{lookup_result_name}'
        }},
        {'$unwind': f'${lookup_result_name}'},
        
                
        {'$match': conditionals},
        {'$project': project_dict}
    ]

    # pprint(lookup)
    return lookup, lookup_builder['from_collection']


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
    que = {'$set':{}}
    
    for seg in split:
        split_second = seg.split('=')
        que['$set'][TRIM(split_second[0])] = exist_and_strip(TRIM(split_second[1]))
    return que

def parser_where(where_sec):
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
        
    temp1 = {
        '$project': {name:f'$_id.{name}' for name in disp_names}
    }

    for name in aggrate_names:
        temp1['$project'][name] = f'${name}'

    temp1['$project']['_id'] = 0
    # temp1['$project']['$sort'] = {"$_id.count":1}
    
    return [match_dict, temp, temp1]
    

# def parser_from()


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
# s = "Select name, address from cu stomers where num = 5 or name = 'Amy'"

if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["mydatabase"]
    collections = mydb.list_collection_names()
    # mycol = mydb["customers"]
    # create_db(mydb)
    while True:
        s = input("Please type your query: ")
        if s == 'q':
            print('Bye Bye ..')
            exit()
        else:
            sql_dict = create_one_struct(start_index=0, end_index=len(s), sql_query=s)
            
            if not sql_dict['from']:
                hold = s.split(' ') 
                col = hold[1]
                mycol = mydb[col]
                
            else:
                if sql_dict['from']['lookup']:
                    # to lookup
                    mycol = mydb[sql_dict['from']['collection']]
                    pprint(sql_dict['from']['lookup'])
                    for doc in mycol.aggregate(sql_dict['from']['lookup']):
                        pprint(doc)
                    continue



                else:
                    if sql_dict['from']['collection'] in collections: 
                        mycol = mydb[sql_dict['from']['collection']] 
                    else:
                        print(f"{sql_dict['from']['collection']} collection not found")
                        continue
            
            if sql_dict['delete']:
                if sql_dict['where']:
                    cond = parser_where(sql_dict['where'])

                pprint(cond)                      
                x = mycol.delete_many(cond)
                print(x.deleted_count, " documents updated.") 

            elif sql_dict['update']:
                if sql_dict['where']:
                    filter = parser_where(sql_dict['where'])
                newValues = get_new_values(s)
                pprint(filter)
                pprint(newValues)
                x = mycol.update_one(filter,newValues)
            
            else: # Select Statement
                que = SelectQue(sql_dict)

                # if sql_dict['from']['collection'] in collections:
                #     mycol = mydb[sql_dict['from']['collection']]
                # else:
                #     print(f"{sql_dict['from']} collection not found")

                distinct_names = [TRIM(str(key)) for key, val in que['present'].items() if val ==1]
                if sql_dict['select']['distinct']:
                    pprint(distinct_names)
                    pprint(que['conditionals'])

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
                                pprint(distinct_names)
                                pprint(que['conditionals'])
                                for x in mycol.aggregate(multi_group_by(distinct_names, que['conditionals'])):
                                    print(x)
                                to_stop = True
                                break

                        if to_stop:
                            break
                            
                    if not to_stop:
                        pprint(que['present'])
                        pprint(que['conditionals'])
                        for x in mycol.find(que['conditionals'], que['present']):
                            print(x)

