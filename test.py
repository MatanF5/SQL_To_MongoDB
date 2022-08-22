from pprint import pprint
import pymongo
import re



def find_all_occurances(sub, sql_query):
    return [sub.start() for sub in re.finditer(sub, sql_query.lower())]

TRIM = lambda x : x.strip()
def create_one_struct(start_index, end_index, sql_query):
    query_sec = sql_query[start_index:end_index]
    print(query_sec)
    one_query = {
        'select':None,
        'from': None,
        'where': None,
        'having': None,
        'group by': None
    }
    select_index = find_all_occurances('select', query_sec)
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
        else:
            one_query['select']['distinct'] = False

        select_statment = TRIM(query_sec[start_index:end_index])
        if ',' in select_statment:
            one_query['select']['elements'] = [TRIM(elemnt) for elemnt in select_statment.split(',')]
        else:
            one_query['select']['elements'] = select_statment
        end_index = select_index[0]
    from pprint import pprint
    return one_query
    # print('*')
    # print(query_sec)
    # print(find_all_occurances('select', query_sec))
    # print(find_all_occurances('group by', query_sec))
    # rest_seq = query_sec
    # for key in one_query.keys():

    #     key_part, rest_seq = rest_seq.split(key)
        
    #     print(key, rest_seq.split(key))


    # print(sql_query[start_index:end_index])

def query2struct(sql_query):
    select_indexes = find_all_occurances('select', sql_query)
    print(select_indexes)
    is_nested = False
    if len(select_indexes) > 1:
        is_nested = True
        parenthesis_indexes = find_all_occurances('\)', sql_query)
        print(parenthesis_indexes)
        for i, parenthesis_index in enumerate(parenthesis_indexes):
            start_index = select_indexes[(len(select_indexes)-1) - i]
            end_index = parenthesis_index
            create_one_struct(start_index=start_index, end_index=end_index, sql_query=sql_query.lower())
            print(start_index, end_index)
    create_one_struct(start_index=0, end_index=len(sql_query)-1, sql_query=sql_query.lower())


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
collections = mydb.list_collection_names()
 
 
# Printing the name of the collections to the console.
print(collections)
mycol = mydb["customers"]

col = "customers"
# mylist = [
#   { "_id": 1, "name": "John", "address": "Highway 37"},
#   { "_id": 2, "name": "Peter", "address": "Lowstreet 27"},
#   { "_id": 3, "name": "Amy", "address": "Apple st 652"},
#   { "_id": 4, "name": "Hannah", "address": "Mountain 21"},
#   { "_id": 5, "name": "Michael", "address": "Valley 345"},
#   { "_id": 6, "name": "Sandy", "address": "Ocean blvd 2"},
#   { "_id": 7, "name": "Betty", "address": "Green Grass 1"},
#   { "_id": 8, "name": "Richard", "address": "Sky st 331"},
#   { "_id": 9, "name": "Susan", "address": "One way 98"},
#   { "_id": 10, "name": "Vicky", "address": "Yellow Garden 2"},
#   { "_id": 11, "name": "Ben", "address": "Park Lane 38"},
#   { "_id": 12, "name": "William", "address": "Central st 954"},
#   { "_id": 13, "name": "Chuck", "address": "Main Road 989"},
#   { "_id": 14, "name": "Viola", "address": "Sideway 1633"}
# ]
# print([mycol.insert_one(doc).inserted_id for doc in mylist])

# x = mycol.find_one({"name": "Viola"})
# print(x)



# mycol.delete_one({'name': 'John', 'address': 'Highway 37'}) 
# a=a

def SelectQue(sql_dict):
    que = {
        'first': {},
        'second': {}
    }
    if not sql_dict['select']['elements'] == "*":
        
        hold = sql_dict['select']['elements']
        if "_id" not in hold:
            que['_id'] = 0
        for x in hold:
            que[x] = 1 
    return que
    
def Distinct(sql_dict):
    que = {}
    if sql_dict['select']['elements'] == "*":
        return que
    hold = sql_dict['select']['elements']
    if "_id" not in hold:
        que['_id'] = 0
    for x in hold:
        que[x] = 1 
    return que
    

    
s = "Select * from customers where name = 'John'"
sql_dict = query2struct(s)
sql_dict = create_one_struct(start_index=0, end_index=len(s)-1, sql_query=s.lower())
pprint(sql_dict)

if sql_dict['from'] in collections:
    mycol = mydb[sql_dict['from']]
else:
    print(f"{sql_dict['from']} collection not found")

if sql_dict['select']['distinct']:
    que = Distinct(sql_dict)
    for x in mycol.distinct({}, que):
      print(x)
else:
    que = SelectQue(sql_dict)
    print(que)

    for x in mycol.find({},que['first']):
        print(x)