################################################################
#                                                              #
#       Use this file to fill up a given database table        #
#                                                              #
#    NOTE: This file contains a lot of progress bar prints.    #
#       Feel free to remove for a tiny performance boost.      #
#                                                              #
################################################################

import MongoDB.session as ses
import MongoDB.read_collection as read
import PostgreSQL.connect_db as connect
import PostgreSQL.select as sel

def filter_data(data, index, new_type):
    """Filter a nested array

    Due to, lets say, interesting datatypes in our MongoDB databse,
    we require quite a bit of type filtering to insert the correct types into PostgreSQL.
    To do so, this funciton can change the type of a variable on a given index for each datapoint.

    @params
    data: the data that requires filtering. Data must consist of itterable datapoins.
    index: the index on which the information in a given datapoint is changed.
    new_type: the type the data will be converted to. (can be: 'string', 'int', 'non-array', 'date')

    return: filtered datapoints
    """ 
    for datapoint in data:
        if datapoint[index] != None:
            if new_type == "string":
                datapoint[index] = str(datapoint[index])
            elif new_type == "int":
                datapoint[index] = int(datapoint[index])
            elif new_type == "non-array":
                datapoint[index] = datapoint[index][0]
            elif new_type == "date":
                datapoint[index] = str(datapoint[index]).split(' ',1)[0]
    return data

def filter_array(array, new_type):
    """Filter a non-nested array

    Similar to filter_data(), this function will apply type conversion for all items in given array.

    @params
    array: array of information that has to be converted
    new_type: the type the data will be converted to. (can be: 'string', 'int', 'non-array', 'date')

    return: filtered datapoints
    """ 
    filtered_array = []
    for item in array:
        if new_type == "string":
            filtered_array.append(str(item))
        elif new_type == "int":
            filtered_array.append(int(item))
        elif new_type == "non-array":
            filtered_array.append(item[0])
        elif new_type == "date":
            filtered_array.append(str(item).split(' ',1)[0])
    return filtered_array

def insert_data(cursor, table, data, labels):
    """Insert array of datapoints into table of given cursor
    
    @params
    cursor: cursor corresponding to required connection
    table: name of table to insert into
    data: list of datapoints that are to be inserted
    labels: list of strings of database labels
    """
    number_of_datapoints = len(data)
    for i, datapoint in enumerate(data):
        percentage = round(i/(number_of_datapoints-1)*100, 2)
        print('Progress: [{}{}{}]  {}%'.format( ('=' * int(percentage//10) ), ('>' if percentage < 100 else ''), ('.' * int(10-(((percentage)//10))-1)), percentage ), end='\r')
        insert_item(cursor, table, labels, datapoint)

def insert_item(cursor, table, labels, datapoint):
    """insert one datapoint into table of given cursor
    
    @params
    cursor: cursor corresponding to required connection
    table: name of table to insert into
    labels: list of strings of database labels
    datapoint: list of data that has to be inserted
    """
    cursor.execute(f'''INSERT INTO {table} ({', '.join(labels)}) VALUES ({('%s,'*len(datapoint))[:-1]})''',  datapoint)

def filter_orders(data):
    """Filter orders

    This function will filter orders from each session.
    For each session all potentially ordered products are saved.
    Only sessions with orders will appear in this data

    @params
    data: sessions data

    return: filtered orders
    """
    print("filtering orders")
    filtered_data = []
    number_of_datapoints = len(data)
    for i, item in enumerate(data):
        percentage = round(i/(number_of_datapoints-1)*100, 2)
        print('Progress: [{}{}{}]  {}%'.format( ('=' * int(percentage//10) ), ('>' if percentage < 100 else ''), ('.' * int(10-(((percentage)//10))-1)), percentage ), end='\r')
        try:
            if item[1]:
                for products in item[2]:
                    product_id = products["id"]
                    filtered_data.append([item[0], product_id])
        except TypeError:
            continue
    return filtered_data

def filter_history(data):
    """Filter history data

    This function will check which history types are contained within the profile data.
    Only previously recommended or viewed before products are saved, 
    all other data or users without said history will not be saved.

    @params
    data: profile data

    return: filtered history data
    """
    filtered_data = []
    for item in data:
        try:
            for product_id in item[1]:
                if len(product_id) > 32: # Some product-ids are saved faulty
                    continue
                filtered_data.append([item[0], product_id, "previously_recommended"])
        except TypeError:
            continue
        try:
            for product_id in item[2]:
                if len(product_id) > 32: # Some product-ids are saved faulty
                    continue
                filtered_data.append([item[0], product_id, "viewed_before"])
        except TypeError:
            continue
    return filtered_data

def filter_profiles(data):
    """Filter profiles data

    This function will separate the profiles with their BUIDS into separate instances.
    Profiles without BUIDS will be skipped.
    BUID duplicates are not allowed, the latest profile_id will be linked.
    
    @params
    data: Unfiltered profile data 

    return: list of tuples containing (buid, profile_id)
    """
    filtered_data = {}
    for datapoint in data:
        if datapoint[1] != None:
            for buid in datapoint[1]:
                filtered_data[buid] = datapoint[0]
    return list(filtered_data.items())

def link_sessions_to_profile(cursor, data):
    """Link sessions to profiles
    
    For each session a field containing a profile_id is required.
    To get said profile_id, a querry looking for the profile_id in profiles will be carried out.
    If no profile is found, the session will not be saved.
    If multiple profiles correspond to the same session, the first profile will count.
    When a profile is found, the datapoint will be saved and eventually returned

    @params
    cursor: PostgreSQL cursor
    data: unlinked sessions data

    return: array of linked sessions data  
    """
    print("linking sessions to profiles")
    linked_data = []
    number_of_datapoints = len(data)
    for i, datapoint in enumerate(data):
        percentage = round(i/(number_of_datapoints-1)*100, 2)
        print('Progress: [{}{}{}]  {}%'.format( ('=' * int(percentage//10) ), ('>' if percentage < 100 else ''), ('.' * int(10-(((percentage)//10))-1)), percentage ), end='\r')
        if isinstance(datapoint[1], list):
            datapoint[1] = datapoint[1][0]
        profile_id = sel.postgresql_select(cursor, ["profile_id"], "profiles", [f"buid = '{datapoint[1]}'"])
        if len(profile_id) == 0:
            continue
        profile_id = filter_array(profile_id, "non-array")
        linked_data.append([datapoint[0], profile_id[0]])
    return linked_data

def fill_table(table_name, cursor, collection, collection_labels, table_labels, filters=None):
    """Fill a desired table
    
    To prevent code duplication this function is a catch all for filling a table based on one MongoDB collection.
    It will read the data from the given MongoDB collection on given labels.
    After which the data can be filtered, either broadly or specifically for certain tables.
    The data will then be inserted into PostgreSQL

    @params
    table_name: the name of the PostgreSQL table the data will be inserted into
    cursor: PostgreSQL cursor 
    collection: MongoDB collection
    collection_labels: List of strings, containing what data will be read from the collection
    table_labels: list of strings, corresponding to the labels in the PostgreSQL table
    filters [OPTIONAL]: list of strings or tuples. Eg. [[1, 'string'], 'filter_orders'] 
    """
    data = read.get_collection_information(collection, collection_labels)
    if filters != None:
        for filter in filters:
            if filter == "filter_orders":
                data = filter_orders(data)
            elif filter == "filter_history":
                data = filter_history(data)
            elif filter == "filter_profiles":
                data = filter_profiles(data)
            else:
                data = filter_data(data, filter[0], filter[1])
    print("writing data to PostgreSQL")
    insert_data(cursor, table_name, data, table_labels)

def fill_product_table(mongo_database, cursor):
    """Fill empty product table

    This function will use a connection to a MongoDB to set up a connection to a products collection.
    The reading, filtering and writing of the data is handled by fill_table()

    @params
    mongo_database: a connection to a MongoDB database
    cursor: PostgreSQL cursor 
    """   
    product_collection = ses.get_collection(mongo_database, "products")
    product_collection_labels = ["_id", "brand", "category", "sub_category", "herhaalaankopen", "fast_mover", ["properties", "stock"], ["properties", "discount"]]
    product_table_labels =  ['product_id', 'brand', 'category', 'sub_category', 'repeat_product', 'fast_mover', 'stock', 'discount']
    fill_table('product', cursor, product_collection, product_collection_labels, product_table_labels, filters=[[6, "int"]])

def fill_profiles_table(mongo_database, cursor):
    """Fill empty profiles table

    This function will use a connection to a MongoDB to set up a connection to a visitors collection.
    The reading, filtering and writing of the data is handled by fill_table()

    @params
    mongo_database: a connection to a MongoDB database
    cursor: PostgreSQL cursor 
    """  
    profiles_collection = ses.get_collection(mongo_database, "visitors")
    profiles_collection_labels = ["_id", "buids"]
    profiles_table_labels =  ['BUID', 'profile_id']
    fill_table('profiles', cursor, profiles_collection, profiles_collection_labels, profiles_table_labels, filters=[[0, "string"],"filter_profiles"])

def fill_sessions_table(mongo_database, cursor):
    """Fill empty sessions table

    This function will use a connection to a MongoDB to set up a connection to a sessions collection.
    From this it will read the required data.
    Said data is then filtered.
    After filtering it will be inserted into the sessions table in Postgres    

    NOTE:
    To fill the sessions table, a profiles table is required!

    @params
    mongo_database: a connection to a MongoDB database
    cursor: PostgreSQL cursor 
    """
    sessions_collection = ses.get_collection(mongo_database, "sessions")
    sessions_collection_labels = ["_id", "buid"]
    sessions_data = read.get_collection_information(sessions_collection, sessions_collection_labels, filters={'has_sale' : {'$eq' : True}})
    sessions_data = filter_data(sessions_data, 0, "string")
    sessions_data = filter_data(sessions_data, 1, "non-array")
    sessions_data = link_sessions_to_profile(cursor, sessions_data)
    sessions_table_labels =  ['session_id', 'profile_id']
    insert_data(cursor, "sessions", sessions_data, sessions_table_labels)

def fill_ordered_table(mongo_database, cursor):
    """Fill empty ordered table

    This function will use a connection to a MongoDB to set up a connection to a sessions collection.
    The reading, filtering and writing of the data is handled by fill_table()

    @params
    mongo_database: a connection to a MongoDB database
    cursor: PostgreSQL cursor 
    """  
    session_collection = ses.get_collection(mongo_database, "sessions")
    order_collection_labels = ["_id", "has_sale", ["order", "products"]]
    order_table_labels =  ['session_id', 'product_id']
    fill_table('ordered', cursor, session_collection, order_collection_labels, order_table_labels, filters=[[0, "string"], "filter_orders"])


def fill_history_table(mongo_database, cursor):
    """Fill empty history table

    This function will use a connection to a MongoDB to set up a connection to a visitors collection.
    The reading, filtering and writing of the data is handled by fill_table()

    @params
    mongo_database: a connection to a MongoDB database
    cursor: PostgreSQL cursor 
    """
    profile_collection = ses.get_collection(mongo_database, "visitors")
    history_collection_labels = ["_id", "previously_recommended", ["recommendations", "viewed_before"]]
    history_table_labels =  ['profile_id', 'product_id', 'history_type']
    fill_table('history', cursor, profile_collection, history_collection_labels, history_table_labels, filters=[[0, "string"], "filter_history"])

def set_constraints(cursor):
    """Set constraints for database to make querries faster.
    
    @params
    cursor: PostgreSQL cursor
    """
    cursor.execute('''Delete from history where product_id not in (select product_id from product)''')
    cursor.execute('''ALTER TABLE history ADD CONSTRAINT product_id FOREIGN KEY (product_id) REFERENCES product (product_id);''')
    cursor.execute('''Delete from ordered where product_id not in (select product_id from product)''')
    cursor.execute('''Delete from ordered where session_id not in (select session_id from sessions)''')
    cursor.execute('''ALTER TABLE ordered ADD CONSTRAINT product_id FOREIGN KEY (product_id) REFERENCES product (product_id);''')
    cursor.execute('''ALTER TABLE ordered ADD CONSTRAINT session_id FOREIGN KEY (session_id) REFERENCES sessions (session_id);''')

#################################################################################
#                                                                               #
#                  Establish Mongo and Postgres connections                     #
#                                                                               #
#      Be sure all the variables correspond to your current information.        #
#                                                                               #
#################################################################################

connection = connect.connect_db(host='localhost', database='opisop_sql', user='postgres', password='postgres')
cursor = connection.cursor()
mongo_database=ses.get_database(client=ses.get_client(host="Localhost", port=27017), database_name="opisop")

#################################################################################
#                                                                               #
#                           How to fill the tables                              #
#                                                                               #
#                   Uncomment each line to fill the tables.                     #
#               NOTE: you have to run profiles before sessions.                 #
#                                                                               #
#################################################################################


fill_product_table(mongo_database, cursor)
fill_profiles_table(mongo_database, cursor)
fill_sessions_table(mongo_database, cursor)
fill_ordered_table(mongo_database, cursor)
fill_history_table(mongo_database, cursor)


# Assuming youve filled all tables, run the line below to clean them up and set the key constraints.
set_constraints(cursor)


# close cursor and commit connection at the end of the file.
cursor.close()
connection.commit()
