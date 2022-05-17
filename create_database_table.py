#####################################################################
#                                                                   #
#    Use this file to set up a PostgreSQL database with table(s)    #
#                                                                   #
#####################################################################

import PostgreSQL.connect_db as connect

def create_table(name, column_strings):
    """Create SQL statement for creating a table containing given columns
    
    @params
    name: name of table
    column_strings: array of table strings (valid SQL format)

    return: string of SQL statment
    """
    sql_statement = f'CREATE TABLE {name} (' + ','.join(column_strings) + ')'
    return sql_statement

def create_column_string(name, type, optional_flags):
    """Create string for given column

    @params
    name: name of column
    type: type for the variables inside column
    optional_flags: optional flags, like NOT NULL etc

    return: joined string of entered items
    """
    return f'{name} {type} {optional_flags}'

def create_columns(columns):
    """combine singular columns into array of strings

    @params
    columns: a list of columns, where each column consists of 3 items, not 1 singular string

    return: array of singular comments as strings 
    """
    column_list = []
    for i in columns:
        column_list.append(create_column_string(i[0], i[1], i[2]))
    return column_list



# Use the following line to connect to your PostgreSQL 
connection = connect.connect_db(host='localhost', database='opisop_sql', user='postgres', password='postgres')

# Define columns for required categories
product_columns = [
    ['product_id', 'VARCHAR(32)', 'PRIMARY KEY NOT NULL'],
    ['brand', 'VARCHAR(64)', ''],
    ['category', 'VARCHAR(64)', ''],
    ['sub_category', 'VARCHAR(32)', ''],
    ['repeat_product', 'BOOLEAN', ''],
    ['fast_mover', 'BOOLEAN', ''],
    ['stock', 'INT', ''],
    ['discount', 'varchar(64)', ''],
]
sessions_columns = [
    ['session_id', 'VARCHAR(128)', 'PRIMARY KEY NOT NULL'],
    ['profile_id', 'VARCHAR(32)', ''],
]
ordered_columns = [
    ['session_id', 'VARCHAR(128)', 'NOT NULL'],
    ['product_id', 'VARCHAR(32)', 'NOT NULL'], 
]
history_columns = [
    ['profile_id', 'VARCHAR(32)', 'NOT NULL'],
    ['product_id', 'VARCHAR(32)', 'NOT NULL'],
    ['history_type','VARCHAR(128)','NOT NULL'], # can be either "viewed before" or "previously reccomended"
]
content_rule_columns = [
    ['product_id', 'VARCHAR(32)', 'NOT NULL'],
    ['recommended_product_ids', 'VARCHAR(32)[]', ''],
]
profiles_columns = [
    ['profile_id', 'VARCHAR(32)', ''],
    ['BUID', 'VARCHAR(128)', 'PRIMARY KEY NOT NULL'],
]

# convert columns to strings
product_column_strings = create_columns(product_columns)
sessions_column_strings = create_columns(sessions_columns)
ordered_column_strings = create_columns(ordered_columns)
history_column_strings = create_columns(history_columns)
content_rule_column_strings = create_columns(content_rule_columns)
profiles_column_strings= create_columns(profiles_columns)

# create and collect sql statements
sql_statements = []
sql_statements.append(create_table(name='product', column_strings=product_column_strings))
sql_statements.append(create_table(name='sessions', column_strings=sessions_column_strings))
sql_statements.append(create_table(name='ordered', column_strings=ordered_column_strings))
sql_statements.append(create_table(name='history', column_strings=history_column_strings))
sql_statements.append(create_table(name='content_rule', column_strings=content_rule_column_strings))
sql_statements.append(create_table(name='profiles', column_strings=profiles_column_strings))

cursor = connection.cursor()
for statement in sql_statements:
    cursor.execute(statement)
cursor.close()
connection.commit()


#TODO: foreighn key constraingts