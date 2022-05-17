def postgresql_select(cursor, items, table, filters=None):
    """excecutes select command based on parameters
    
    @params
    cursor: cursor for given connection
    items: items you want to select, in the form of an array of strings
    table: string corresponding to table
    filters: OPTIONAL let you use the where statement, in the form of array of strings
    
    returns result of statement
    """
    sql_statement = f'''SELECT {', '.join(items)} FROM {table}''' 
    if filters != None:
        sql_statement += f''' Where {' and '.join(filters)}'''
    cursor.execute(sql_statement)
    return cursor.fetchall()