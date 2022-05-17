import psycopg2

def connect_db(host, database, user, password):
    """Connect to a PostgreSQL database

    @params
    host: string of host 
    database: string of database
    password: string form of database password

    return: connection
    """
    connection = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    return connection
