from pymongo import MongoClient

def get_client(host: str="Localhost", port: int=27017):
    """Get a client for given host and port
    
    @params
    host: host for client connection (DEFAULT=Localhost)
    port: port for given host (DEFAULT=27017)

    return: mongoDB client connection
    """
    return MongoClient(host, port)

def get_database(client: MongoClient, database_name: str):
    """Get requested database instance for given client
    
    @params
    client: a given mongoclient
    database_name: string containing name of required database

    return: requested database
    """
    return client[database_name]

def get_collection(database, collection_name: str):
    """Get collection from given database
    
    @params
    database: a given database containing required connection
    collection_name: string containing name of required collection

    return: requested collection
    """
    return database[collection_name]
