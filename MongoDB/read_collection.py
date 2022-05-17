def get_item_information(item, labels):
    """Get array of information from given item, based on labels
    
    @params
    item: a item of a given mongoDB collection
    labels: a list containing the labels of the info looked for in the item

    return: array of information from the item, corresponding to the labels 
    """
    item_info = []
    for label in labels:
        if isinstance(label, list):
            if len(label) == 2:
                try:
                    item_info.append(item[label[0]][label[1]])
                except KeyError:
                    item_info.append(None)
                except TypeError:
                    item_info.append(None)
            elif len(label) == 3:
                try:
                    item_info.append(item[label[0]][label[1]][label[2]])
                except KeyError:
                    item_info.append(None)
                except TypeError:
                    item_info.append(None)
        else:
            try:
                item_info.append(item[label])
            except KeyError:
                item_info.append(None)
            except TypeError:
                item_info.append(None)
    return item_info

def get_collection_information(collection, labels, filters=None):
    """Get array of information from collection, based on given list of labels
    
    @params
    collection: a mongoDB collection
    labels: a list containing the labels of the info looked for in the item

    return: array of arrays of information from the item, corresponding to the labels 
    """
    collection_info = []
    if filters != None:
        for item in collection.find(filters):
            collection_info.append(get_item_information(item, labels))
    else: 
        for item in collection.find():
            collection_info.append(get_item_information(item, labels))
    return collection_info
