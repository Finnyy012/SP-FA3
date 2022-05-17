#########################################################
#                                                       #
#   Use this file to pre-load the content rule table    #
#                                                       #
#########################################################

import PostgreSQL.connect_db as connect
import PostgreSQL.select as sel
import fill_database_table as fill

def generate_content_rule_data(cursor, product_ids):
    """
    generate related data based on content rule

    This content rule works as follows:
        Each product has a brand, category and sub_category
        Each instance of these three fields has 1 or more instances
        Each instance will be linked together
    This does mean that if only one product has a certain combination of the three fields, 
    no recommendation will be made.
    Same for products that do not contain all three fields or just a subset,
    they will not generate a recommendations either.

    @params
    cursor: cursor corresponding to your connection to PostgreSQL
    product_ids: list of all product ids that the content rule will be ran upon

    returns: list of elements as follows:
    [product_id, [list of reccomended product_ids]]
    Where only the instances of products with actual reccomendations are saved
    """
    product_data = []
    for id in product_ids:
        if id.find("'") == -1:
            data = sel.postgresql_select(cursor, ["brand", "category", "sub_category"], "product", [f"product_id = '{id}'", "brand is not NULL", "category is not NULL", "sub_category is not NULL"])
            if len(data) == 0:
                product_data.append(None)
            else:
                product_data.append(data[0])
    content_rule_data = []
    for data in set(product_data):
        if data != None and data[0].find("'") == -1 and data[1].find("'") == -1 and data[2].find("'") == -1:
            corresponding_ids = sel.postgresql_select(cursor, ["product_id"], "product", [f"brand = '{data[0]}'", f"category = '{data[1]}'", f"sub_category = '{data[2]}'"])
            corresponding_ids = fill.filter_array(corresponding_ids, "non-array")
            for i, id in enumerate(corresponding_ids):
                if len(corresponding_ids) != 1:
                    content_rule_data.append([id, corresponding_ids[:i]+corresponding_ids[i+1:]])
    return content_rule_data

# Establish connection with PostgreSQL
connection = connect.connect_db(host='localhost', database='opisop_sql', user='postgres', password='postgres')
cursor = connection.cursor()

# Get all product ids 
all_product_ids = sel.postgresql_select(cursor, ["product_id"], "product")
all_product_ids = fill.filter_array(all_product_ids, "non-array")

# get all related products together
content_rule_data = generate_content_rule_data(cursor, all_product_ids)

# insert content rule data into table
fill.insert_data(cursor, "content_rule", content_rule_data, labels=["product_id", "recommended_product_ids"])


cursor.close()
connection.commit()
