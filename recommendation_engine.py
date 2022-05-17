############################################
#                                          #
#   Use this file to get recommendations   #
#                                          #
############################################

import PostgreSQL.connect_db as connect
import PostgreSQL.select as sel

def get_content_recommendations(product_id, ammount):
    """Retrieve content recommendation
    
    As our content recommendation is pregenerated into the content_rule table,
    this function will try to fetch the first 'amount' of recommendations from said table.
    
    If a product has less then 'amount' of recommendations, less will be returned
    If a product does not have any recommmendations an empty array will return

    @params
    product_id: product_id for the product that needs recommendations
    amount: the maximum amount of recommendations returned

    returns: list of product ids corresponding to the request
    """
    connection = connect.connect_db(host='localhost', database='opisop_sql', user='postgres', password='postgres')
    cursor = connection.cursor()

    recommended_product_ids = sel.postgresql_select(cursor, ["recommended_product_ids"], "content_rule", [f"product_id = '{product_id}'"])
    try:
        recommended_product_ids = recommended_product_ids[0][0] # stupid SQL nesting
        connection.close()
        cursor.close()
        return recommended_product_ids[:ammount]
    except IndexError:
        return []

def get_profile_recommendations(profile_id, comparative_user_ammount, recommendation_amount):
    """get recomendation for given profile

    For our specific rule, we compare a user to the 'comparative_user_ammount' most corresponding users,
    when looking at the overlap in products bought between the two.
    For said corresponding users, the products that do not overlap will be collected.
    These products are then ordered based on frequency of appearence, stock, discount, repeat product, fast_mover
    from this the top 'recommendation_amount' will be selected and returned.

    @params
    profile_id: the id of the user that needs recommendations
    comparative_user_amounts: the amount of users the profile is compared to
    recommendation_amount: amount of products returned at the end

    return: list of product ids with the maximal length of 'recommendation_amount'
    """
    connection = connect.connect_db(host='localhost', database='opisop_sql', user='postgres', password='postgres')
    cursor = connection.cursor()

    cursor.execute(
    f'''SELECT p.product_id, p.category, p.sub_category, p.repeat_product, p.fast_mover, p.stock, p.discount,
            COUNT(profile_id) AS product_frequency
        FROM sessions AS s, ordered AS o, product AS p
        WHERE s.session_id=o.session_id 
            AND p.product_id=o.product_id
            AND profile_id IN (
                SELECT profile_id
                FROM(
                    SELECT s.profile_id, 
                        COUNT(o.product_id) AS total
                    FROM sessions AS s, 
                        ordered AS o
                    WHERE s.session_id=o.session_id 
                        AND (o.product_id IN(
                            SELECT o.product_id
                            FROM sessions AS s, ordered AS o
                            WHERE s.session_id=o.session_id AND s.profile_id='{profile_id}'))
                        AND NOT (s.profile_id='{profile_id}')
                    GROUP BY s.profile_id 
                    ORDER BY total DESC) AS table1
                LIMIT {comparative_user_ammount}
            )
            AND o.product_id NOT IN(
                SELECT o.product_id
                FROM sessions AS s, ordered AS o
                WHERE s.session_id=o.session_id AND s.profile_id='{profile_id}'
            )    
        GROUP BY p.product_id
        ORDER BY product_frequency DESC, stock DESC, discount ASC, repeat_product DESC, fast_mover DESC
        LIMIT {recommendation_amount}'''
    )
    recommended_product_ids = cursor.fetchall()
    recommended_product_ids = [x[0] for x in recommended_product_ids]

    connection.close()
    cursor.close()
    return recommended_product_ids