import pymysql.cursors 
import json

class SuperQuery(object):
    """
    """

    def __init__(self):
        """
        """
        self.auth = { "username": "", "password": ""}
        self.explain = {}
        self.result = []
        self.stats = {}
        self.connection = None

    def get_data(self, sql, get_stats=False):
        # Connect to the database.

        try:
            with self.connection.cursor() as cursor:
            
                # SQL 
                sql = sql
                
                # Execute query.
                cursor.execute(sql)
                
                for row in cursor:
                    self.result.append(row)

                if (len(self.result) == 1):
                    print("1 row received")
                else: 
                    print("{0} rows received".format(len(self.result)))

                if (get_stats):
                    self.get_statistics(cursor)
        except Exception as e:
            print("We couldn't get your data...")
            print(e)
            
        finally:
            # Close connection.
            self.connection.close()

            # Return the data
            return self.result

    def authenticate_connection(self, username, password, project_id, dataset_id):
        self.connection = pymysql.connect(host='proxy.superquery.io',
                             user=username,
                             password=password,                             
                             db=project_id + "." + dataset_id,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        print ("Connection to superQuery successful!")

    def get_statistics(self, cursor):

        # SQL 
        sql = "explain;"
        
        # Execute query.
        cursor.execute(sql)
        
        for row in cursor:
            self.stats = row

        self.stats = json.loads(self.stats["statistics"])



    

