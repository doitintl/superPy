import pymysql.cursors 

class SuperQuery(object):
    """
    """

    def __init__(self):
        """
        """
        self.auth = { username: "", password: ""}
        self.explain = {}
        self.result = None
        self.connection = None

    def get_data(self, sql, get_stats=False):
        # Connect to the database.
        try:
            with self.connection.cursor() as cursor:
            
                # SQL 
                sql = sql
                
                # Execute query.
                cursor.execute(sql)
                
                print ("cursor.description: ", cursor.description)
                print()
                for row in cursor:
                    print(row)

                if (get_stats):
                    self.get_statistics(cursor)
        except:
            print("We couldn't get your data...")
            
        finally:
            # Close connection.
            connection.close()


    def authenticate_connection(self, username, password, project_id, dataset_id):
        self.connection = pymysql.connect(host='proxy.superquery.io',
                             user=username,
                             password=password,                             
                             db=project_id + "." + dataset_id,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        print ("Connection to superQuery successful!")

    def get_statistics(cursor):

        # SQL 
        sql = "explain;"
        
        # Execute query.
        cursor.execute(sql)
        
        print ("cursor.description: ", cursor.description)
        print()
        for row in cursor:
            print(row)

    


