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

    def get_data():
        # Connect to the database.
        try:
            with self.connection.cursor() as cursor:
            
                # SQL 
                sql = "SELECT COUNT(*) FROM `yourproject.yourdataset.yourtable`"
                
                # Execute query.
                cursor.execute(sql)
                
                print ("cursor.description: ", cursor.description)
                print()
                for row in cursor:
                    print(row)
                # SQL 
                sql = "explain;"
                
                # Execute query.
                cursor.execute(sql)
                
                print ("cursor.description: ", cursor.description)
                print()
                for row in cursor:
                    print(row)
        except:
            print("We couldn't get your data...")
            
        finally:
            # Close connection.
            connection.close()


    def authenticate_connection(username, password, project_id, dataset_id):
        self.connection = pymysql.connect(host='proxy.superquery.io',
                             user=username,
                             password=password,                             
                             db=project_id + "." + dataset_id,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        print ("Connection to superQuery successful!")

    


