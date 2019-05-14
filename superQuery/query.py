import pymysql.cursors 
import json

class AttributeDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttributeDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

class SuperQuery(object):

    def __init__(self):
        self.auth = { "username": None, "password": None}
        self.result = AttributeDict({"data": [], "stats": None})
        self.connection = None

    def get_data(self, sql, get_stats=False, username=None, password=None):
        
        try:

            if ( (username != None) & (password != None) ):
                self.authenticate_connection(username, password)

            with self.connection.cursor() as cursor:
            
                # SQL 
                sql = sql
                
                # Execute query.
                cursor.execute(sql)
                
                for row in cursor:
                    self.result["data"].append(row)

                if (len(self.result["data"]) == 1):
                    print("1 row received")
                else: 
                    print("{0} rows received".format(len(self.result)))

                if (get_stats):
                    self.result["stats"] = self.stats

        except Exception as e:
            print("We couldn't get your data...")
            print(e)
            
        finally:
            # Close connection.
            self.connection.close()

            # Return the data
            return self.result

    def authenticate_connection(self, username=None, password=None):

        try:
            if ( (username != None) & (password != None) ):
                self.auth["username"] = username
                self.auth["password"] = password
       
            self.connection = pymysql.connect(host='proxy.superquery.io',
                                user=self.auth["username"] if self.auth["username"] else username,
                                password=self.auth["password"] if self.auth["password"] else password,                          
                                db="",
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
            
            if (self.connection):
                print ("Connection to superQuery successful!")
            else:
                print("Couldn't connect to superQuery!")
        except Exception as e:
            print("Authentication problem!")
            print(e)

    @property
    def stats(self):

        if (self.result["stats"]):
            return self.result["stats"]
        elif (self.connection.cursor()):

            cursor = self.connection.cursor()

            # SQL 
            sql = "explain;"
            
            # Execute query.
            cursor.execute(sql)
            
            for row in cursor:
                self.result["stats"] = json.loads(row["statistics"])

            return self.result["stats"]
        else:
            return {}



    


