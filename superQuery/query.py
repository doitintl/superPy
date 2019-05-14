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

    def get_data_by_key(self, key, username=None, password=None):
        print("Up next...")

    def get_data(self, sql, get_stats=False, dry_run=False, username=None, password=None):
        
        try:

            if ( (username != None) & (password != None) ):
                self.authenticate_connection(username, password)

            self.set_dry_run(dry_run)

            with self.connection.cursor() as cursor:
        
                # Execute query.
                print("[sQ] Executing> ", sql)
                cursor.execute(sql)
                
                self.result["data"] = cursor.fetchall()

                if (len(self.result["data"]) == 1):
                    print("[sQ]...1 row received")
                else: 
                    print("[sQ]...{0} rows received".format(len(self.result["data"])))

                if (get_stats):
                    self.result["stats"] = self.stats

        except Exception as e:
            print("[sQ]...We couldn't get your data.")
            print(e)
            
        finally:
            self.connection.close()
            return self.result

    def set_dry_run(self, on=False):
        if ( (self.connection != None) & on ):
                print("[sQ]...Doing a dry-run")
                self.connection._execute_command(3, "SET super_isDryRun=true")
                self.connection._read_ok_packet()
        else:
            print("[sQ]...Running a query")
            self.connection._execute_command(3, "SET super_isDryRun=false")
            self.connection._read_ok_packet()    

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
                print ("[sQ]...Connection to superQuery successful!")
            else:
                print("[sQ]...Couldn't connect to superQuery!")
        except Exception as e:
            print("[sQ]...Authentication problem!")
            print(e)

    @property
    def stats(self):
        if (self.result["stats"]):
            return self.result["stats"]
        elif (self.connection.cursor()):
            cursor = self.connection.cursor()
            
            cursor.execute("explain;")
            explain = cursor.fetchall()
            self.result["stats"] = json.loads(explain[0]["statistics"])
            return self.result["stats"]
        else:
            return {}



    


