import pymysql.cursors 
import json

class Result:
    
    def __init__(self, cur=None, stats=None):
        self.cur = cur
        self.start = 1
        self.stop = 10
        self._stats = stats

    def __iter__(self):
        row = None
        for i in range(1, self.stop+1):
            if (i >= self.start):
                row = self.cur.fetchone()
                if row is None:
                    break
                else:
                    yield row
            else:
                self.cur.fetchone()

    def set_cur(self, cur):
        self.cur = cur
    
    def set_start_stop(self, start, stop):
        self.start = start if start > 0 else 1
        self.stop = stop if start > 0 else 10

    def set_stats(self, stats):

        for key, value in stats.items():
            setattr(self, key, stats[key])

    @property
    def stats(self):
        return self._stats

class SuperQuery(object):

    def __init__(self):
        self.auth = { "username": None, "password": None}
        self.result = Result()
        self.connection = None

    def get_data_by_key(self, key, username=None, password=None):
        print("Up next...")

    def get_data(self, sql, dry_run=False, username=None, password=None, close_connection_afterwards=True, start=1, stop=10):
        
        try:
            if ( (username != None) & (password != None) | (not self.connection)):
                self.authenticate_connection(username, password)
            
            self.set_dry_run(dry_run)
            self.set_user_agent(agentString="proxyApi")

            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.stats
                self.result.set_cur(cursor)
                self.result.set_start_stop(start, stop)
                return self.result
                
        except Exception as e:
            print("[sQ]...We couldn't get your data.")
            print(e)
            
        finally:
            if (close_connection_afterwards):
                self.close_connection()
            return self.result

    def close_connection(self):
        if (self.connection):
            self.connection.close()
            self.connection = None

    def set_user_agent(self, agentString=None):
        if ( (self.connection != None) & (agentString != None) ):
                self.connection._execute_command(3, "SET super_userAgent=proxyApi")
                self.connection._read_ok_packet()
        
    def set_dry_run(self, on=False):
        if ( (self.connection != None) & on ):
                self.connection._execute_command(3, "SET super_isDryRun=true")
                self.connection._read_ok_packet()
        else:
            self.connection._execute_command(3, "SET super_isDryRun=false")
            self.connection._read_ok_packet()    

    def authenticate_connection(self, username=None, password=None):
        try:
            if ( (username != None) & (password != None) ):
                self.auth["username"] = username
                self.auth["password"] = password
       
            if (not self.connection):
                self.connection = pymysql.connect(
                                    host='proxy.superquery.io',
                                    user=self.auth["username"] if self.auth["username"] else username,
                                    password=self.auth["password"] if self.auth["password"] else password,                          
                                    db="",
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
                if (self.connection):
                    print ("[sQ]...Connection to superQuery successful")
                else:
                    print("[sQ]...Couldn't connect to superQuery!")
            else:
                print("[sQ]...Connection to superQuery already established!")
            
        except Exception as e:
            print("[sQ]...Authentication problem!")
            print(e)

    @property
    def stats(self):       
        if (self.result.stats):
            return self.result.stats
        elif (self.connection.cursor()):
            cursor = self.connection.cursor()
            cursor.execute("explain;")
            explain = cursor.fetchall()
            self.result.set_stats(json.loads(explain[0]["statistics"]))
            return self.result.stats
        else:
            return {}



    


