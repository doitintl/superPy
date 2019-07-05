import pymysql.cursors 
import os
import json
from copy import deepcopy

class Row(object):
    def __init__(self, rowdict):
        self.__dict__ = rowdict

    def to_dict(self):
        return deepcopy(self.__dict__)

class Result:
    
    def __init__(self, cur=None, stats=None):
        self.cur = cur
        self._stats = stats

    def result(self): 
        while True:
            row = self.cur.fetchone()
            if row is not None:
                row = Row(row)
                yield row 
            else:
                break
        
    def set_cur(self, cur):
        self.cur = cur

    def set_stats(self, stats):
        for key, value in stats.items():
            setattr(self, key, stats[key])

    def set_job_reference(self, jobRef):
        for key, value in jobRef.items():
            setattr(self, key, jobRef[key])
            
    @property
    def stats(self):
        return self._stats

class Client(object):

    def __init__(self):
        self.auth = { "username": None, "password": None}
        self.result = Result()
        self.connection = None
        self.username = os.environ["SUPERQUERY_USERNAME"] if os.environ["SUPERQUERY_USERNAME"] is not None else None
        self.password = os.environ["SUPERQUERY_PASSWORD"] if os.environ["SUPERQUERY_PASSWORD"] is not None else None

    def get_data_by_key(self, key, username=None, password=None):
        print("Up next...")

    def query(self, sql, project_id=None, dry_run=False, username=None, password=None, close_connection_afterwards=True):
        
        try:
            if (username is None or password is None):
                username = self.username 
                password = self.password 

            if ((username is not None and password is not None) or (not self.connection)):
                self.authenticate_connection(username, password)
            
            self.set_dry_run(dry_run)
            self.set_user_agent(agentString="proxyApi")
            
            if (project_id):
                self.set_project_id(projectId=project_id)

            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.stats
                self.result.set_cur(cursor)
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
        if (self.connection is not None and agentString is not None):
            self.connection._execute_command(3, "SET super_userAgent=python")
            self.connection._read_ok_packet()

    def set_project_id(self, projectId=None):
        if (self.connection is not None and projectId is not None):
            print("[sQ] ...Setting the projectId to ", projectId)
            self.connection._execute_command(3, "SET super_projectId=" + projectId)
            self.connection._read_ok_packet()
        
    def set_dry_run(self, on=False):
        if (self.connection is not None and on):
            self.connection._execute_command(3, "SET super_isDryRun=true")
            self.connection._read_ok_packet()
        else:
            self.connection._execute_command(3, "SET super_isDryRun=false")
            self.connection._read_ok_packet()    

    def authenticate_connection(self, username=None, password=None, hostname='bi.superquery.io'):
        try:
            if (username is not None and password is not None):
                self.auth["username"] = username
                self.auth["password"] = password
       
            if (not self.connection):
                self.connection = pymysql.connect(
                                    host=hostname,
                                    user=self.auth["username"] if self.auth["username"] else username,
                                    password=self.auth["password"] if self.auth["password"] else password,                          
                                    db="",
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
                if (self.connection):
                    print ("[sQ]...Connection to superQuery successful")
                    return self.connection
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
            self.result.set_job_reference(json.loads(explain[0]["jobReference"]))
            return self.result.stats
        else:
            return {}



    

