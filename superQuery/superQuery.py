import pymysql.cursors 
import os
import json
from copy import deepcopy

class QueryJobConfig(object):

    def __init__(self):
        self.destination = None
        
class Row(object):
    def __init__(self, rowdict):
        self.__dict__ = rowdict

    def __getitem__(self, key):
        return self.__dict__[key]

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
        self.username = os.environ["SUPERQUERY_USERNAME"] if os.environ["SUPERQUERY_USERNAME"] is not None else None
        self.password = os.environ["SUPERQUERY_PASSWORD"] if os.environ["SUPERQUERY_PASSWORD"] is not None else None
        self._destination_dataset = None 
        self._destination_project = None 
        self._destination_table = None
        self._write_disposition = None
        self.connection = self.authenticate_connection(self.username, self.password)
    
    def dataset(self, dataset):
        self._destination_dataset = dataset
        return self

    def table(self, table):
        self._destination_table = table
        return self

    def write_disposition(self, disposition):
        self._write_disposition = disposition
        return self

    def destination_project(self, project):
        self._destination_project = project
        return self

    def get_data_by_key(self, key, username=None, password=None):
        print("Up next...")

    def query(self, sql, project=None, dry_run=False, username=None, password=None, close_connection_afterwards=True, job_config=None):
        
        try:
            if (username is None or password is None):
                username = self.username 
                password = self.password 

            if ((username is not None and password is not None) and self.connection is None):
                self.connection = self.authenticate_connection(username, password)

            if (job_config):
                if (job_config.destination):
                    self.set_destination_project(self._destination_project) 
                    self.set_destination_dataset(self._destination_dataset) 
                    self.set_destination_table(self._destination_table)
                    self.set_write_disposition(self._write_disposition if self._write_disposition else "WRITE_TRUNCATE")
            
            self.set_dry_run(dry_run)
            self.set_user_agent(agentString="python")
            
            if (project):
                self.set_project(project=project)
        
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.result.set_cur(cursor)
                self.stats
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
            print("[sQ] ...Closing the connection")
            self.connection.close()
            self.connection = None

    def set_user_agent(self, agentString=None):
        if (self.connection is not None and agentString is not None):
            self.connection._execute_command(3, "SET super_userAgent=python")
            self.connection._read_ok_packet()

    def set_project(self, project=None):
        if (self.connection is not None and project is not None):
            print("[sQ] ...Setting the project to ", project)
            self._project = project 
            self.connection._execute_command(3, "SET super_projectId=" + project)
            self.connection._read_ok_packet()

    def set_destination_project(self, project=None):
        if (self.connection is not None and project is not None):
            print("[sQ] ...Setting the destination project to", project)
            self.connection._execute_command(3, "SET super_destinationProject=" + project)
            self.connection._read_ok_packet()
        elif (self.connection is not None and self._project is not None):
            print("[sQ] ...Setting the destination project to the current project ", self._project)
            self.connection._execute_command(3, "SET super_destinationProject=" + self._project)
            self.connection._read_ok_packet()

    def set_destination_dataset(self, dataset=None):
        if (self.connection is not None and dataset is not None):
            print("[sQ] ...Setting the destination dataset to", dataset)
            self.connection._execute_command(3, "SET super_destinationDataset=" + dataset)
            self.connection._read_ok_packet()

    def set_destination_table(self, table=None):
        if (self.connection is not None and table is not None):
            print("[sQ] ...Setting the destination table to", table)
            self.connection._execute_command(3, "SET super_destinationTable=" + table)
            self.connection._read_ok_packet()

    def set_write_disposition(self, disposition=None):
        if (self.connection is not None and disposition is not None):
            print("[sQ] ...Setting write-disposition to", disposition)
            self.connection._execute_command(3, "SET super_destinationWriteDisposition=" + disposition)
            self.connection._read_ok_packet()
        
    def set_dry_run(self, on=False):
        if (self.connection is not None and on):
            self.connection._execute_command(3, "SET super_isDryRun=true")
            self.connection._read_ok_packet()
        else:
            self.connection._execute_command(3, "SET super_isDryRun=false")
            self.connection._read_ok_packet()    

    def authenticate_connection(self, username=None, password=None, hostname='bi.superquery.io', port=3306):
        try:
            if (username is not None and password is not None):
                self.auth["username"] = username
                self.auth["password"] = password
       
            if (not hasattr(self, 'connection') or (hasattr(self, 'connection') and self.connection is None)):
                self.connection = pymysql.connect(
                                    host=hostname,
                                    port=port,
                                    user=self.auth["username"] if self.auth["username"] else username,
                                    password=self.auth["password"] if self.auth["password"] else password,                          
                                    db="",
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
                if (self.connection):
                    print ("[sQ] ...Connection to superQuery successful")
                    return self.connection
                else:
                    print("[sQ] ...Couldn't connect to superQuery!")
            else:
                print("[sQ] ...Using existing superQuery connection!")
                return self.connection
            
        except Exception as e:
            print("[sQ] ...Authentication problem!")
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
            # self.result.set_job_reference(json.loads(explain[0]["jobReference"]))
            return self.result.stats
        else:
            return {}



    


