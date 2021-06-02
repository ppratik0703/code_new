import pyodbc
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class CustomSQLHook(BaseHook):
        
    def __init__(self,
                 driver,
                 server,
                 username,
                 password,
                 database):

        self.cnxn = pyodbc.connect('DRIVER='+driver+
                                   ';SERVER='+server+
                                   ';DATABASE='+database+
                                   ';UID='+username+
                                   ';PWD='+password+';')
        self.cursor = self.cnxn.cursor()


    def insert_rows(self, insert_query, params):
        try:
            self.cursor.execute(insert_query, params)
            self.cnxn.commit()
        except Exception as e:
            raise AirflowException('Error while executing query: {}'.format(e))

    def close_connection(self):
        self.cnxn.close()
