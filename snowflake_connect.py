import snowflake.connector
import pandas as pd
import os
import glob
from snowflake.connector.pandas_tools import write_pandas

class snowflakeLoader:

    def __init__(self,objlst):
        self.cnn = snowflake.connector.connect(user='COE'
                                          ,password='Apple123#'
                                          ,account='xg46362.southeast-asia.azure'
                                          ,database='CONTROL_DB'
                                          ,warehouse='COMPUTE_WH'
                                          ,schema='LOAD_SCHEMA')
        self.ddl_file ='salesforce_DDL_new.sql'
        self.objlst = objlst
        with open(self.ddl_file, "r", encoding="utf-8") as f:
            for cs in self.cnn.execute_stream(f):
                for rt in cs:
                    print(rt)

    def loader(self):
        for each in self.objlst:
            extract_file= glob.glob("SalesforceExtract/*/"+each.lower()+".csv")
            for file in extract_file:
                print(file)
                if os.stat(file).st_size == 0:
                    continue
                df = pd.read_csv(file)
                table=file.split('\\')[-1].split('.')[0].upper()
                print(table)
                success, nchunks, nrows, _ = write_pandas(self.cnn, df, table)
        self.cnn.close()
        print('done')
