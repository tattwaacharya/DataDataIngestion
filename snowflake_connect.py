import snowflake.connector
cnn = snowflake.connector.connect(user='HITESH'
                                  ,password='1234ABcd'
                                  ,account='qg60264.southeast-asia.azure'
                                  ,database='CONTROL_DB'
                                  ,warehouse='COMPUTE_WH'
                                  ,schema='LOAD_SCHEMA')
ddl_file ='snow_Dml_new.sql'
with open(ddl_file, "r", encoding="utf-8") as f:
    for cs in cnn.execute_stream(f):
        for rt in cs:
            print(rt)
cnn.close()
print('done')