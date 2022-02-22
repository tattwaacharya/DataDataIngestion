import json
import pandas as pd
from pathlib import Path
import sys
# import numpy as np
from simple_salesforce import Salesforce, SalesforceLogin, SFType


class SalesforceSrc:
    def __init__(self,objLst):
        self.username = 'tatwa@atos.com'
        self.password = 'Apple025#'
        self.token = 'wgg6DbaRnOu1oUyETZDFa1Lzo'
        self.domain = 'login'
        self.sf = Salesforce(username=self.username, password=self.password, security_token=self.token,
                             domain=self.domain)
        self.objLst = objLst

    # print(sf)
    # print(dir(sf))
    # def get_metadadata(objName):
    #     #obj = sf.objName
    #     obj_metadata = obj.describe()
    #     df_obj_md = pd.DataFrame(obj_metadata.get('fields'))
    #     return df_obj_md

    # get_metadadata('account')
    # df_account_md[['name','type','length']].to_csv('account_metadata.csv')
    # df_table = pd.DataFrame(sf.query("SELECT QualifiedApiName, Label, IsQueryable, IsDeprecatedAndHidden, IsCustomSetting FROM EntityDefinition")['records'])
    # df_table = pd.DataFrame(sf.query("SELECT QualifiedApiName, Label, IsQueryable, IsDeprecatedAndHidden, IsCustomSetting FROM EntityDefinition")['records'])
    # df_table.to_csv('entityDef.csv')
    # def GetObjLst(self):
    #     self.objLst = []
    #     for obj in self.sf.describe()["sobjects"]:
    #         self.objLst.append(obj["name"])
    #     print(self.objLst)
    #     self.objLst = ['AcceptedEventRelation', 'mycustobj__c', 'Contact', 'AccountCleanInfo']

    # objLst =['AcceptedEventRelation']
    def sfExtract(self,):
        with open("salesforce_DDL_new.sql", "w+", encoding="utf-8") as f:
            f.write('--salesforce metadata converted to snowflake ddls\n')
        f.close
        # with open("salesforce_extract.soql", "w+", encoding="utf-8") as f:
        #     f.write('--salesforce object query for full extract\n')
        #     f.close
        snow_ddls = ""
        soqls = ""

        # def to_json(input_ordered_dict):
        #    return loads(dumps(input_ordered_dict))
        for each in self.objLst:
            md = pd.DataFrame(SFType(each, self.sf.session_id, self.sf.sf_instance, self.sf.sf_version,
                                     self.sf.proxies).describe().get('fields'))
            #md.to_csv('metadata.csv')
            # print(md[['name','type','length']])
            # print(md.name[1])
            md.type.replace(
                {"anytype": "variant", "base64": "binary", "currency": "number",
                 "datacategorygroupreference": "varchar",
                 "datetime": "timestamp_ntz", "email": "varchar", "id": "varchar", "int": "number",
                 "multipicklist": "variant",
                 "percent": "number", "picklist": "variant", "reference": "varchar", "textarea": "string",
                 "url": "varchar", "complexvalue": "varchar", "address": "variant","phone":"number"}, inplace=True)
            var_col= list(md['name'][md.type == "variant"])
            fieldDatatype = ""
            fieldNames = ""
            for i, row in md.iterrows():
                fieldDatatype = fieldDatatype + """{columns},\n""".format(
                    columns=str(row['name']) + ' ' + str(row.type) + '(' + str(row.length) + ')')
                if str(row.type) == 'boolean' or str(row.type) == 'variant' or str(row.type)=='date' or str(row.type)=='number' or str(row.type) == 'double':
                    fieldDatatype = fieldDatatype.replace('(' + str(row.length) + '),\n', ',\n')
                fieldNames = fieldNames + """{columns} """.format(columns=str(row['name']) + ',')
            snow_ddls = snow_ddls + "\nCREATE OR REPLACE TABLE {tableName} (".format(
                tableName=each) + fieldDatatype.rstrip(',\n') + ");"
            soqls = "select " + fieldNames.rstrip(' ,') + " from {tableName}".format(tableName=each)
            print(soqls)
            print(var_col)
            response = self.sf.query_all(soqls)
            lstrecrds = response.get('records')
            df_records = pd.DataFrame(lstrecrds)
            print(df_records.keys())
            # df_records.drop('attributes',inplace=True,axis = 1)
            if df_records.empty == False:
                for var_col  in var_col:
                    # print(var_col)
                    df_records[var_col] = df_records[var_col].apply(json.dumps).apply(json.loads)
            dirPath = "C:\\Users\\a833122\\PycharmProjects\\DataIngestion\\"
            Path(dirPath + each).mkdir(parents=True, exist_ok=True)
            df_file = df_records.to_csv(dirPath + each + "\\" + each + ".csv")

            with open("salesforce_DDL_new.sql", "w+", encoding="utf-8") as f:
                f.write(snow_ddls)
            f.close


#    print(sf.str(x["name"]).describe())
# sf_data = sf.query_all("""SELECT Owner.Name, store_id__c,
# account_number__c, username__c, password__c, program_status__c, FROM Account
# WHERE program_status__c IN ('Live','Test')""")
# def main():
#     objlst = sys.argv[1]
#     A = SalesforceSrc(objlst)
#
#     try:
#         A.sfExtract()
#     except Exception as e:
#         raise Exception(e)
#
#
# if __name__ == "__main__":
#     main()