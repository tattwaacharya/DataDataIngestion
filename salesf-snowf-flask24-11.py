from flask import Flask, request, render_template, session, redirect, jsonify
import salesforce_connect
import numpy as np
import pandas as pd
from flask import Flask, request, render_template, session, redirect
import numpy as np
import pandas as pd
from flask import Flask, render_template
import json
import pandas as pd
from simple_salesforce import Salesforce,SalesforceLogin,SFType
import os


app = Flask(__name__)
app.secret_key = "super secret key"
username = 'tatwa@atos.com'
password = 'Apple025#'
token = 'wgg6DbaRnOu1oUyETZDFa1Lzo'
domain = 'login'
sf=Salesforce(username=username, password=password ,security_token = token, domain = domain)
#print(sf)
#print(dir(sf))
#account = sf.account
#account_metadata = account.describe()
#df_account_md = pd.DataFrame(account_metadata.get('fields'))
#df_sf=df_account_md[['name','type','length']]
#df_account_md[['name','type','length']].to_csv('account_metadata.csv')

#@app.route('/', methods=("POST", "GET"))
#def html_table():
table_names = []
objdf = pd.DataFrame(sf.describe()["sobjects"])
#print(objdf.keys())
objdf=objdf[(objdf['queryable'] == True) & (objdf['createable']== True) & (objdf['replicateable']== True)]
#objdf=objdf[(objdf['custom']== False)|]
objdf.to_csv('objects.csv')
table_names = objdf['name'].sort_values(ascending=False).tolist()
#for x in objdf:
    
    #table_names = table_names +'\n' + x["name"]
    #table_names.append(x["name"])
#print(table_names)

@app.route('/', methods=["GET", "POST"])
def your_view():
    parser = Parser()
    if request.method == 'POST':
        
        data = request.json
        #print(data)
        data=parser.obj_data(data)
        #print(data)
        session['data'] = data
        
        #exec(open("extract.py").read())
        return jsonify(data)
        
    return render_template('tables13.html', table_names=table_names)

@app.route('/page2', methods=["GET", "POST"])
def page2():
    data = session.get('data', None)
    
    #print("second func: ",data)
    #print(type(data))
    #exec(open("extract.py").read())
    sf = salesforce_connect.SalesforceSrc
    sf(data).sfExtract()
    
    return render_template('page2.html',data=data)

@app.route('/page3', methods=["GET", "POST"])
def page3():
    
    print("Inside page3")
     
    exec(open("snowflake_connect.py").read())
    
    return render_template('page3.html')



class Parser():
    def obj_data(self,data):
        print("data in obj_func: ",data)
        return data
if __name__ == '__main__':
    app.run()