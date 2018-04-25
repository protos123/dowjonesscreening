import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
import os
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='172.18.35.22', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

df=pd.DataFrame([500509,500111,1230,500234],columns=['UserId'])
users=df['UserId'].tolist()
print users
empty=pd.DataFrame(columns=['User','Document', 'IdType','Telephone','City','Address','RelationshipName','Country'])
temp = []
for index in range(0, len(users)):
    user=users[index]
    #Bring document, document type, city, address, names, country from database
    result=cursor.execute("""SELECT usuario_id, documento, tipo_documento, telefonos, direccion_ciudad, 
                    concat_ws('', direccion, direccion_linea2, direccion_linea3) as direccion, nombres, pais
                    FROM pps.usuario 
                    WHERE usuario_id=%(user)s limit 10""",{'user':user})
    if cursor.rowcount>0:
            print ('encontrado')
            resultado=cursor.fetchone()
            temp.append(resultado)
    else:
            temp.append(pd.Series([np.nan]))
            print ('no se encontro')

query = pd.DataFrame(temp, columns=['User','Document', 'IdType','Telephone','City','Address','RelationshipName','Country'])

query.loc[-1] = ['usuarios', 'documentos', 'tipo' ,'telefono','ciudad','direccion','relacion','pais']
query.loc[-2] = ['1', '2', '3' ,'4','5','6','7','8']
query.index=query.index +1
query=query.sort_index()
query.drop(index,inplace=True)
print query