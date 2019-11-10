import pandas as pd
import numpy as np
import psycopg2 as db
import logging
import sys
import datetime
import os
now=datetime.datetime.now()
today = datetime.date.today()
loggername='queries-'+str(today)+'.log'
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', filename=loggername, filemode='a+',level=logging.DEBUG)

#Connecting to DB for Merchant Information
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='address', password='sdasdasdsadsadas')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

logging.warning('==================== DJRC massive upload bot executed =====================')
logging.warning('Script triggered at %(now)s',{'now':now})

#Capturing the file with the merchants for analysis
path= os.getcwd()
files =os.listdir(path)
files_xlsx=['DJRCMerchants.xlsx']
logging.warning('Data captured. Excel succesfully read')
#Create dataframe and getting the date of the execution
df=pd.DataFrame()
today = datetime.date.today()
#Bring file into dataframe
for f in files_xlsx:
    data=pd.read_excel(f,'Hoja1')
    df = df.append(data)
#Eliminate duplicate users
df = df.drop_duplicates()
df.columns=['UserId']
df['CaseID']=df['UserId'].astype(str) + '_PayULatam_'+ str(today)
users=df['UserId'].tolist()

logging.warning('Number of merchants for screening : %(users)s', {'users':str(len(users))})
#List to save.warningrmation from queries

temp = []
for index in range(0, len(users)):
    user=users[index]
    #Bring document, document type, city, address, names, country from database
    result = cursor.execute("""SELECT usuario_id, documento, tipo_documento, telefonos, direccion_ciudad, 
                        concat_ws('', direccion, direccion_linea2, direccion_linea3) as direccion, nombres, pais
                        FROM pps.usuario 
                        WHERE usuario_id=%(user)s limit 10""", {'user': user})
    if cursor.rowcount > 0:
        resultado = cursor.fetchone()
        temp.append(resultado)
    else:
        temp.append(pd.Series([np.nan]))

logging.warning('Query finished. Information brought from PayU Databases')

#Create a dataframe with the.warningrmation from database
query = pd.DataFrame(temp, columns=['User','Document', 'IdType','Telephone','City','Address','RelationshipName','Country'])
#Replacing Countries according DJ regulations
query['Country'].replace({'AR':'Argentina','PE':'Peru','CL':'Chile','CO':'Colombia','MX':'Mexico','PA':'Panama',
                          'BR':'Brazil','PL':'Poland','US':'United States','IT':'Italy','EE':'Estonia','GB':'United Kingdom',
                          'ES':'Spain'},inplace=True)
#Replacing the IdType with the ones required by DJ
query['IdType'].replace({'CI':'National ID', 'CC':'National ID', 'CE':'National ID', 'ID':'National ID', 'DNI':'National ID',
               'DNIE':'National ID', 'DE':'National ID', 'RUN':'National ID', 'CPF':'National ID', 'CURP':'National ID',
               'RE':'National ID','CNPJ':'Others (Entity)', 'CUIL':'Others (Entity)', 'CUIT':'Others (Entity)',
               'EIN':'Others (Entity)', 'NIF':'Others (Entity)', 'NIT':'Others (Entity)', 'RFC':'Others (Entity)',
               'RIF':'Others (Entity)', 'RUC':'Others (Entity)', 'RUT':'Others (Entity)','IFE':'Others (Individual)',
               'IDC':'Others (Individual)','PP':'Passport No.','SSN':'Social Security No.'}, inplace=True)
#Concatenate previous dataframe.warningrmation with the.warningrmation from the queries
df=pd.concat([df,query], axis=1)

#Create additional Columns to complete the file
df['CaseName']=df['RelationshipName']
df['RelationShipID']=df['CaseID']
df['RelationshipType'] = np.where(df.IdType.isin(['National ID','Others(Individual)','Passport No.','Social Security No.']),'Individual', 'Entity')
df['First Name']= np.where(df['RelationshipType']== 'Individual', df['RelationshipName'], None)
df['RowAction']=str('Insert')
df['CaseOwner']=str('Liliana Rios')
df['Requestor']=str('Liliana Andrade Rios')
df['Phone']=str('+57 318 848 0913')
df['Email']=str('liliana.andrade@payulatam.com')
df['Priority']=str('Medium')
df['Segment']=str('Default Segment')
df['Comment']=str('AUTOMATED SCREENING PROCESS. PAYU LATAM')
df['CaseStatus']=str('Submitted')
df['MatchRelationship']=str('FALSE')
df['IsClient']=str('True')
df['Screening']=str('Active')
df['Service-DJRC']=str('yes')
df['Service-DJNews']=str('no')
#Create columns that have no.warningrmation or non relevant for the screening
Addons=pd.DataFrame(columns=['Middle Name', 'Surname', 'Gender', 'DoB', 'AlternativeName', 'Occupation', 'Notes1', 'Notes2',
                    'AssociationType', 'IndustrySector', 'DocumentLinks', 'AddressURL', 'State', 'PostalCode'])
df=pd.concat([df,Addons],axis=1)

#Reorganizing Columns to create file
df=df[['RowAction','CaseID','CaseName','CaseOwner','Requestor','Phone','Email','Priority','Segment','Comment','CaseStatus',
       'MatchRelationship','RelationshipType','RelationShipID','RelationshipName','First Name','Middle Name','Surname',
       'Gender','DoB','AlternativeName','Occupation','IdType','Document','Notes1','Notes2','AssociationType','IndustrySector',
       'IsClient','Screening','Priority','DocumentLinks','Country','Address','AddressURL','Telephone','City','State','PostalCode',
       'Service-DJRC','Service-DJNews']]
df2 = pd.DataFrame(columns=['RowAction','CaseID','CaseName','CaseOwner','Requestor','Phone','Email','Priority','Segment','Comment','CaseStatus',
       'MatchRelationship','RelationshipType','RelationShipID','RelationshipName','First Name','Middle Name','Surname',
       'Gender','DoB','AlternativeName','Occupation','IdType','Document','Notes1','Notes2','AssociationType','IndustrySector',
       'IsClient','Screening','Priority','DocumentLinks','Country','Address','AddressURL','Telephone','City','State','PostalCode',
       'Service-DJRC','Service-DJNews'])
df2.loc[0] = ['Row Action', 'Case ID', 'Case Name' , 'Case Owner', 'Requestor', 'Phone', 'Email', 'Priority',
              'Segment', 'Comment', 'Case Status', 'Match Relationship','Relationship Type', 'Relationship ID',
              'Relationship Name', ' First Name', 'Middle Name', 'Surname', 'Gender', 'Date of Birth','Alternative Name',
              'Occupation', 'Identification Type', 'Identification Value', 'Notes 1', 'Notes 2', 'Association Type',
              'Industry Sector', 'Is Client', 'Screening', 'Priority', 'Document Links', 'Country', 'Address Line',
              'Address URL', 'Phone', 'City', 'State', 'Postal Code', 'Service - DJ R&C', 'Service - DJ News']
df2.loc[1]=['investigation.inv_row_action.False.string','case.inv_request_id.False.string','case.inv_inv_name.False.string',
              'case.inv_investigator.False.string','case.inv_requestor_name.False.string','case.inv_requestor_phone.False.string',
              'case.inv_requestor_email.False.string','case.inv_priority.False.string','case.inv_queue.False.string',
              'case.inv_comment.False.string','case.inv_status.False.string','case.MatchEntity.False.bool',
              'entity.ent_ent_type.False.string','entity.client_ent_id.False.string','entity.ent_name.False.string',
              'entity.ent_first_name.False.string','entity.ent_middle_name.False.string','entity.ent_last_name.False.string',
              'entity.ent_gender.False.string','entity.ent_date_of_birth.False.date','entity.ent_original_script_name.True.string',
              'entity.ent_occupation.True.string','entity.ent_identification_type.True.string','entity.ent_identification_value.True.string',
              'entity.ent_notes_1.True.string','entity.ent_notes_2.True.string','entity.ent_association_type.True.string',
              'entity.inv_industry_v.True.string','entity.ie_is_client.False.bool','entity.ie_has_monitoring.False.string',
              'entity.ent_risk_level.False.string','entity.ent_doc_links.False.string','address.ent_address_country.False.string',
              'address.ent_address_address.False.string','address.ent_address_address_5.False.string','address.ent_address_address_6.False.string',
              'address.ent_address_city.False.string','address.ent_address_state.False.string','address.ent_address_zip.False.string',
              'service.165.False.string','service.44.False.string']
df=pd.concat([df2,df])
logging.warning('Dataframe created. Proceding to create file')
#Save file with the name DJRC and date itno excel
filename = 'bulk-import-PayULatam-' + str(today) + '.xlsx'
writer = pd.ExcelWriter(filename)
df.to_excel(writer,sheet_name='case', index=False, header=False)
writer.save()
logging.warning('Process Finished. File created with the name %(filename)s',{'filename':filename})
logging.warning('==================== DJRC massive upload bot finished. Please contact support for any issues with execution. =====================')
