#import psycopg2
import pandas as pd 
import json
import requests
from pandas.io.json import json_normalize
import os
from datetime import datetime
import tracemalloc
import cx_Oracle

tracemalloc.start()
conn_info = {
    'host': '',
    'port': 1521,
    'user': '',
    'psw': '',
    'service': ''
}
connstr = '{user}/{psw}@{host}:{port}/{service}'.format(**conn_info)
conn = cx_Oracle.connect(connstr)
startTime = datetime.now()
retornoFim = pd.DataFrame()
df_merge_difkey = pd.DataFrame()
df_bd_invest = pd.read_sql_query("""SELECT distinct(COD_CUSTOMER) as ID FROM dbaad.ANALISE_AUDIT_INVEST_CONS""",con=conn)
#print(df_bd_invest.head())
#os.system("pause")

for index, linha in df_bd_invest.iterrows():
#for linha in df_bd_invest.itertuples(index=True): #for por tuplas
	#print(type(linha))
	#print(linha)
	#urlApi = (f"http://xxxx/xxxx/{linha.id}")#busca por tuple
	#urlApi = (f"http://xxxx/xxxx/{linha['ID']}")#busca por index
	urlApi = (f"http://xxxx/xxxx/{linha['ID']}")

	response = requests.get(urlApi, verify=False)
	post = json.loads(response.content)
	retorno = pd.json_normalize(post)


	try:
		retorno['dat_nasc'] = post['born_date']
	except:
		retorno['dat_nasc'] = ''

	for end in post['addresses']:
		try:
			retorno['rua'] = end['street']
		except:
			retorno['rua'] = ''
		try:
			retorno['numero'] = end['number']
		except:
			retorno['numero'] = ''
		try:	
			retorno['bairro'] = end['district']
		except:
			retorno['bairro'] = ''
		try:
			retorno['cidade'] = end['city']
		except:
			retorno['cidade'] = ''	
		try:
			retorno['uf'] = end['state']
		except:
			retorno['uf'] = ''
		try:
			retorno['cep'] = end['postal_code']
		except:
			retorno['cep'] = ''


	# lista_empty = []
	# try:
	dict_phone_empty = {}
	dict_ddd_empty = {}
	
	for i in post['phones']:
		if i['label'] == 'cellphone':
			keys = i['label']
			try:
				values = i['number']
			except:
				values = ''
			dict_phone_empty.update({'{key}'.format(key = keys):'{value}'.format(value = values)})

	for i in post['phones']:
		if i['label'] == 'homephone':
			keys = i['label']
			try:
				values = i['number']
			except:
				values = ''
			dict_phone_empty.update({'{key}'.format(key = keys):'{value}'.format(value = values)})

	for i in post['phones']:
		if i['label'] == 'cellphone':
			keys = i['label']
			try:
				values = i['area_code']
			except:
				values = ''
			dict_ddd_empty.update({'{key}'.format(key = keys):'{value}'.format(value = values)})
	# 		# print(dict_empty)
			
	for i in post['phones']:
		if i['label'] == 'homephone':
			keys = i['label']
			try:
				values = i['area_code']
			except:
				values = ''
			dict_ddd_empty.update({'{key}'.format(key = keys):'{value}'.format(value = values)})
	try:
		retorno['cellphone'] = dict_phone_empty['cellphone']
	except:
		retorno['cellphone'] = ''
	try:
		retorno['homephone'] = dict_phone_empty['homephone']
	except:
		retorno['homephone'] = ''
	try:
		retorno['ddd_cell'] = dict_ddd_empty['cellphone']
	except:
		retorno['ddd_cell'] = ''
	try:
		retorno['ddd_home'] = dict_ddd_empty['homephone']
	except:
		retorno['ddd_home'] = ''




	
	retorno = retorno[['id','type','document','dat_nasc','rua','numero','bairro','cidade','uf', 'cep','ddd_cell', 'cellphone', 'ddd_home', 'homephone']]
	retornoFim = pd.concat([retornoFim, retorno])


	df_bd_investx = pd.read_sql_query(f"""SELECT A.COD_CUSTOMER as "id", A.NUM_RG, A.DAT_RG, A.DES_RG_ISSUER, A.DES_RG_UF, A.DES_NATURALNESS, A.DES_STATE_BIRTH, A.NAM_MOTHERS_NAME, A.NUM_EQUITY_VALUE,  A.DAT_CREATION_APL , A.NAM_DESCRIPTION, A.COD_EXTERNAL, A.NUM_INITIAL_INVESTMENT_VALUE, A.NUM_PERCENTAGE FROM dbaad.ANALISE_AUDIT_INVEST_CONS A where  A.COD_CUSTOMER = '{linha['ID']}'""",con=conn) 
	df_merge_difkey = pd.concat([df_merge_difkey, df_bd_investx])


df_merge = retornoFim.merge(df_merge_difkey, how='inner' ,on='id')

	
df_merge.to_csv(f'arq_extract_audit_invest_{datetime.now():%Y-%m-%d}.csv',sep=';', index=False)	

print(datetime.now() - startTime)
# #seu_codigo()
current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
tracemalloc.stop()