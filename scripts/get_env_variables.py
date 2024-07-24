import os
os.environ['envn']='Dev'
os.environ['header']='True'
os.environ['inferSchema']='True'
os.environ['user']='root'
os.environ['Password']='0303'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
envn =os.environ['envn']

appName = 'snowpark'

current_dir =   os.getcwd()

src_olap =current_dir +'\src\olap'
src_oltp = current_dir +'\src\oltp'

account='baxkfzd-sw19303'
user='mani6301'
password='Mani6301'
database='PANDAS'
schema='PUBLIC'
warehouse='COMPUTE_WH'
role='ACCOUNTADMIN'

file_dirs=[current_dir +'\src\olap\*',current_dir +'\src\oltp\*']


