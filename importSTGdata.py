import pandas as pd
from sqlalchemy import create_engine

username = 'testusr'
password = "Inceptez%40123"#rather than @ i am mentioning %40
host = '34.63.107.222'
db = 'stgdb_bfsi'

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:3306/{db}")
#folder="C:\\dataset\\"
folder="D:\\Learning\\Cloud Engineering\\DWH Project\\Dataset\\dataset DWH\\"
table_list_dict = {
    "stg_accounts": folder+"accounts.csv",
    "stg_transactions": folder+"transactions.csv",
    "stg_payments": folder+"payments.csv",
    "stg_creditcard": folder+"creditcard.csv",
    "stg_loans": folder+"loans.csv",
    "stg_cust_profile": folder+"cust.csv",
    "stg_branches": folder+"branches.csv",
    "stg_employees": folder+"employee.csv"}

for table, file in table_list_dict.items():
    df = pd.read_csv(file)
    df.to_sql(table, con=engine, index=False, if_exists="replace")
    print(f"Rows loaded in the table {table}")
