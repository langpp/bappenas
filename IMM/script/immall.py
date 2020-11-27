import pandas as pd
from sqlalchemy import create_engine
import os
#import nltk
#nltk.download()

def uploadToPSQL(columns, table, filePath, engine):
    df = pd.read_csv(
        os.path.abspath(filePath),
        names=columns,
        skiprows=1, 
        header=None,
        keep_default_na=False,
        encoding='utf-8'
    )
    df.fillna('')
    df['news_content']= df['news_content'].str.replace('<[^<]+?>', '') 
    print(df)
    df.to_sql(
        table, 
        engine,
        if_exists='replace',
        index=False
    )

    #if len(df) == 0:
        #return False
    #else:
        #return True
   
    
columns = [
    "news_id",
    "news_code",
    "news_type",
    "news_link",
    "media_displayName",
    "news_date",
    "news_title",
    "news_content",
    "news_image",
    "news_urlimage",
    "news_created_date",
]

host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "imm"
table = 'newsimm'
filePath = 'C:/Users/ASUS/Documents/bappenas/IMM/news.csv';
engine = create_engine('postgresql://'+username+':'+password+'@'+host+':'+port+'/'+database)

checkUpload = uploadToPSQL(columns, table, filePath, engine)
#if checkUpload == True:
    #print('Success')
#else:
    #print("Error When Upload CSV")