import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
    newsid= df['news_id']
    newscode= df['news_code']
    newslink= df['news_link']
    newsdate= df['news_date']
    newstitle= df['news_title']
    newscontent = df['news_content'].str.replace('<[^<]+?>', ' ')
    arrayfind = ['cukai']
    wilayah = ""
    strfind = ', '.join(arrayfind)
    arraycontent = []
    datafix = [list(list(x) for x in zip(newsid, newscode, newslink, newsdate, newstitle, newscontent))]
    
    for newscontent in datafix[0]:
        x = newscontent[5].split('.')
        matching = [s for s in x if any(xs in s for xs in arrayfind)]
        if matching != []:
            if len(matching) > 1:
                for matcharray in matching:
                    arraycontent.append(tuple([matcharray, newscontent[0], newscontent[1], newscontent[2], newscontent[3], newscontent[4], strfind, wilayah]))
            else:
                arraycontent.append(tuple([str(matching)[2:-2], newscontent[0], newscontent[1], newscontent[2], newscontent[3], newscontent[4], strfind, wilayah]))
    rawstr = str(arraycontent)[1:-1]
    try:
        connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=database)
        cursor = connection.cursor()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        cursor.execute("SELECT * FROM information_schema.tables where table_name=%s", (table,))
        exist = bool(cursor.rowcount)
        if exist == True:
            cursor.execute("DROP TABLE "+ table + " CASCADE")
            cursor.execute("CREATE TABLE "+table+" (index SERIAL, news_id varchar, news_code varchar, news_link varchar, news_date date, news_title varchar, news_content varchar, keyword varchar, wilayah varchar);")
        else:
            cursor.execute("CREATE TABLE "+table+" (index SERIAL, news_id varchar, news_code varchar, news_link varchar, news_date date, news_title varchar, news_content varchar, keyword varchar, wilayah varchar);")
            
        cursor.execute('INSERT INTO '+table+'(news_content, news_id, news_code, news_link, news_date, news_title, keyword, wilayah) values ' +rawstr)
        
        return len(arraycontent)  
        
    except (Exception, psycopg2.Error) as error :
        return error
    
    finally:
        if(connection):
            cursor.close()
            connection.close()
            
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
if checkUpload != False:
    print("Success, Total Data : " + str(checkUpload))
else:
    print("Error When Upload CSV")