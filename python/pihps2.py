import pandas as pd
import psycopg2
from matplotlib import pyplot as plt
from matplotlib import style
from fpdf import FPDF
import io
import base64
import json
import requests
from datetime import timedelta, date
from sqlalchemy import create_engine
import xlsxwriter 
from sqlalchemy import create_engine

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def markettype(numofvalue):
    number = ['1','2','3','4']
    name = ['Pasar Tradisional','Pasar Moderen','Pedagang Besar','Produsen']
    out = dict(zip(number, name))
    return out[numofvalue]

def comodityname(numofvalue):
    number = ['1','2','3','4','5','6','7','8','9','10']
    name = ['Bawang Merah','Bawang Putih','Beras','Cabai Merah','Cabai Rawit','Daging Ayam','Daging Sapi','Gula Pasir','Minyak Goreng','Telur Ayam']
    out = dict(zip(number, name))
    return out[numofvalue]

def qualityname(numofvalue):
    number = ['20','1','2','3','4','5','6','17','18','19','88','16','14','15','27','39','7','8','9','16']
    name = ['Ukuran Sedang','Beras Kualitas Medium I','Beras Kualitas Medium II','Beras Kualitas Super I','Beras Kualitas Super II','Beras Kualitas Bawah I','Beras Kualitas Bawah II','Cabai Merah Besar','Cabai Merah Keriting','Cabai Rawit Hijau','Cabai Rawit Merah','Daging Ayam Ras Segar','Daging Sapi Kualitas 1','Daging Sapi Kualitas 2','Gula Pasir Lokal','Gula Pasir Kualitas Premium','Minyak Goreng Curah','Minyak Goreng Kemasan Bermerk 1','Minyak Goreng Kemasan Bermerk 2','Telur Ayam Ras Segar']
    out = dict(zip(number, name))
    return out[numofvalue]

def uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, province_id):
    headers = {
        "Authorization": "0bd7b7f5164e6497543c93db6080fa5ff90746467fc915438e6004524a86b743ae91abbe7bd376f4bcda49685c78ff52d122b00cc3437f0af182685d5d24aabdrhSH6PH2pSOdOtLsaNZV8SeQFrfLwmVbMCbmDqVBijQ8Q2Nlni7eWioxBbzo+LPg"
    }
    cnumber = ['1','2','3','4','5','6','7','8','9','10']
    qnumber = ['20','1','2','3','4','5','6','17','18','19','88','16','14','15','27','39','7','8','9','16']
    req1 = requests.get('http://api.bappenas.go.id/bus/api/domain/newpihps/getIntegrationMarkets?province_id='+province_id, headers=headers).json()
    df1 = pd.json_normalize(data = req1)
    result = df1.to_json(orient="table")
    parsed = json.loads(result)
    df2 = pd.json_normalize(data = parsed['data'], record_path = 'data')
    
    result1 = df2.to_json(orient="table")
    
    for cn in cnumber:
        for qn in qnumber:
            parsed1 = json.loads(result1)
            df3 = pd.json_normalize(data = parsed1['data'], meta =['market_id', 'market_desc', 'seller_count', 'price_type_id', 'region_id', 'region_desc'])
            df3 = df3[['market_id', 'market_desc', 'seller_count', 'price_type_id', 'region_id', 'region_desc']]
            
            df3.index = df3.index + 1
           
            df3['market_id'] = df3['market_id'].astype('int64')
            df3['commodity_id'] = cn
            df3['quality_id'] = qn
            df3
            uri = 'postgresql://'+username+':'+password+'@'+host+':'+port+'/'+database
            engine = create_engine(uri)
            
            #df3.to_sql(table, con = engine, if_exists='append')
    return True

def uploadToPSQLprice(host, username, password, database, port, table, table2, judul, name, subjudul, province_id):
    responses1 = []
    start_dt = date(2020, 11, 10)
    end_dt = date(2020, 11, 26)
    rangtanggalheader = str(start_dt) + ' - ' + str(end_dt)
    headers = {
        "Authorization": "0bd7b7f5164e6497543c93db6080fa5ff90746467fc915438e6004524a86b743ae91abbe7bd376f4bcda49685c78ff52d122b00cc3437f0af182685d5d24aabdrhSH6PH2pSOdOtLsaNZV8SeQFrfLwmVbMCbmDqVBijQ8Q2Nlni7eWioxBbzo+LPg"
    }

    for dt in daterange(start_dt, end_dt):   
        req1 = requests.get('http://api.bappenas.go.id/bus/api/domain/newpihps/getIntegrationprices?province_id='+province_id+'&period='+dt.strftime("%Y-%m-%d"), headers=headers).json()
        responses1.append(req1)
    
    df1 = pd.json_normalize(data = req1)
    result = df1.to_json(orient="table")
    parsed = json.loads(result)
    df2 = pd.json_normalize(data = parsed['data'], record_path = 'data')
    result1 = df2.to_json(orient="table")
    parsed1 = json.loads(result1)
    
    df3 = pd.json_normalize(data = parsed1['data'], record_path = 'details', meta =['market_id', 'date', 'inputted', 'validated'])
    df3 = df3[['date','inputted','validated','market_id','commodity_id','quality_id','price']]
    df3.index = df3.index + 1
    df3['market_id'] = df3['market_id'].astype('int64')
    df3
    
    uri = 'postgresql://'+username+':'+password+'@'+host+':'+port+'/'+database
    engine = create_engine(uri)
    
    #df3.to_sql(table2, con = engine,  if_exists='replace')
    makeChart(host, username, password, database, port, table, table2, judul, name, subjudul, rangtanggalheader)
    return True

def makeChart(host, username, password, db, port, table, table2, judul, name, subjudul, rangtanggalheader):
    try:        
        connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=db)
        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT DISTINCT a.market_id, a.commodity_id, a.quality_id, a.market_desc, a.price_type_id, a.region_id, a.region_desc FROM "+table+" AS a WHERE a.price_type_id != 1 ORDER BY a.market_id, a.commodity_id, a.quality_id ASC"
        
        cursor.execute(postgreSQL_select_Query)
        mobile_records = cursor.fetchall() 
        datapush = []
        
        for row in mobile_records:
            datasame = []
            #market_name = marketname(str(row[0]))
            postgreSQL_select_Query2 = "SELECT date, price, market_id, commodity_id, quality_id FROM "+table2+" WHERE market_id='"+str(row[0])+"' AND  commodity_id='"+str(row[1])+"' AND quality_id='"+str(row[2])+"' ORDER BY market_id, commodity_id, quality_id ASC"
            cursor.execute(postgreSQL_select_Query2)
            datarowarray = cursor.fetchall() 
                
            for rowdate in datarowarray:
                if str(row[0]) == str(rowdate[2]) and str(row[1]) == str(rowdate[3]) and str(row[2]) == str(rowdate[4]):
                    datasame.append(rowdate)
                    
            if datarowarray:
                comodity_name = comodityname(row[1])
                quality_name = qualityname(row[2])
                datapush.append(list([row[0], row[3], markettype(str(row[4])), row[5], row[6], comodity_name, quality_name, datasame]))
                uid = []
                lengthx = []
                lengthy = []
                i = 0
                for chartrow in datasame:
                    i += 1
                    lengthx.append(chartrow[0])
                    lengthy.append(chartrow[1])
                    uid.append(i)
                #bar
                style.use('ggplot')
           
                fig, ax = plt.subplots()
                
                ax.bar(uid, lengthy, align='center')
                ax.set_title(judul+' '+row[1]+' '+comodity_name+' '+quality_name)
                ax.set_ylabel('Price')
                ax.set_xlabel('Tanggal')
                ax.set_xticks(uid)
                ax.set_xticklabels((lengthx))
                b = io.BytesIO()
                plt.savefig(b, format='png', bbox_inches="tight")
                
                barChart = base64.b64encode(b.getvalue()).decode("utf-8").replace("\n", "")
                plt.show()
                
                #line
                plt.plot(lengthx, lengthy)
                plt.xlabel('Price')
                plt.ylabel('Total')
            
                plt.title(judul+' '+row[1]+' '+comodity_name+' '+quality_name)
                plt.grid(True)
                l = io.BytesIO()
                plt.savefig(l, format='png', bbox_inches="tight")
                
                lineChart = base64.b64encode(l.getvalue()).decode("utf-8").replace("\n", "")
                plt.show()
                
                #pie
                plt.title(judul+' '+row[1]+' '+comodity_name+' '+quality_name)
                plt.pie(lengthy, labels=lengthx, autopct='%1.1f%%', 
                shadow=True, startangle=90)
            
                plt.axis('equal')
                p = io.BytesIO()
                plt.savefig(p, format='png', bbox_inches="tight")
                
                pieChart = base64.b64encode(p.getvalue()).decode("utf-8").replace("\n", "")
                plt.show()
                
                bardata = base64.b64decode(barChart)
                barname = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+row[1]+' '+comodity_name+' '+quality_name+'-bar.png'
                with open(barname, 'wb') as f:
                    f.write(bardata)
                
                linedata = base64.b64decode(lineChart)
                linename = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+row[1]+' '+comodity_name+' '+quality_name+'-line.png'
                with open(linename, 'wb') as f:
                    f.write(linedata)
                    
                piedata = base64.b64decode(pieChart)
                piename = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+row[1]+' '+comodity_name+' '+quality_name+'-pie.png'
                with open(piename, 'wb') as f:
                    f.write(piedata)
        makePDF(rangtanggalheader, datapush, judul, name, subjudul)  
        makeExcel(rangtanggalheader, datapush, judul, name, subjudul)
            
        
    except (Exception, psycopg2.Error) as error :
        print (error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
    
def makePDF(rangtanggalheader, datarow, judul, name, subjudul):
    #print(datarow)
    pdf = FPDF()
    pdf = FPDF('L', 'mm', [210,297])
    pdf.add_page()
    pdf.set_font('helvetica', 'B', 20.0)
    pdf.set_xy(145.0, 15.0)
    pdf.cell(ln=0, h=2.0, align='C', w=10.0, txt=judul, border=0)
    
    pdf.set_font('arial', '', 14.0)
    pdf.set_xy(145.0, 25.0)
    pdf.cell(ln=0, h=2.0, align='C', w=10.0, txt=subjudul, border=0)
    pdf.line(10.0, 30.0, 287.0, 30.0)
    pdf.set_font('times', '', 10.0)
    pdf.set_xy(17.0, 37.0)
    
    pdf.set_font('Times','',10.0) 
    pdf.set_font('Times','B',10.0) 
    pdf.ln(0.5)
    th1 = pdf.font_size

    pdf.cell(100, 2*th1, "Tanggal", border=1, align='C')
    pdf.cell(177, 2*th1, rangtanggalheader, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Jenis Market", border=1, align='C')
    pdf.cell(177, 2*th1, 'Pasar Non Tradisional', border=1, align='C')
    pdf.ln(2*th1)
    
    pdf.set_xy(17.0, 55.0)
    pdf.set_font('Times','',10.0) 
    
    epw = pdf.w - 2*pdf.l_margin
    col_width = epw/2
    
    pdf.set_font('Times','B',8.0) 
    th = pdf.font_size
    pdf.ln(2*th)
    
    pdf.cell(20, 2*th, "Market ID", border=1, align='C')
    pdf.cell(60, 2*th, "Market", border=1, align='C')
    pdf.cell(50, 2*th, "Jenis Market", border=1, align='C')
    pdf.cell(40, 2*th, "Region ID", border=1, align='C')
    pdf.cell(40, 2*th, "Region", border=1, align='C')
    pdf.cell(40, 2*th, "Komoditas", border=1, align='C')
    pdf.cell(40, 2*th, "Kualitas", border=1, align='C')
    for dt in datarow[7]:
        pdf.cell(40, 2*th, dt[0], border=1, align='C')
    pdf.ln(2*th1)
    
    pdf.set_font('Arial','',7)
    th = pdf.font_size
    for data in datarow:
        pdf.cell(20, 2*th, str(data[0]), border=1, align='C')
        pdf.cell(60, 2*th, str(data[1]), border=1, align='C')
        pdf.cell(50, 2*th, str(data[2]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[3]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[4]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[5]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[6]), border=1, align='C')
        for dc in data[7]:
            pdf.cell(40, 2*th, str(dc[1]), border=1, align='C')
        pdf.ln(2*th)
        
    for datachart in datarow:
        pdf.add_page()
        col = pdf.w - 2*pdf.l_margin
        pdf.ln(2*th)
        widthcol = col/3
        pdf.image('C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+datachart[0][1]+' '+datachart[1]+' '+datachart[2]+'-bar.png', link='', type='',x=8, y=80, w=widthcol)
        pdf.set_xy(17.0, 144.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image('C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+datachart[0][1]+' '+datachart[1]+' '+datachart[2]+'-line.png', link='', type='',x=103, y=80, w=widthcol)
        pdf.set_xy(17.0, 144.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image('C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+datachart[0][1]+' '+datachart[1]+' '+datachart[2]+'-pie.png', link='', type='',x=195, y=80, w=widthcol)
        pdf.ln(4*th)
    
    pdf.output('C:/Users/ASUS/Documents/bappenas/PIHPS/pdf/'+name+ ' ' +rangtanggalheader+'.pdf', 'F')

def makeExcel(rangtanggalheader, datarow, judul, name, subjudul):
    workbook = xlsxwriter.Workbook('C:/Users/ASUS/Documents/bappenas/PIHPS/excel/'+name+ ' ' +rangtanggalheader+'.xlsx')
    worksheet = workbook.add_worksheet('name')
    row1 = workbook.add_format({'border': 2, 'bold': 1})
    row2 = workbook.add_format({'border': 2})
    w = 0
    header = ["Market ID","Market","Jenis Market","Region ID","Region","Komoditas","Kualitas"]
    
    for dt in datarow[7]:
        header.append(dt[0])   
    for col_num, data in enumerate(header):
        worksheet.write(0, col_num, data, row1)
            
    for data in datarow:
        w+=1
        body = [data[0], data[1], data[2], data[3], data[4], data[5], data[6]]
        for dc in data[7]:
            body.append(str(dc[1]))
            
        for col_num, data in enumerate(body):
            worksheet.write(w, col_num, data, row2)
    workbook.close()
name = "Pasar Non Tradisional"
host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "pihps"
table = "pihps_2"
table2 = "price_pihps_2"
judul = "Data Pasar Pasar Non Tradisional"
subjudul = "Badan Perencanaan Pembangunan Nasional"
province_id = '11'

sql = uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, province_id)
price = uploadToPSQLprice(host, username, password, database, port, table, table2, judul, name, subjudul, province_id)
if sql == True and price == True:
    print('Running PHIPS Script')
else:
    print(sql)
