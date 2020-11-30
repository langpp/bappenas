import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from matplotlib import pyplot as plt
from matplotlib import style
from fpdf import FPDF
import io
import base64
import numpy as np
import xlsxwriter 

def uploadToPSQL(host, username, password, database, port, table, judul, filePath, name, subjudul, rawstr):
    try:
        connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=database)
        cursor = connection.cursor()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        cursor.execute("SELECT * FROM information_schema.tables where table_name=%s", (table,))
        exist = bool(cursor.rowcount)
        if exist == True:
            cursor.execute("DROP TABLE "+ table + " CASCADE")
            cursor.execute("CREATE TABLE "+table+" (index SERIAL, tanggal date, total varchar);")
        else:
            cursor.execute("CREATE TABLE "+table+" (index SERIAL, tanggal date, total varchar);")
            
        cursor.execute('INSERT INTO '+table+'(tanggal, total) values ' +str(rawstr)[1:-1])
        
        cursor.execute("SELECT * FROM "+table+" ORDER BY tanggal DESC")
        mobile_records = cursor.fetchall() 
        uid = []
        lengthx = []
        lengthy = []
        for row in mobile_records:
            uid.append(row[0])
            lengthx.append(row[1])
            if row[2] == "":
                lengthy.append(float(0))
            else:
                lengthy.append(float(row[2]))
        return True   
        
    except (Exception, psycopg2.Error) as error :
        return error
    
    finally:
        if(connection):
            cursor.close()
            connection.close()
            
def makeChart(host, username, password, db, port, table, judul, filePath, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, negara):
    try:
        connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=db)
        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT * FROM "+table+" ORDER BY tanggal DESC LIMIT " + str(limitdata)
    
        cursor.execute(postgreSQL_select_Query)
        mobile_records = cursor.fetchall() 
        uid = []
        lengthx = []
        lengthy = []
        for row in mobile_records:
            uid.append(row[0])
            lengthx.append(row[1])
            lengthy.append(row[2])
        
        #bar
        style.use('ggplot')
   
        fig, ax = plt.subplots()
    
        ax.bar(uid, lengthy, align='center')
        ax.set_title(judul)
        ax.set_ylabel('Total')
        ax.set_xlabel('Tanggal')
    
        ax.set_xticks(uid)
        ax.set_xticklabels((lengthx))
        b = io.BytesIO()
        plt.savefig(b, format='png', bbox_inches="tight")
        
        barChart = base64.b64encode(b.getvalue()).decode("utf-8").replace("\n", "")
        plt.show()
        
        #line
        plt.plot(lengthx, lengthy)
        plt.xlabel('Tanggal')
        plt.ylabel('Total')
    
        plt.title(judul)
        plt.grid(True)
        l = io.BytesIO()
        plt.savefig(l, format='png', bbox_inches="tight")
        
        lineChart = base64.b64encode(l.getvalue()).decode("utf-8").replace("\n", "")
        plt.show()
        
        #pie
        plt.title(judul)
        plt.pie(lengthy, labels=lengthx, autopct='%1.1f%%', 
        shadow=True, startangle=90)
    
        plt.axis('equal')
        p = io.BytesIO()
        plt.savefig(p, format='png', bbox_inches="tight")
        
        pieChart = base64.b64encode(p.getvalue()).decode("utf-8").replace("\n", "")
        plt.show()
        makeExcel(mobile_records, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, name, limitdata)
        makePDF(mobile_records, judul, barChart, lineChart, pieChart, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, negara)        
        
    except (Exception, psycopg2.Error) as error :
        print (error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
    
def makePDF(datarow, judul, bar, line, pie, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, lengthPDF, negara):
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
    
    pdf.set_font('Times','B',11.0) 
    pdf.ln(0.5)
    
    th1 = pdf.font_size
    
    pdf.cell(100, 2*th1, "Kategori", border=1, align='C')
    pdf.cell(177, 2*th1, A2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Region", border=1, align='C')
    pdf.cell(177, 2*th1, B2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Frekuensi", border=1, align='C')
    pdf.cell(177, 2*th1, C2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Unit", border=1, align='C')
    pdf.cell(177, 2*th1, D2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Sumber", border=1, align='C')
    pdf.cell(177, 2*th1, E2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Status", border=1, align='C')
    pdf.cell(177, 2*th1, F2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "ID Seri", border=1, align='C')
    pdf.cell(177, 2*th1, G2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Kode SR", border=1, align='C')
    pdf.cell(177, 2*th1, H2, border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Tanggal Obs. Pertama", border=1, align='C')
    pdf.cell(177, 2*th1, str(I2.date()), border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Tanggal Obs. Terakhir ", border=1, align='C')
    pdf.cell(177, 2*th1, str(J2.date()), border=1, align='C')
    pdf.ln(2*th1)
    pdf.cell(100, 2*th1, "Waktu pembaruan terakhir", border=1, align='C')
    pdf.cell(177, 2*th1, str(K2.date()), border=1, align='C')
    pdf.ln(2*th1)
    
    pdf.set_xy(17.0, 125.0)
    
    pdf.set_font('Times','B',11.0) 
    data=list(datarow)
    epw = pdf.w - 2*pdf.l_margin
    col_width = epw/(lengthPDF+1)
    
    pdf.ln(0.5)
    th = pdf.font_size
    
    pdf.cell(col_width, 2*th, str("Negara"), border=1, align='C')
    for row in data:
        pdf.cell(col_width, 2*th, str(row[1]), border=1, align='C')
    pdf.ln(2*th)
    
    pdf.set_font('Times','B',10.0)
    pdf.set_font('Arial','',9)
    pdf.cell(col_width, 2*th, negara, border=1, align='C')
    for row in data:
        pdf.cell(col_width, 2*th, str(row[2]), border=1, align='C')
    pdf.ln(2*th)
    
    bardata = base64.b64decode(bar)
    barname = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+'-bar.png'
    with open(barname, 'wb') as f:
        f.write(bardata)
    
    linedata = base64.b64decode(line)
    linename = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+'-line.png'
    with open(linename, 'wb') as f:
        f.write(linedata)
        
    piedata = base64.b64decode(pie)
    piename = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+'-pie.png'
    with open(piename, 'wb') as f:
        f.write(piedata)
    
    col = pdf.w - 2*pdf.l_margin
    widthcol = col/3
    pdf.image(barname, link='', type='',x=8, y=144, w=widthcol)
    pdf.set_xy(17.0, 144.0)
    col = pdf.w - 2*pdf.l_margin
    pdf.image(linename, link='', type='',x=103, y=144, w=widthcol)
    pdf.set_xy(17.0, 144.0)
    col = pdf.w - 2*pdf.l_margin
    pdf.image(piename, link='', type='',x=195, y=144, w=widthcol)
    pdf.ln(2*th)
    
    pdf.output('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/pdf/'+name+'.pdf', 'F')

def makeExcel(datarow, A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, name, limit):
    workbook = xlsxwriter.Workbook('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/excel/'+name+'.xlsx')
    worksheet = workbook.add_worksheet('name')
    row1 = workbook.add_format({'border': 2, 'bold': 1})
    row2 = workbook.add_format({'border': 2})
    data=list(datarow)
    header = ["Kategori","Region","Frekuensi","Unit","Sumber","Status","ID Seri","Kode SR","Tanggal Obs. Pertama","Tanggal Obs. Terakhir ","Waktu pembaruan terakhir"]
    body = [A2, B2, C2, D2, E2, F2, G2, H2, str(I2.date()), str(J2.date()), str(K2.date())]
    
    for rowhead2 in datarow:
        header.append(str(rowhead2[1]))   
        
    for rowbody2 in data:
        body.append(str(rowbody2[2]))
        
    for col_num, data in enumerate(header):
        worksheet.write(0, col_num, data, row1)
        
    for col_num, data in enumerate(body):
        worksheet.write(1, col_num, data, row2)
    
    workbook.close()

name = "01. Produk Domestik Bruto (AA001-AA007)"
host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "ceic_pendapatannasional01"
table = "produkdomestikbruto"
judul = "Produk Domestik Bruto (AA001-AA007)"
subjudul = "Badan Perencanaan Pembangunan Nasional"
filePath = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/'+name+'.xlsx';
limitdata = int(8)
negara = "Indonesia"

readexcel = pd.read_excel(filePath)
df = list(readexcel.values)
head = list(readexcel)
body = list(df[0])
readexcel.fillna(0)

A2 = body[0]
B2 = body[1]
C2 = body[2]
D2 = body[3]
E2 = body[4]
F2 = body[5]
G2 = body[6]
H2 = body[7]
I2 = body[8]
J2 = body[9]
K2 = body[10]
dataheader = []
for listhead in head[11:]:
    dataheader.append(str(listhead))
    
databody = []
for listbody in body[11:]:
    if ~np.isnan(listbody) == False:
        databody.append(str('0'))
    else:
        databody.append(str(listbody))
   
rawstr = [tuple(x) for x in zip(dataheader, databody)]
sql = uploadToPSQL(host, username, password, database, port, table, judul, filePath, name, subjudul, rawstr)
if sql == True:
    makeChart(host, username, password, database, port, table, judul, filePath, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, negara)
else:
    print(sql)