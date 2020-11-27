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
import n0similarities as n0

def uploadToPSQL(host, username, password, database, port, table, judul, filePath, name, subjudul, dataheader, databody):
    try:
        for t in range(0, len(table)):
            rawstr = [tuple(x) for x in zip(dataheader, databody[t])]
            connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=database)
            cursor = connection.cursor()
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
            cursor.execute("SELECT * FROM information_schema.tables where table_name=%s", (table[t],))
            exist = bool(cursor.rowcount)
            
            if exist == True:    
                cursor.execute("DROP TABLE "+ table[t] + " CASCADE")
                cursor.execute("CREATE TABLE "+table[t]+" (index SERIAL, tanggal date, total varchar);")
            else:
                cursor.execute("CREATE TABLE "+table[t]+" (index SERIAL, tanggal date, total varchar);")
                
            cursor.execute('INSERT INTO '+table[t]+'(tanggal, total) values ' +str(rawstr)[1:-1])
            
        return True   
        
    except (Exception, psycopg2.Error) as error :
        return error
    
    finally:
        if(connection):
            cursor.close()
            connection.close()
            
def makeChart(host, username, password, db, port, table, judul, filePath, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, wilayah, tabledata):
    try:
        datarowsend = []
        for t in range(0, len(table)):
            connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=db)
            cursor = connection.cursor()
            postgreSQL_select_Query = "SELECT * FROM "+table[t]+" ORDER BY tanggal DESC LIMIT " + str(limitdata)
        
            cursor.execute(postgreSQL_select_Query)
            mobile_records = cursor.fetchall() 
            uid = []
            lengthx = []
            lengthy = []
            for row in mobile_records:
                uid.append(row[0])
                lengthx.append(row[1])
                lengthy.append(row[2])
            datarowsend.append(mobile_records)
            #bar
            style.use('ggplot')
       
            fig, ax = plt.subplots()
        
            ax.bar(uid, lengthy, align='center')
            ax.set_title(A2 + " " + wilayah[t])
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
        
            plt.title(A2 + " " + wilayah[t])
            plt.grid(True)
            l = io.BytesIO()
            plt.savefig(l, format='png', bbox_inches="tight")
            
            lineChart = base64.b64encode(l.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            #pie
            plt.title(A2 + " " + wilayah[t])
            plt.pie(lengthy, labels=lengthx, autopct='%1.1f%%', 
            shadow=True, startangle=90)
        
            plt.axis('equal')
            p = io.BytesIO()
            plt.savefig(p, format='png', bbox_inches="tight")
            
            pieChart = base64.b64encode(p.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            bardata = base64.b64decode(barChart)
            barname = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[t]+'-bar.png'
            with open(barname, 'wb') as f:
                f.write(bardata)
            
            linedata = base64.b64decode(lineChart)
            linename = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[t]+'-line.png'
            with open(linename, 'wb') as f:
                f.write(linedata)
                
            piedata = base64.b64decode(pieChart)
            piename = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[t]+'-pie.png'
            with open(piename, 'wb') as f:
                f.write(piedata)
        makeExcel(datarowsend, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, name, limitdata, table, wilayah)
        makePDF(datarowsend, judul, barChart, lineChart, pieChart, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, table, wilayah)        
        
    except (Exception, psycopg2.Error) as error :
        print (error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
    
def makePDF(datarow, judul, bar, line, pie, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, lengthPDF, table, wilayah):
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
    
    epw = pdf.w - 2*pdf.l_margin
    col_width = epw/(lengthPDF+1)
    
    pdf.ln(0.5)
    th = pdf.font_size
    
    pdf.cell(col_width, 2*th, str("Wilayah"), border=1, align='C')
    for row in datarow[1]:
        pdf.cell(col_width, 2*th, str(row[1]), border=1, align='C')
    pdf.ln(2*th)
    for w in range(0, len(table)):
        data=list(datarow[w])
    
        pdf.set_font('Times','B',10.0)
        pdf.set_font('Arial','',9)
        pdf.cell(col_width, 2*th, wilayah[w], border=1, align='C')
        for row in data:
            pdf.cell(col_width, 2*th, str(row[2]), border=1, align='C')
        pdf.ln(2*th)
        
    for s in range(0, len(table)):
        col = pdf.w - 2*pdf.l_margin
        pdf.ln(2*th)
        widthcol = col/3
        pdf.add_page()
        pdf.image('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[s]+'-bar.png', link='', type='',x=8, y=80, w=widthcol)
        pdf.set_xy(17.0, 144.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[s]+'-line.png', link='', type='',x=103, y=80, w=widthcol)
        pdf.set_xy(17.0, 144.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/img/'+name+''+table[s]+'-pie.png', link='', type='',x=195, y=80, w=widthcol)
        pdf.ln(4*th)
    
    pdf.output('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/pdf/'+A2+'.pdf', 'F')


def makeExcel(datarow, A2, B2, C2, D2, E2, F2, G2, H2, I2, J2, K2, name, limit, table, wilayah):
    workbook = xlsxwriter.Workbook('C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/excel/'+A2+'.xlsx')
    worksheet = workbook.add_worksheet('name')
    row1 = workbook.add_format({'border': 2, 'bold': 1})
    row2 = workbook.add_format({'border': 2})
    
    header = ["Wilayah", "Kategori","Region","Frekuensi","Unit","Sumber","Status","ID Seri","Kode SR","Tanggal Obs. Pertama","Tanggal Obs. Terakhir ","Waktu pembaruan terakhir"]
    
    for rowhead2 in datarow[0]:
        header.append(str(rowhead2[1]))   
    for col_num, data in enumerate(header):
        worksheet.write(0, col_num, data, row1)
            
    for w in range(0, len(table)):
        data=list(datarow[w])
        body = [wilayah[w], A2, B2, C2, D2, E2, F2, G2, H2, str(I2.date()), str(J2.date()), str(K2.date())]
        for rowbody2 in data:
            body.append(str(rowbody2[2]))
            
        for col_num, data in enumerate(body):
            worksheet.write(w+1, col_num, data, row2)
    
    workbook.close()

#START FROM THIS
filePathwilayah = 'C:/Users/ASUS/Documents/bappenas/CEIC/allwilayah.xlsx';

readexcelwilayah = pd.read_excel(filePathwilayah)
dfwilayah = list(readexcelwilayah.values)

readexcelwilayah.fillna(0)
allwilayah = []
tipewilayah = 'prov'
if tipewilayah == 'prov':
    for x in range(0, len(dfwilayah)):
        allwilayah.append(dfwilayah[x][1])
elif tipewilayah=='kabkot':
    for x in range(0, len(dfwilayah)):
        allwilayah.append(dfwilayah[x][3])
elif tipewilayah == 'kec':
    for x in range(0, len(dfwilayah)):
        allwilayah.append(dfwilayah[x][5])
elif tipewilayah == 'kel':
    for x in range(0, len(dfwilayah)):
        allwilayah.append(dfwilayah[x][7])

semuawilayah = list(set(allwilayah))

#ENTER IN HERE TO GET DATA SIMILAR WITH ALLWILAYAH.XLSX
name = "01. Produk Domestik Bruto (AA001-AA007)"
host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "ceic_pendapatannasional01"
judul = "Produk Domestik Bruto (AA001-AA007)"
subjudul = "Badan Perencanaan Pembangunan Nasional"
filePath = 'C:/Users/ASUS/Documents/bappenas/CEIC/01. Pendapatan Nasional/'+name+'.xlsx';
limitdata = int(8)
readexcel = pd.read_excel(filePath)
tabledata = []
wilayah = []
databody = []

df = list(readexcel.values)
head = list(readexcel)
body = list(df[0])
readexcel.fillna(0)
rangeawal = 1060
rangeakhir = 1070
rowrange = range(rangeawal, rangeakhir)

for x in rowrange:
    rethasil = 0
    big_w = 0
    for w in range(0, len(semuawilayah)):
        namawilayah = semuawilayah[w].lower().strip()      
        nama_wilayah_len = len(namawilayah)      
        hasil = n0.get_levenshtein_similarity(df[x][0].lower().strip()[nama_wilayah_len*-1:], namawilayah)      
        if hasil > rethasil:
            rethasil = hasil
            big_w = w
    wilayah.append(semuawilayah[big_w].capitalize())
    tabledata.append('produkdomestikbruto_'+semuawilayah[big_w].lower().replace(" ", "") + "" + str(x))
    testbody = []
    for listbody in df[x][11:]:
        if ~np.isnan(listbody) == False:
            testbody.append(str('0'))
        else:
            testbody.append(str(listbody))
    databody.append(testbody)

A2 = "(Setop Rilis)Produk Domestik Bruto SNA 1993 Non Migas"
B2 = df[rangeawal][1]
C2 = df[rangeawal][2]
D2 = df[rangeawal][3]
E2 = df[rangeawal][4]
F2 = df[rangeawal][5]
G2 = df[rangeawal][6]
H2 = df[rangeawal][7]
I2 = df[rangeawal][8]
J2 = df[rangeawal][9]
K2 = df[rangeawal][10]

dataheader = []
for listhead in head[11:]:
    dataheader.append(str(listhead))
   
sql = uploadToPSQL(host, username, password, database, port, tabledata, judul, filePath, name, subjudul, dataheader, databody)
if sql == True:
    makeChart(host, username, password, database, port, tabledata, judul, filePath, name, subjudul, A2, B2, C2, D2, E2, F2, G2, H2,I2, J2, K2, limitdata, wilayah, tabledata)
else:
    print(sql)