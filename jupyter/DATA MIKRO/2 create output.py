import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from matplotlib import pyplot as plt
from matplotlib import style
from fpdf import FPDF
import math
import io
import base64
import xlsxwriter 

def uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, databodyr):
    try:
        #Script sebenarnya dikomen dlu soalny leg kalau dijalanin
        #for t in range(0, len(table)):
        for t in range(0, 5):
            connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=database)
            cursor = connection.cursor()
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
            cursor.execute("SELECT * FROM information_schema.tables where table_name=%s", (table[t].lower(),))
            exist = bool(cursor.rowcount)
            if exist == True:    
                cursor.execute("DROP TABLE "+ table[t] + " CASCADE")
                cursor.execute("CREATE TABLE "+table[t]+" (index SERIAL, id_desa varchar, kode_provinsi varchar, nama_provinsi varchar, kode_kabkot varchar, nama_kabkot varchar, kode_kecamatan varchar, nama_kecamatan varchar, kode_desa varchar, nama_desa varchar, tahun varchar, nilai varchar);")
            else:
                cursor.execute("CREATE TABLE "+table[t]+" (index SERIAL, id_desa varchar, kode_provinsi varchar, nama_provinsi varchar, kode_kabkot varchar, nama_kabkot varchar, kode_kecamatan varchar, nama_kecamatan varchar, kode_desa varchar, nama_desa varchar, tahun varchar, nilai varchar);")

            cursor.execute('INSERT INTO '+table[t]+'(id_desa, kode_provinsi, nama_provinsi, kode_kabkot, nama_kabkot, kode_kecamatan, nama_kecamatan, kode_desa, nama_desa, tahun, nilai) values ' +str(databodyr[t])[1:-1])
            
        return True   
        
    except (Exception, psycopg2.Error) as error :
        return error
    
    finally:
        if(connection):
            cursor.close()
            connection.close()
    
def makeChart(host, username, password, db, port, table, judul, name, subjudul, basepath):
    try:
        datarowsend = []
        for t in range(0, 5):
            connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=db)
            cursor = connection.cursor()
            postgreSQL_select_Query = "SELECT index, tahun, nilai, nama_desa, id_desa, kode_provinsi, nama_provinsi, kode_kabkot, nama_kabkot, kode_kecamatan, nama_kecamatan, kode_desa FROM "+table[t]+" ORDER BY tahun ASC"
        
            cursor.execute(postgreSQL_select_Query)
            mobile_records = cursor.fetchall() 
            uid = []
            lengthx = []
            lengthy = []
            wilayah = []
            for row in mobile_records:
                uid.append(row[0])
                lengthx.append(row[1])
                lengthy.append(row[2])
                wilayah.append(row[3])
            datarowsend.append(mobile_records)
            #bar
            style.use('ggplot')
       
            fig, ax = plt.subplots()
            ax.bar(uid, lengthy, align='center')
            ax.set_title(judul + " " + wilayah[0] + ' ' + table[t])
            ax.set_ylabel('Nilai')
            ax.set_xlabel('Tahun')
        
            ax.set_xticks(uid)
            ax.set_xticklabels((lengthx))
            b = io.BytesIO()
            plt.savefig(b, format='png', bbox_inches="tight")
            
            barChart = base64.b64encode(b.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            #line
            plt.plot(lengthx, lengthy)
            plt.xlabel('Nilai')
            plt.ylabel('Tahun')
        
            plt.title(judul + " Desa " + wilayah[0] + ' ' + table[t])
            plt.grid(True)
            l = io.BytesIO()
            plt.savefig(l, format='png', bbox_inches="tight")
            
            lineChart = base64.b64encode(l.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            #pie
            plt.title(judul + " " + wilayah[0] + ' ' + table[t])
            plt.pie(lengthy, labels=lengthx, autopct='%1.1f%%', 
            shadow=True, startangle=90)
        
            plt.axis('equal')
            p = io.BytesIO()
            plt.savefig(p, format='png', bbox_inches="tight")
            
            pieChart = base64.b64encode(p.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            bardata = base64.b64decode(barChart)
            barname = basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-bar.png'
            with open(barname, 'wb') as f:
                f.write(bardata)
            
            linedata = base64.b64decode(lineChart)
            linename = basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-line.png'
            with open(linename, 'wb') as f:
                f.write(linedata)
                
            piedata = base64.b64decode(pieChart)
            piename = basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-pie.png'
            with open(piename, 'wb') as f:
                f.write(piedata)
        makeExcel(datarowsend, judul, name, basepath, table)
        makePDF(datarowsend, judul, name, subjudul, basepath, table)        
        
    except (Exception, psycopg2.Error) as error :
        print (error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
    
def makePDF(datarowsend, judul, name, subjudul, basepath, table):
    for t in range(0, len(datarowsend)):
        pdf = FPDF('L', 'mm', [210,297])
        pdf.add_page()
        pdf.set_font('helvetica', 'B', 20.0)
        pdf.set_xy(145.0, 15.0)
        pdf.cell(ln=0, h=2.0, align='C', w=10.0, txt=judul + ' Desa ' + datarowsend[t][0][3] + ' ' + table[t] , border=0)
       
        pdf.set_font('arial', '', 14.0)
        pdf.set_xy(145.0, 25.0)
        pdf.cell(ln=0, h=2.0, align='C', w=10.0, txt=subjudul, border=0)
        pdf.line(10.0, 30.0, 287.0, 30.0)
        pdf.set_font('times', '', 10.0)
        pdf.set_xy(17.0, 37.0)
        pdf.set_font('Times','',10.0) 
        pdf.set_font('Times','B',12.0) 
        pdf.ln(0.5)
        th1 = pdf.font_size
        pdf.cell(100, 2*th1, "ID Desa", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][4], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Kode Provinsi", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][5], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Nama Provinsi", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][6], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Kode Kabupaten / Kota", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][7], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Nama Kabupaten / Kota", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][8], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Kode Kecamatan", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][9], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Nama Kecamatan", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][10], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Kode Desa", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][11], border=1, align='C')
        pdf.ln(2*th1)
        pdf.cell(100, 2*th1, "Nama Desa", border=1, align='C')
        pdf.cell(177, 2*th1, datarowsend[t][0][3], border=1, align='C')
        pdf.ln(2*th1)
        
        pdf.set_xy(17.0, 115.0)
        
        pdf.set_font('Times','B',11.0) 
        epw = pdf.w - 2*pdf.l_margin
        col_width = epw/4
        
        pdf.ln(0.5)
        th = pdf.font_size
        
        for row in datarowsend[t]:
            pdf.cell(col_width, 2*th, str(row[1]), border=1, align='C')
        pdf.ln(2*th)
        
        pdf.set_font('Times','B',10.0)
        pdf.set_font('Arial','',9)
        for row in datarowsend[t]:
            pdf.cell(col_width, 2*th, str(row[2]), border=1, align='C')
        pdf.ln(2*th)
        
        pdf.set_xy(17.0, 137.0)
        col = pdf.w - 2*pdf.l_margin
        widthcol = col/3
        pdf.image(basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-bar.png', link='', type='',x=8, y=137, w=widthcol)
        pdf.set_xy(17.0, 75.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image(basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-line.png', link='', type='',x=103, y=137, w=widthcol)
        pdf.set_xy(17.0, 75.0)
        col = pdf.w - 2*pdf.l_margin
        pdf.image(basepath+'jupyter/Datamikro - Podes/img/'+name+' '+table[t]+'-pie.png', link='', type='',x=195, y=137, w=widthcol)
        pdf.ln(2*th)
        
        pdf.output(basepath+'jupyter/Datamikro - Podes/pdf/'+name+' '+table[t]+'.pdf', 'F')
    
def makeExcel(datarowsend, judul, name, basepath, table):
    for t in range(0, len(datarowsend)):
        workbook = xlsxwriter.Workbook(basepath+'jupyter/Datamikro - Podes/excel/'+name+' '+table[t]+'.xlsx')
        worksheet = workbook.add_worksheet('sheet1')
        row1 = workbook.add_format({'border': 2, 'bold': 1})
        row2 = workbook.add_format({'border': 2})
        body = [datarowsend[t][0][4], datarowsend[t][0][5], datarowsend[t][0][6], datarowsend[t][0][7], datarowsend[t][0][8], datarowsend[t][0][9], datarowsend[t][0][10], datarowsend[t][0][11], datarowsend[t][0][3]] 
        header = ["ID Desa ", "Kode Provinsi","Nama Provinsi ","Kode Kabupaten / Kota ","Nama Kabupaten / Kota ","Kode Kecamatan","Nama Kecamatan ","Kode Desa","Nama Desa"]
    
        for rowhead2 in datarowsend[t]:
            header.append(str(rowhead2[1]))   
        for col_num, data in enumerate(header):
            worksheet.write(0, col_num, data, row1)
                
        for rowhead1 in datarowsend[t]:
            body.append(str(rowhead1[2]))   
        for col_num, data in enumerate(body):
            worksheet.write(1, col_num, data, row2)
        
        workbook.close()
    
name = "Data Mikro"
host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "datamikro"
table = []
judul = "Data Mikro Indonesia"
subjudul = "Badan Perencanaan Pembangunan Nasional"
data2004 = []
data2006 = []
data2008 = []
data2011 = []
basepath = "C:/Users/ASUS/Documents/bappenas/"
file2004 = basepath + "data mentah/Datamikro - Podes/2004-data format 2011_coding.xlsx"
file2006 = basepath + "data mentah/Datamikro - Podes/2006-data format 2011_coding.xlsx"
file2008 = basepath + "data mentah/Datamikro - Podes/2008-data format 2011_coding.xlsx"
file2011 = basepath + "data mentah/Datamikro - Podes/2011-data_coding.xlsx"

readexcel2004 = pd.read_excel(file2004)
readexcel2006 = pd.read_excel(file2006)
readexcel2008 = pd.read_excel(file2008)
readexcel2011 = pd.read_excel(file2011)

df2004 = list(readexcel2004.values)
head2004 = list(readexcel2004)

df2006 = list(readexcel2006.values)
head2006 = list(readexcel2006)

df2008 = list(readexcel2008.values)
head2008 = list(readexcel2008)

df2011 = list(readexcel2011.values)
head2011 = list(readexcel2011)

for headertitle in head2011[9:]:
    for dfisi in df2011:
        table.append(str(headertitle)+'_'+str(dfisi[0]))

for content in df2004:
    data2004.append(content[9:])

for content in df2006:
    data2006.append(content[9:])

for content in df2008:
    data2008.append(content[9:])

for content in df2011:
    data2011.append(content[9:])

namadaerah = []
for content in df2011:
    namadaerah.append(content[:9])
#print(namadaerah)
databodyr = []
for d2 in range(0, len(data2011)):
    for r2 in range(0, len(data2011[d2])):
        if r2 < len(data2004[d2]):
            if math.isnan(data2004[d2][r2]):
                r2004 = 0
            else:
                r2004 = data2004[d2][r2]
        else:
            r2004 = 0
        
        if r2 < len(data2006[d2]):
            if math.isnan(data2006[d2][r2]):
                r2006 = 0
            else:
                r2006 = data2006[d2][r2]
        else:
            r2006 = 0
            
        if r2 < len(data2008[d2]):
            if math.isnan(data2008[d2][r2]):
                r2008 = 0
            else:
                r2008 = data2008[d2][r2]
        else:
            r2008 = 0
        
        if r2 < len(data2011[d2]):
            if math.isnan(data2011[d2][r2]):
                r2011 = 0
            else:
                r2011 = data2011[d2][r2]
        else:
            r20011 = 0
        databodyr.append(list([tuple([namadaerah[d2][0], namadaerah[d2][1], namadaerah[d2][2], namadaerah[d2][3], namadaerah[d2][4], namadaerah[d2][5], namadaerah[d2][6], namadaerah[d2][7], namadaerah[d2][8], '2004', r2004]), tuple([namadaerah[d2][0], namadaerah[d2][1], namadaerah[d2][2], namadaerah[d2][3], namadaerah[d2][4], namadaerah[d2][5], namadaerah[d2][6], namadaerah[d2][7], namadaerah[d2][8], '2006', r2006]), tuple([namadaerah[d2][0], namadaerah[d2][1], namadaerah[d2][2], namadaerah[d2][3], namadaerah[d2][4], namadaerah[d2][5], namadaerah[d2][6], namadaerah[d2][7], namadaerah[d2][8], '2008', r2008]), tuple([namadaerah[d2][0], namadaerah[d2][1], namadaerah[d2][2], namadaerah[d2][3], namadaerah[d2][4], namadaerah[d2][5], namadaerah[d2][6], namadaerah[d2][7], namadaerah[d2][8], '2011', data2011[d2][r2]])]))

sql = uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, databodyr)
if sql == True:
    makeChart(host, username, password, database, port, table, judul, name, subjudul, basepath)
else:
    print(sql)