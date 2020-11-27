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

def marketname(numofvalue):
    number = ['1101', '1102', '1103', '1104', '1105', '1106', '1107', '1108', '1109', '1110', '1111', '1112', '1113', '1114', '1115', '1116', '1117', '1118', '1171', '1172', '1173', '1174', '1175', '1201', '1202', '1203', '1204', '1205', '1206', '1207', '1208', '1209', '1210', '1211', '1212', '1213', '1214', '1215', '1216', '1217', '1218', '1219', '1220', '1221', '1222', '1223', '1224', '1225', '1271', '1272', '1273', '1274', '1275', '1276', '1277', '1278', '1301', '1302', '1303', '1304', '1305', '1306', '1307', '1308', '1309', '1310', '1311', '1312', '1371', '1372', '1373', '1374', '1375', '1376', '1377', '1401', '1402', '1403', '1404', '1405', '1406', '1407', '1408', '1409', '1410', '1471', '1473', '1501', '1502', '1503', '1504', '1505', '1506', '1507', '1508', '1509', '1571', '1572', '1601', '1602', '1603', '1604', '1605', '1606', '1607', '1608', '1609', '1610', '1611', '1612', '1613', '1671', '1672', '1673', '1674', '1701', '1702', '1703', '1704', '1705', '1706', '1707', '1708', '1709', '1771', '1801', '1802', '1803', '1804', '1805', '1806', '1807', '1808', '1809', '1810', '1811', '1812', '1813', '1871', '1872', '1901', '1902', '1903', '1904', '1905', '1906', '1971', '2101', '2102', '2103', '2104', '2105', '2171', '2172', '3101', '3171', '3172', '3173', '3174', '3175', '3201', '3202', '3203', '3204', '3205', '3206', '3207', '3208', '3209', '3210', '3211', '3212', '3213', '3214', '3215', '3216', '3217', '3218', '3271', '3272', '3273', '3274', '3275', '3276', '3277', '3278', '3279', '3301', '3302', '3303', '3304', '3305', '3306', '3307', '3308', '3309', '3310', '3311', '3312', '3313', '3314', '3315', '3316', '3317', '3318', '3319', '3320', '3321', '3322', '3323', '3324', '3325', '3326', '3327', '3328', '3329', '3371', '3372', '3373', '3374', '3375', '3376', '3401', '3402', '3403', '3404', '3471', '3501', '3502', '3503', '3504', '3505', '3506', '3507', '3508', '3509', '3510', '3511', '3512', '3513', '3514', '3515', '3516', '3517', '3518', '3519', '3520', '3521', '3522', '3523', '3524', '3525', '3526', '3527', '3528', '3529', '3571', '3572', '3573', '3574', '3575', '3576', '3577', '3578', '3579', '3601', '3602', '3603', '3604', '3671', '3672', '3673', '3674', '5101', '5102', '5103', '5104', '5105', '5106', '5107', '5108', '5171', '5201', '5202', '5203', '5204', '5205', '5206', '5207', '5208', '5271', '5272', '5301', '5302', '5303', '5304', '5305', '5306', '5307', '5308', '5309', '5310', '5311', '5312', '5313', '5314', '5315', '5316', '5317', '5318', '5319', '5320', '5321', '5371', '6101', '6102', '6103', '6104', '6105', '6106', '6107', '6108', '6109', '6110', '6111', '6112', '6171', '6172', '6201', '6202', '6203', '6204', '6205', '6206', '6207', '6208', '6209', '6210', '6211', '6212', '6213', '6271', '6301', '6302', '6303', '6304', '6305', '6306', '6307', '6308', '6309', '6310', '6311', '6371', '6372', '6401', '6402', '6403', '6404', '6405', '6409', '6411', '6471', '6472', '6474', '6501', '6502', '6503', '6504', '6571', '7101', '7102', '7103', '7104', '7105', '7106', '7107', '7108', '7109', '7110', '7111', '7171', '7172', '7173', '7174', '7201', '7202', '7203', '7204', '7205', '7206', '7207', '7208', '7209', '7210', '7211', '7212', '7271', '7301', '7302', '7303', '7304', '7305', '7306', '7307', '7308', '7309', '7310', '7311', '7312', '7313', '7314', '7315', '7316', '7317', '7318', '7322', '7325', '7326', '7371', '7372', '7373', '7401', '7402', '7403', '7404', '7405', '7406', '7407', '7408', '7409', '7410', '7411', '7412', '7413', '7414', '7415', '7471', '7472', '7501', '7502', '7503', '7504', '7505', '7571', '7601', '7602', '7603', '7604', '7605', '7606', '8101', '8102', '8103', '8104', '8105', '8106', '8107', '8108', '8109', '8171', '8172', '8201', '8202', '8203', '8204', '8205', '8206', '8207', '8208', '8271', '8272', '9101', '9102', '9103', '9104', '9105', '9106', '9107', '9108', '9109', '9110', '9111', '9112', '9171', '9401', '9402', '9403', '9404', '9408', '9409', '9410', '9411', '9412', '9413', '9414', '9415', '9416', '9417', '9418', '9419', '9420', '9426', '9427', '9428', '9429', '9430', '9431', '9432', '9433', '9434', '9435', '9436', '9471']
    name = "ref_kabkot"
    filePath = 'C:/Users/ASUS/Documents/bappenas/PIHPS/'+name+'.xlsx';
    
    readexcel = pd.read_excel(filePath)
    df = list(readexcel.values)
    readexcel.fillna(0)
    listpasar = []
    for listdata in range(0, len(df)):
        listpasar.append(list([str(df[listdata][0]), str(df[listdata][1])]))
    out = dict(zip(number, listpasar))
    return out[numofvalue]

def uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, province_id):
    headers = {
        "Authorization": "0bd7b7f5164e6497543c93db6080fa5ff90746467fc915438e6004524a86b743ae91abbe7bd376f4bcda49685c78ff52d122b00cc3437f0af182685d5d24aabdrhSH6PH2pSOdOtLsaNZV8SeQFrfLwmVbMCbmDqVBijQ8Q2Nlni7eWioxBbzo+LPg"
    }

    req1 = requests.get('http://api.bappenas.go.id/bus/api/domain/newpihps/getIntegrationMarkets?province_id='+province_id, headers=headers).json()
    df1 = pd.json_normalize(data = req1)
    result = df1.to_json(orient="table")
    parsed = json.loads(result)
    df2 = pd.json_normalize(data = parsed['data'], record_path = 'data')
    result1 = df2.to_json(orient="table")
    parsed1 = json.loads(result1)
    df3 = pd.json_normalize(data = parsed1['data'], meta =['market_id', 'market_desc', 'seller_count', 'price_type_id', 'region_id', 'region_desc'])
    df3 = df3[['market_id', 'market_desc', 'seller_count', 'price_type_id', 'region_id', 'region_desc']]
    df3.index = df3.index + 1
    df3['market_id'] = df3['market_id'].astype('int64')
    df3
    uri = 'postgresql://'+username+':'+password+'@'+host+':'+port+'/'+database
    engine = create_engine(uri)
    
    df3.to_sql(table, con = engine, if_exists='replace')
    return True

def uploadToPSQLprice(host, username, password, database, port, table, table2, judul, name, subjudul):
    responses1 = []
    start_dt = date(2017, 8, 19)
    end_dt = date(2017, 8, 30)
    province_id = '32'
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
    
    df3.to_sql(table2, con = engine,  if_exists='replace')
    makeChart(host, username, password, database, port, table, table2, judul, name, subjudul, rangtanggalheader)
    return True

def makeChart(host, username, password, db, port, table, table2, judul, name, subjudul, rangtanggalheader):
    try:        
        connection = psycopg2.connect(user=username,password=password,host=host,port=port,database=db)
        cursor = connection.cursor()
        postgreSQL_select_Query = "SELECT b.market_desc, b.price_type_id, b.region_desc, a.market_id, a.commodity_id, a.quality_id FROM "+table2+" AS a JOIN "+table+" AS b ON a.market_id = b.market_id WHERE b.price_type_id != 1 GROUP BY b.market_desc, b.price_type_id, b.region_desc, a.market_id, a.commodity_id, a.quality_id ORDER BY market_id ASC LIMIT 3"
        
        cursor.execute(postgreSQL_select_Query)
        mobile_records = cursor.fetchall() 
        print(mobile_records)
        datapush = []
        for row in mobile_records:
            datasame = []
            market_name = marketname(str(row[0]))
            comodity_name = comodityname(str(row[1]))
            quality_name = qualityname(str(row[2]))
            postgreSQL_select_Query = "SELECT date, price, market_id, commodity_id, quality_id FROM "+table2+" WHERE commodity_id='"+str(row[1])+"' AND quality_id='"+str(row[2])+"' AND market_id='"+str(row[0])+"' ORDER BY market_id ASC LIMIT 3"
            cursor.execute(postgreSQL_select_Query)
            datarowarray = cursor.fetchall() 
            
            for rowdate in datarowarray:
                if str(row[0]) == str(rowdate[2]) and str(row[1]) == str(rowdate[3]) and str(row[2]) == str(rowdate[4]):
                    datasame.append(rowdate)
            datapush.append(list([market_name, comodity_name, quality_name, datasame]))
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
            ax.set_title(judul+' '+market_name[1]+' '+comodity_name+' '+quality_name)
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
        
            plt.title(judul+' '+market_name[1]+' '+comodity_name+' '+quality_name)
            plt.grid(True)
            l = io.BytesIO()
            plt.savefig(l, format='png', bbox_inches="tight")
            
            lineChart = base64.b64encode(l.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            #pie
            plt.title(judul+' '+market_name[1]+' '+comodity_name+' '+quality_name)
            plt.pie(lengthy, labels=lengthx, autopct='%1.1f%%', 
            shadow=True, startangle=90)
        
            plt.axis('equal')
            p = io.BytesIO()
            plt.savefig(p, format='png', bbox_inches="tight")
            
            pieChart = base64.b64encode(p.getvalue()).decode("utf-8").replace("\n", "")
            plt.show()
            
            bardata = base64.b64decode(barChart)
            barname = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+market_name[1]+' '+comodity_name+' '+quality_name+'-bar.png'
            with open(barname, 'wb') as f:
                f.write(bardata)
            
            linedata = base64.b64decode(lineChart)
            linename = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+market_name[1]+' '+comodity_name+' '+quality_name+'-line.png'
            with open(linename, 'wb') as f:
                f.write(linedata)
                
            piedata = base64.b64decode(pieChart)
            piename = 'C:/Users/ASUS/Documents/bappenas/PIHPS/img/'+judul+' '+market_name[1]+' '+comodity_name+' '+quality_name+'-pie.png'
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
    pdf.cell(177, 2*th1, 'Pasar Tradisional', border=1, align='C')
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
    pdf.cell(40, 2*th, "Jenis Market", border=1, align='C')
    pdf.cell(20, 2*th, "Lintang", border=1, align='C')
    pdf.cell(20, 2*th, "Bujur", border=1, align='C')
    pdf.cell(25, 2*th, "Provinsi ID", border=1, align='C')
    pdf.cell(40, 2*th, "Provinsi", border=1, align='C')
    pdf.cell(35, 2*th, "Kabupaten/Kota ID", border=1, align='C')
    pdf.cell(40, 2*th, "Kabupaten/Kota", border=1, align='C')
    pdf.cell(25, 2*th, "Kecamatan ID", border=1, align='C')
    pdf.cell(40, 2*th, "Kecamatan", border=1, align='C')
    pdf.cell(25, 2*th, "Kelurahan ID", border=1, align='C')
    pdf.cell(40, 2*th, "Kelurahan", border=1, align='C')
    pdf.cell(40, 2*th, "Komoditas", border=1, align='C')
    pdf.cell(40, 2*th, "Kualitas", border=1, align='C')
    
    for dt in datarow[0][3]:
        pdf.cell(40, 2*th, dt[0], border=1, align='C')
    
    pdf.ln(2*th1)
    
    pdf.set_font('Arial','',7)
    th = pdf.font_size
    for data in datarow:
        pdf.cell(20, 2*th, str(data[0][0]), border=1, align='C')
        pdf.cell(60, 2*th, str(data[0][1]), border=1, align='C')
        pdf.cell(60, 2*th, "Tradisional", border=1, align='C')
        pdf.cell(20, 2*th, str(data[0][11]), border=1, align='C')
        pdf.cell(20, 2*th, str(data[0][12]), border=1, align='C')
        pdf.cell(25, 2*th, str(data[0][2]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[0][3]), border=1, align='C')
        pdf.cell(35, 2*th, str(data[0][4]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[0][5]), border=1, align='C')
        pdf.cell(25, 2*th, str(data[0][6]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[0][7]), border=1, align='C')
        pdf.cell(25, 2*th, str(data[0][8]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[0][9]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[1]), border=1, align='C')
        pdf.cell(40, 2*th, str(data[2]), border=1, align='C')
        for dc in data[3]:
            pdf.cell(40, 2*th, str(dc[1]), border=1, align='C')
        pdf.cell(40, 2*th, 'Tradisional', border=1, align='C')
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
    header = ["Market ID","Market","Jenis Market","Lintang","Bujur","Provinsi ID","Provinsi","Kabupaten/Kota ID","Kabupaten/Kota","Kecamatan ID","Kecamatan","Kelurahan ID","Kelurahan","Komoditas","Kualitas"]
    
    for dt in datarow[0][3]:
        header.append(dt[0])   
    for col_num, data in enumerate(header):
        worksheet.write(0, col_num, data, row1)
            
    for data in datarow:
        w+=1
        body = [data[0][0], data[0][1], "Tradisional", data[0][11], data[0][12], data[0][2], data[0][3], data[0][4], data[0][5], data[0][6], data[0][7], data[0][8], data[0][9], data[1], data[2]]
        for dc in data[3]:
            body.append(str(dc[1]))
            
        for col_num, data in enumerate(body):
            worksheet.write(w, col_num, data, row2)
    workbook.close()
name = "Pasar"
host = "localhost"
username = "postgres"
password = "1234567890"
port = "5432"
database = "pihps"
table = "pihps_2"
table2 = "price_pihps_2"
judul = "Data Pasar Pasar"
subjudul = "Badan Perencanaan Pembangunan Nasional"
province_id = '32'

sql = uploadToPSQL(host, username, password, database, port, table, judul, name, subjudul, province_id)
price = uploadToPSQLprice(host, username, password, database, port, table, table2, judul, name, subjudul)
if sql == True and price == True:
    print('Running PHIPS Script')
else:
    print(sql)