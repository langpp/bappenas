import pandas as pd
import lib.data.transpose as dtrans


#read data & layout path

layoutpath_wilayah = pd.read_csv('process wilayah 5 layout path.csv')
layoutpath_kategori = pd.read_csv('process kategori 5 layout path.csv')
data_2008 = pd.read_excel('input data 2008.xlsx')
#data_2011 = pd.read_excel('input data 2011.xlsx')


#transpose data 2008 menjadi 2011

F1 = dtrans.transpose_wilayah(data_2008, layoutpath_wilayah)

F1.to_csv('process data 1 wilayah transposed.csv', index = False)

F2 = dtrans.transpose_kategori(F1, layoutpath_kategori)

F2.to_csv('process data 2 wilayah+kategori transposed.csv', index = False)