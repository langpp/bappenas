import pandas as pd
import lib.wilayah.n2similarities as wn2
import lib.kategori.n2similarities as cn2
import lib.data.transpose as dtrans
import os

def get_layoutpath_recomendation(
        xls_wil_awal, 
        xls_wil_tujuan,
        csv_wil_output,
        
        xls_cat_awal, 
        xls_cat_tujuan,
        csv_cat_output,
        
        wil_provcode_method='L',
        wil_provname_method='L',
        wil_kotcode_method='L',
        wil_kotname_method='L',
        wil_keccode_method='L',
        wil_kecname_method='L',
        wil_kelcode_method='L',
        wil_kelname_method='L',
        
        wil_provcode_weight=1,
        wil_provname_weight=1,
        wil_kotcode_weight=1,
        wil_kotname_weight=1,
        wil_keccode_weight=1,
        wil_kecname_weight=1,
        wil_kelcode_weight=1,
        wil_kelname_weight=1,
		wil_minimum_similar_score=13,
        
        cat_code_method='L',
        cat_title_method='L',
        cat_subcode_method='L',
        cat_subtitle_method='L',
        
        cat_code_weight=1,
        cat_title_weight=1,
        cat_subcode_weight=1,
        cat_subtitle_weight=1,
		cat_minimum_similar_score=7,
        
        print_process=True
    ):
    
    #baca wilayah
    
    wF1 = pd.read_excel(xls_wil_awal)
    wF2 = pd.read_excel(xls_wil_tujuan)
    if print_process:
        print("Baca wilayah done")
    
    #read dan merge
    
    wFF = wn2.get_standard_merged(wF1, wF2)
    if print_process:
        print("Read & merge done")
    
    #get similarities value
    
    wFF = wn2.get_similarities_value(
        wFF, 
        method_for_provcode=wil_provcode_method, 
        method_for_provname=wil_provname_method, 
        method_for_kotcode=wil_kotcode_method, 
        method_for_kotname=wil_kotname_method, 
        method_for_keccode=wil_keccode_method, 
        method_for_kecname=wil_kecname_method, 
        method_for_kelcode=wil_kelcode_method, 
        method_for_kelname=wil_kelname_method
    )
    
    if print_process:
        print("Get similarities value done")
    
    #get similarity score & match
    
    wFF = wn2.get_most_similar(wFF)

    wFF = wn2.get_similarity_score(wFF, 
        provcode_weight=wil_provcode_weight, 
        provname_weight=wil_provname_weight, 
        kotcode_weight=wil_kotcode_weight, 
        kotname_weight=wil_kotname_weight, 
        keccode_weight=wil_keccode_weight, 
        kecname_weight=wil_kecname_weight, 
        kelcode_weight=wil_kelcode_weight, 
        kelname_weight=wil_kelname_weight
    )

    wFF = wn2.get_match(wFF, wil_minimum_similar_score)
    
    if print_process:
        print("Get similarity score & match done")
    
    #get recomendation
    
    wFF = wn2.get_recomendation(wFF)
    
    wFF['ACTION'] = wFF['RECOMENDATION']
    
    if print_process:
        print("Get recomendation done")
    
    #output
    
    wFF.to_csv(csv_wil_output, index = False)
    
    if print_process:
        print("Send output done")
    
    #baca kategori
    
    cF1 = pd.read_excel(xls_cat_awal)
    cF2 = pd.read_excel(xls_cat_tujuan)
    
    if print_process:
        print("Baca kategori done")
    
    #read dan merge
    
    cFF = cn2.get_standard_merged(cF1, cF2)
    
    if print_process:
        print("Read & merge done")
    
    #get similarities value
    
    cFF = cn2.get_similarities_value(
        cFF, 
        method_for_code=cat_code_method, 
        method_for_title=cat_title_method, 
        method_for_subcode=cat_subcode_method, 
        method_for_subtitle=cat_subtitle_method
    )
    
    if print_process:
        print("Get similarities value done")
    
    #get similarity score & match
    
    cFF = cn2.get_most_similar(cFF)

    cFF = cn2.get_similarity_score(
        cFF, 
        code_weight=cat_code_weight, 
        title_weight=cat_title_weight, 
        subcode_weight=cat_subcode_weight, 
        subtitle_weight=cat_subtitle_weight
    )

    cFF = cn2.get_match(cFF, cat_minimum_similar_score)
    
    if print_process:
        print("Get similarity score & match done")
    
    #get recomendation
    
    cFF = cn2.get_recomendation(cFF)
    
    cFF['ACTION'] = cFF['RECOMENDATION']
    
    if print_process:
        print("Get recomendation done")
    
    #output
    cFF.to_csv(csv_cat_output, index = False)
    
    if print_process:
        print("Send output done")
    

def minimize_layoutpath(csv_wil, csv_wil_output, csv_cat, csv_cat_output):
    
    #wilayah
    
    wFF = pd.read_csv(csv_wil, dtype={
    'PROVCODE_A':str, 'PROVNAME_A':str, 'KOTCODE_A':str, 'KOTNAME_A':str, 'KECCODE_A':str, 'KECNAME_A':str, 'KELCODE_A':str, 'KELNAME_A':str,
    'PROVCODE_B':str, 'PROVNAME_B':str, 'KOTCODE_B':str, 'KOTNAME_B':str, 'KECCODE_B':str, 'KECNAME_B':str, 'KELCODE_B':str, 'KELNAME_B':str,
    'PROVCODE_SIMILARITIES':float, 'PROVNAME_SIMILARITIES':float,
    'KOTCODE_SIMILARITIES':float, 'KOTNAME_SIMILARITIES':float,
    'KECCODE_SIMILARITIES':float, 'KECNAME_SIMILARITIES':float,
    'KELCODE_SIMILARITIES':float, 'KELNAME_SIMILARITIES':float,
    'PROVCODE_MOSTSIMILAR_Aisone':bool, 'PROVNAME_MOSTSIMILAR_Aisone':bool,	 'KOTCODE_MOSTSIMILAR_Aisone':bool, 'KOTNAME_MOSTSIMILAR_Aisone':bool, 'KECCODE_MOSTSIMILAR_Aisone':bool, 'KECNAME_MOSTSIMILAR_Aisone':bool, 'KELCODE_MOSTSIMILAR_Aisone':bool, 'KELNAME_MOSTSIMILAR_Aisone':bool,
    'PROVCODE_MOSTSIMILAR_Bisone':bool, 'PROVNAME_MOSTSIMILAR_Bisone':bool, 'KOTCODE_MOSTSIMILAR_Bisone':bool, 'KOTNAME_MOSTSIMILAR_Bisone':bool, 'KECCODE_MOSTSIMILAR_Bisone':bool, 'KECNAME_MOSTSIMILAR_Bisone':bool, 'KELCODE_MOSTSIMILAR_Bisone':bool, 'KELNAME_MOSTSIMILAR_Bisone':bool,
    'SIMILAR_SCORE_Aisone':int,'SIMILAR_SCORE_Bisone':int,'SIMILAR_SCORE_TOTAL':int,
    'IS_MATCH':bool,'RECOMENDATION':str, 'ACTION':str
    })
    
    wFFmin = wFF[wFF.ACTION.isin(['match', 'membelah', 'membelah-keepvalue', 'bergabung', 'newB'])].filter(['PROVCODE_A', 'PROVNAME_A', 'KOTCODE_A', 'KOTNAME_A', 'KECCODE_A', 'KECNAME_A', 'KELCODE_A', 'KELNAME_A', 'PROVCODE_B', 'PROVNAME_B', 'KOTCODE_B', 'KOTNAME_B', 'KECCODE_B', 'KECNAME_B', 'KELCODE_B', 'KELNAME_B', 'ACTION'], axis=1)

    wFFmin.to_csv(csv_wil_output, index = False)
    
    #kategori
    
    cFF = pd.read_csv(csv_cat, dtype={
    'CODE_A':str, 'TITLE_A':str, 'SUBCODE_A':str, 'SUBTITLE_A':str,
    'CODE_B':str, 'TITLE_B':str, 'SUBCODE_B':str, 'SUBTITLE_B':str,
    'DEPTH':int, 'CODE_SIMILARITIES':float, 'TITLE_SIMILARITIES':float,
    'SUBCODE_SIMILARITIES':float, 'SUBTITLE_SIMILARITIES':float,
    'CODE_MOSTSIMILAR_Aisone':bool,'TITLE_MOSTSIMILAR_Aisone':bool,'SUBCODE_MOSTSIMILAR_Aisone':bool,'SUBTITLE_MOSTSIMILAR_Aisone':bool,
    'CODE_MOSTSIMILAR_Bisone':bool,'TITLE_MOSTSIMILAR_Bisone':bool,'SUBCODE_MOSTSIMILAR_Bisone':bool,'SUBTITLE_MOSTSIMILAR_Bisone':bool,
    'SIMILAR_SCORE_Aisone':int,'SIMILAR_SCORE_Bisone':int,'SIMILAR_SCORE_TOTAL':int,
    'IS_MATCH':bool,'RECOMENDATION':str, 'ACTION':str
    })
    
    cFFmin = cFF[cFF.ACTION.isin(['match', 'newB'])].filter(['CODE_A', 'TITLE_A', 'SUBCODE_A', 'SUBTITLE_A', 'CODE_B', 'TITLE_B', 'SUBCODE_B', 'SUBTITLE_B', 'ACTION'], axis=1)

    cFFmin.to_csv(csv_cat_output, index = False)
    

def transpose(xls_data, csv_wil_layoutpath, csv_cat_layoutpath, xls_data_output, print_process="True"):
    data_2008 = pd.read_excel(xls_data)
    layoutpath_wilayah = pd.read_csv(csv_wil_layoutpath)
    layoutpath_kategori = pd.read_csv(csv_cat_layoutpath)
    
    F1 = dtrans.transpose_wilayah(data_2008, layoutpath_wilayah)
    if print_process:
        print("Transpose wilayah done")
    
    F2 = dtrans.transpose_kategori(F1, layoutpath_kategori)
    if print_process:
        print("Transpose kategori done")
    
    F2.to_excel(xls_data_output, index = False)


'''

#layoutpath 2004 ke 2006

get_layoutpath_recomendation(
    xls_wil_awal="2004-wilayah.xlsx", 
    xls_wil_tujuan="2006-wilayah.xlsx",
    csv_wil_output="layoutpath_2004_to_2006_wilayah.csv",
    xls_cat_awal="2004-kategori.xlsx", 
    xls_cat_tujuan="2006-kategori.xlsx",
    csv_cat_output="layoutpath_2004_to_2006_kategori.csv"
)

minimize_layoutpath("layoutpath_2004_to_2006_wilayah.csv", "layoutpath_2004_to_2006_wilayah_min.csv", "layoutpath_2004_to_2006_kategori.csv", "layoutpath_2004_to_2006_kategori_min.csv")

os.remove("layoutpath_2004_to_2006_wilayah.csv")
os.remove("layoutpath_2004_to_2006_kategori.csv")

print("layoutpath 2004 ke 2006 done")

#layoutpath 2006 ke 2008

get_layoutpath_recomendation(
    xls_wil_awal="2006-wilayah.xlsx", 
    xls_wil_tujuan="2008-wilayah.xlsx",
    csv_wil_output="layoutpath_2006_to_2008_wilayah.csv",
    xls_cat_awal="2006-kategori.xlsx", 
    xls_cat_tujuan="2008-kategori.xlsx",
    csv_cat_output="layoutpath_2006_to_2008_kategori.csv"
)

minimize_layoutpath("layoutpath_2006_to_2008_wilayah.csv", "layoutpath_2006_to_2008_wilayah_min.csv", "layoutpath_2006_to_2008_kategori.csv", "layoutpath_2006_to_2008_kategori_min.csv")

os.remove("layoutpath_2006_to_2008_wilayah.csv")
os.remove("layoutpath_2006_to_2008_kategori.csv")

print("layoutpath 2006 ke 2008 done")

#layoutpath 2008 ke 2011

get_layoutpath_recomendation(
    xls_wil_awal="2008-wilayah.xlsx", 
    xls_wil_tujuan="2011-wilayah.xlsx",
    csv_wil_output="layoutpath_2008_to_2011_wilayah.csv",
    xls_cat_awal="2008-kategori.xlsx", 
    xls_cat_tujuan="2011-kategori.xlsx",
    csv_cat_output="layoutpath_2008_to_2011_kategori.csv"
)

minimize_layoutpath("layoutpath_2008_to_2011_wilayah.csv", "layoutpath_2008_to_2011_wilayah_min.csv", "layoutpath_2008_to_2011_kategori.csv", "layoutpath_2008_to_2011_kategori_min.csv")

os.remove("layoutpath_2008_to_2011_wilayah.csv")
os.remove("layoutpath_2008_to_2011_kategori.csv")

print("layoutpath 2008 ke 2011 done")

'''

#tranpose 2004 ke 2006
transpose("2004-data.xlsx", "layoutpath_2004_to_2006_wilayah_min.csv", "layoutpath_2004_to_2006_kategori_min.csv", "2004-data format 2006.xlsx")

print("tranpose 2004 ke 2006 done")

#tranpose 2006 ke 2008
transpose("2004-data format 2006.xlsx", "layoutpath_2006_to_2008_wilayah_min.csv", "layoutpath_2006_to_2008_kategori_min.csv", "2004-data format 2008.xlsx")
transpose("2006-data.xlsx", "layoutpath_2006_to_2008_wilayah_min.csv", "layoutpath_2006_to_2008_kategori_min.csv", "2006-data format 2008.xlsx")

print("tranpose 2006 ke 2008 done")

#tranpose 2008 ke 2011
transpose("2004-data format 2008.xlsx", "layoutpath_2008_to_2011_wilayah_min.csv", "layoutpath_2008_to_2011_kategori_min.csv", "2004-data format 2011.xlsx")
transpose("2006-data format 2008.xlsx", "layoutpath_2008_to_2011_wilayah_min.csv", "layoutpath_2008_to_2011_kategori_min.csv", "2006-data format 2011.xlsx")
transpose("2008-data.xlsx", "layoutpath_2008_to_2011_wilayah_min.csv", "layoutpath_2008_to_2011_kategori_min.csv", "2008-data format 2011.xlsx")

print("tranpose 2008 ke 2011 done")




