import numpy as np
import pandas as pd
import lib.similarities.n0similarities as n0

def get_standard_merged(F1, F2):
    
    #berikan strip untuk text yg kosong
    F1 = F1.fillna('-')
    F2 = F2.fillna('-')
    
    #buat dataframe baru berisikan seluruh kombinasi row
    F1['key'] = 1
    F2['key'] = 1
    FF = pd.merge(F1,F2,on='key').drop('key',axis=1)
    
    #rename kolom F1
    FF = FF.rename(columns = {'Kode Variabel_x':'CODE_A'})
    FF = FF.rename(columns = {'Keterangan Variabel_x':'TITLE_A'})
    FF = FF.rename(columns = {'Kode Kategori_x':'SUBCODE_A'})
    FF = FF.rename(columns = {'Kategori_x':'SUBTITLE_A'})
    
    #rename kolom F2
    FF = FF.rename(columns = {'Kode Variabel_y':'CODE_B'})
    FF = FF.rename(columns = {'Keterangan Variabel_y':'TITLE_B'})
    FF = FF.rename(columns = {'Kode Kategori_y':'SUBCODE_B'})
    FF = FF.rename(columns = {'Kategori_y':'SUBTITLE_B'})
    
    #ubah semua value ke string
    FF = FF.applymap(str)
    
    #trim all
    FF = FF.applymap(lambda x: x.strip())
    
    #ambil depth terbesar
    FF['DEPTH'] = 2
    FF.loc[((FF.SUBCODE_A == '-') | (FF.SUBCODE_A == '')) & ((FF.SUBCODE_B == '-') | (FF.SUBCODE_B == '')), 'DEPTH'] = 1
    
    return FF


def get_similarities_value(FF, method_for_code='L', method_for_title='L', method_for_subcode='L', method_for_subtitle='L'):

    FF_CODE_A = FF['CODE_A'].to_numpy()
    FF_CODE_B = FF['CODE_B'].to_numpy()
    
    if method_for_code=='L': FF['CODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_code=='J': FF['CODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_code=='JW': FF['CODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    
    FF_TITLE_A = FF['TITLE_A'].to_numpy()
    FF_TITLE_B = FF['TITLE_B'].to_numpy()
    
    if method_for_title=='L': FF['TITLE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_TITLE_A[i], FF_TITLE_B[i]) for i in range(0, len(FF_TITLE_A))]
    if method_for_title=='J': FF['TITLE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_TITLE_A[i], FF_TITLE_B[i]) for i in range(0, len(FF_TITLE_A))]
    if method_for_title=='JW': FF['TITLE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_TITLE_A[i], FF_TITLE_B[i]) for i in range(0, len(FF_TITLE_A))]

    FF_SUBCODE_A = FF['SUBCODE_A'].to_numpy()
    FF_SUBCODE_B = FF['SUBCODE_B'].to_numpy()
    
    if method_for_subcode=='L': FF['SUBCODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_SUBCODE_A[i], FF_SUBCODE_B[i]) for i in range(0, len(FF_SUBCODE_A))]
    if method_for_subcode=='J': FF['SUBCODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_SUBCODE_A[i], FF_SUBCODE_B[i]) for i in range(0, len(FF_SUBCODE_A))]
    if method_for_subcode=='JW': FF['SUBCODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_SUBCODE_A[i], FF_SUBCODE_B[i]) for i in range(0, len(FF_SUBCODE_A))]

    FF_SUBTITLE_A = FF['SUBTITLE_A'].to_numpy()
    FF_SUBTITLE_B = FF['SUBTITLE_B'].to_numpy()

    if method_for_subtitle=='L': FF['SUBTITLE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_SUBTITLE_A[i], FF_SUBTITLE_B[i]) for i in range(0, len(FF_SUBTITLE_A))]
    if method_for_subtitle=='J': FF['SUBTITLE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_SUBTITLE_A[i], FF_SUBTITLE_B[i]) for i in range(0, len(FF_SUBTITLE_A))]
    if method_for_subtitle=='JW': FF['SUBTITLE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_SUBTITLE_A[i], FF_SUBTITLE_B[i]) for i in range(0, len(FF_SUBTITLE_A))]

    
    return FF
    
def get_most_similar(FF):
    
    #A=one B=many
    FF['CODE_MOSTSIMILAR_Aisone'] = FF.groupby(['CODE_A'])['CODE_SIMILARITIES'].transform(max) == FF['CODE_SIMILARITIES']
    FF['TITLE_MOSTSIMILAR_Aisone'] = FF.groupby(['CODE_A', 'TITLE_A'])['TITLE_SIMILARITIES'].transform(max) == FF['TITLE_SIMILARITIES']
    FF['SUBCODE_MOSTSIMILAR_Aisone'] = FF.groupby(['CODE_A', 'SUBCODE_A'])['SUBCODE_SIMILARITIES'].transform(max) == FF['SUBCODE_SIMILARITIES']
    FF['SUBTITLE_MOSTSIMILAR_Aisone'] = FF.groupby(['CODE_A', 'SUBCODE_A', 'SUBTITLE_A'])['SUBTITLE_SIMILARITIES'].transform(max) == FF['SUBTITLE_SIMILARITIES']
    
    #B=one A=many
    FF['CODE_MOSTSIMILAR_Bisone'] = FF.groupby(['CODE_B'])['CODE_SIMILARITIES'].transform(max) == FF['CODE_SIMILARITIES']
    FF['TITLE_MOSTSIMILAR_Bisone'] = FF.groupby(['CODE_B', 'TITLE_B'])['TITLE_SIMILARITIES'].transform(max) == FF['TITLE_SIMILARITIES']
    FF['SUBCODE_MOSTSIMILAR_Bisone'] = FF.groupby(['CODE_B', 'SUBCODE_B'])['SUBCODE_SIMILARITIES'].transform(max) == FF['SUBCODE_SIMILARITIES']
    FF['SUBTITLE_MOSTSIMILAR_Bisone'] = FF.groupby(['CODE_B', 'SUBCODE_B', 'SUBTITLE_B'])['SUBTITLE_SIMILARITIES'].transform(max) == FF['SUBTITLE_SIMILARITIES']
    
    return FF
    
def get_similarity_score(FF, code_weight=1, title_weight=1, subcode_weight=1, subtitle_weight=1):
    #A=one B=many
    FF['SIMILAR_SCORE_Aisone'] = (code_weight * FF['CODE_MOSTSIMILAR_Aisone']) + (title_weight * FF['TITLE_MOSTSIMILAR_Aisone']) + (subcode_weight * FF['SUBCODE_MOSTSIMILAR_Aisone']) + (subtitle_weight * FF['SUBTITLE_MOSTSIMILAR_Aisone'])
    #B=one A=many
    FF['SIMILAR_SCORE_Bisone'] = (code_weight * FF['CODE_MOSTSIMILAR_Bisone']) + (title_weight * FF['TITLE_MOSTSIMILAR_Bisone']) + (subcode_weight * FF['SUBCODE_MOSTSIMILAR_Bisone']) + (subtitle_weight * FF['SUBTITLE_MOSTSIMILAR_Bisone'])
    #total
    FF['SIMILAR_SCORE_TOTAL'] = FF['SIMILAR_SCORE_Aisone'] + FF['SIMILAR_SCORE_Bisone']
    
    return FF
    
def get_match(FF, minimum_similar_score=0):
    
    FF['IS_MATCH'] = False
    
    #untuk yg tidak memiliki sub
    FF.loc[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score), 'IS_MATCH'] = FF[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)].groupby(['CODE_A', 'TITLE_A'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)]['SIMILAR_SCORE_TOTAL']
    FF.loc[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score), 'IS_MATCH'] |= FF[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)].groupby(['CODE_B', 'TITLE_B'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[(FF.DEPTH==1) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)]['SIMILAR_SCORE_TOTAL']
    
    #untuk yg memiliki sub
    FF.loc[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score), 'IS_MATCH'] = FF[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)].groupby(['CODE_A', 'SUBCODE_A', 'SUBTITLE_A'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)]['SIMILAR_SCORE_TOTAL']
    FF.loc[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score), 'IS_MATCH'] |= FF[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)].groupby(['CODE_B', 'SUBCODE_B', 'SUBTITLE_B'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[(FF.DEPTH==2) & (FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score)]['SIMILAR_SCORE_TOTAL']
    
    return FF

def get_recomendation(FF):
    #default is drop & match
    FF['RECOMENDATION'] = 'drop'
    FF.loc[FF.IS_MATCH == True, 'RECOMENDATION'] = 'match'
    
    return FF
    
    '''
    #cek A mekar ke B
    FFcalc = FF[FF.IS_MATCH == True].groupby(['CODE_A', 'TITLE_A', 'SUBCODE_A', 'SUBTITLE_A'])['IS_MATCH'].count()
    FFret = pd.DataFrame(FFcalc[FFcalc>1])
    FFret['RECOMENDATION'] = 'membelah'
    
    FFmerge = pd.merge(FF, FFret, how='left', on=['CODE_A', 'TITLE_A', 'SUBCODE_A', 'SUBTITLE_A'])
    FFmerge.loc[(FFmerge.IS_MATCH_y > 1) & (FFmerge.IS_MATCH_x == True), 'RECOMENDATION_x'] = FFmerge['RECOMENDATION_y']
    del FFmerge['IS_MATCH_y']
    del FFmerge['RECOMENDATION_y']
    FF2 = FFmerge.rename(columns = {'IS_MATCH_x':'IS_MATCH', 'RECOMENDATION_x':'RECOMENDATION'})
    
    
    #cek total A ke B
    FFcalc = FF2[FF2.IS_MATCH == True].groupby(['CODE_B', 'TITLE_B', 'SUBCODE_B', 'SUBTITLE_B'])['IS_MATCH'].count()
    FFret = pd.DataFrame(FFcalc[FFcalc>1])
    FFret['RECOMENDATION'] = 'bergabung'
    
    FFmerge = pd.merge(FF2, FFret, how='left', on=['CODE_B', 'TITLE_B', 'SUBCODE_B', 'SUBTITLE_B'])
    FFmerge.loc[(FFmerge.IS_MATCH_y > 1) & (FFmerge.IS_MATCH_x == True), 'RECOMENDATION_x'] = FFmerge['RECOMENDATION_y']
    del FFmerge['IS_MATCH_y']
    del FFmerge['RECOMENDATION_y']
    return FFmerge.rename(columns = {'IS_MATCH_x':'IS_MATCH', 'RECOMENDATION_x':'RECOMENDATION'})
    '''

