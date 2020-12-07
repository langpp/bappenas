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
    FF = FF.rename(columns = {'R101_x':'PROVCODE_A'})
    FF = FF.rename(columns = {'R101N_x':'PROVNAME_A'})
    FF = FF.rename(columns = {'R102_x':'KOTCODE_A'})
    FF = FF.rename(columns = {'R102N_x':'KOTNAME_A'})
    FF = FF.rename(columns = {'R103_x':'KECCODE_A'})
    FF = FF.rename(columns = {'R103N_x':'KECNAME_A'})
    FF = FF.rename(columns = {'R104_x':'KELCODE_A'})
    FF = FF.rename(columns = {'R104N_x':'KELNAME_A'})
    
    #rename kolom F2
    FF = FF.rename(columns = {'R101_y':'PROVCODE_B'})
    FF = FF.rename(columns = {'R101N_y':'PROVNAME_B'})
    FF = FF.rename(columns = {'R102_y':'KOTCODE_B'})
    FF = FF.rename(columns = {'R102N_y':'KOTNAME_B'})
    FF = FF.rename(columns = {'R103_y':'KECCODE_B'})
    FF = FF.rename(columns = {'R103N_y':'KECNAME_B'})
    FF = FF.rename(columns = {'R104_y':'KELCODE_B'})
    FF = FF.rename(columns = {'R104N_y':'KELNAME_B'})
    
    #ubah semua value ke string
    FF = FF.applymap(str)
    
    #trim all
    FF = FF.applymap(lambda x: x.strip())
    
    return FF


def get_similarities_value(FF, method_for_provcode='L', method_for_provname='L', method_for_kotcode='L', method_for_kotname='L', method_for_keccode='L', method_for_kecname='L', method_for_kelcode='L', method_for_kelname='L'):

    FF_CODE_A = FF['PROVCODE_A'].to_numpy()
    FF_CODE_B = FF['PROVCODE_B'].to_numpy()
    
    if method_for_provcode=='L': FF['PROVCODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_provcode=='J': FF['PROVCODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_provcode=='JW': FF['PROVCODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['PROVNAME_A'].to_numpy()
    FF_CODE_B = FF['PROVNAME_B'].to_numpy()
    
    if method_for_provname=='L': FF['PROVNAME_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_provname=='J': FF['PROVNAME_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_provname=='JW': FF['PROVNAME_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KOTCODE_A'].to_numpy()
    FF_CODE_B = FF['KOTCODE_B'].to_numpy()
    
    if method_for_kotcode=='L': FF['KOTCODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kotcode=='J': FF['KOTCODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kotcode=='JW': FF['KOTCODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KOTNAME_A'].to_numpy()
    FF_CODE_B = FF['KOTNAME_B'].to_numpy()
    
    if method_for_kotname=='L': FF['KOTNAME_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kotname=='J': FF['KOTNAME_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kotname=='JW': FF['KOTNAME_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KECCODE_A'].to_numpy()
    FF_CODE_B = FF['KECCODE_B'].to_numpy()
    
    if method_for_keccode=='L': FF['KECCODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_keccode=='J': FF['KECCODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_keccode=='JW': FF['KECCODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KECNAME_A'].to_numpy()
    FF_CODE_B = FF['KECNAME_B'].to_numpy()
    
    if method_for_kecname=='L': FF['KECNAME_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kecname=='J': FF['KECNAME_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kecname=='JW': FF['KECNAME_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KELCODE_A'].to_numpy()
    FF_CODE_B = FF['KELCODE_B'].to_numpy()
    
    if method_for_kelcode=='L': FF['KELCODE_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kelcode=='J': FF['KELCODE_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kelcode=='JW': FF['KELCODE_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    FF_CODE_A = FF['KELNAME_A'].to_numpy()
    FF_CODE_B = FF['KELNAME_B'].to_numpy()
    
    if method_for_kelname=='L': FF['KELNAME_SIMILARITIES'] = [n0.get_levenshtein_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kelname=='J': FF['KELNAME_SIMILARITIES'] = [n0.get_jaro_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]
    if method_for_kelname=='JW': FF['KELNAME_SIMILARITIES'] = [n0.get_jaro_winkler_similarity(FF_CODE_A[i], FF_CODE_B[i]) for i in range(0, len(FF_CODE_A))]

    
    return FF
    

def get_most_similar(FF):
    
    #A=one B=many
    FF['PROVCODE_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A'])['PROVCODE_SIMILARITIES'].transform(max) == FF['PROVCODE_SIMILARITIES']
    FF['PROVNAME_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'PROVNAME_A'])['PROVNAME_SIMILARITIES'].transform(max) == FF['PROVNAME_SIMILARITIES']
    FF['KOTCODE_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A'])['KOTCODE_SIMILARITIES'].transform(max) == FF['KOTCODE_SIMILARITIES']
    FF['KOTNAME_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A', 'KOTNAME_A'])['KOTNAME_SIMILARITIES'].transform(max) == FF['KOTNAME_SIMILARITIES']
    FF['KECCODE_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A', 'KECCODE_A'])['KECCODE_SIMILARITIES'].transform(max) == FF['KECCODE_SIMILARITIES']
    FF['KECNAME_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A', 'KECCODE_A', 'KECNAME_A'])['KECNAME_SIMILARITIES'].transform(max) == FF['KECNAME_SIMILARITIES']
    FF['KELCODE_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A', 'KECCODE_A', 'KELCODE_A'])['KELCODE_SIMILARITIES'].transform(max) == FF['KELCODE_SIMILARITIES']
    FF['KELNAME_MOSTSIMILAR_Aisone'] = FF.groupby(['PROVCODE_A', 'KOTCODE_A', 'KECCODE_A', 'KELCODE_A', 'KELNAME_A'])['KELNAME_SIMILARITIES'].transform(max) == FF['KELNAME_SIMILARITIES']
    
    #B=one A=many
    FF['PROVCODE_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B'])['PROVCODE_SIMILARITIES'].transform(max) == FF['PROVCODE_SIMILARITIES']
    FF['PROVNAME_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'PROVNAME_B'])['PROVNAME_SIMILARITIES'].transform(max) == FF['PROVNAME_SIMILARITIES']
    FF['KOTCODE_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B'])['KOTCODE_SIMILARITIES'].transform(max) == FF['KOTCODE_SIMILARITIES']
    FF['KOTNAME_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B', 'KOTNAME_B'])['KOTNAME_SIMILARITIES'].transform(max) == FF['KOTNAME_SIMILARITIES']
    FF['KECCODE_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B', 'KECCODE_B'])['KECCODE_SIMILARITIES'].transform(max) == FF['KECCODE_SIMILARITIES']
    FF['KECNAME_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B', 'KECCODE_B', 'KECNAME_B'])['KECNAME_SIMILARITIES'].transform(max) == FF['KECNAME_SIMILARITIES']
    FF['KELCODE_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B', 'KECCODE_B', 'KELCODE_B'])['KELCODE_SIMILARITIES'].transform(max) == FF['KELCODE_SIMILARITIES']
    FF['KELNAME_MOSTSIMILAR_Bisone'] = FF.groupby(['PROVCODE_B', 'KOTCODE_B', 'KECCODE_B', 'KELCODE_B', 'KELNAME_B'])['KELNAME_SIMILARITIES'].transform(max) == FF['KELNAME_SIMILARITIES']
    
    return FF
    
def get_similarity_score(FF, provcode_weight=1, provname_weight=1, kotcode_weight=1, kotname_weight=1, keccode_weight=1, kecname_weight=1, kelcode_weight=1, kelname_weight=1):
    
    #A=one B=many
    FF['SIMILAR_SCORE_Aisone'] = (provcode_weight * FF['PROVCODE_MOSTSIMILAR_Aisone']) + (provname_weight * FF['PROVNAME_MOSTSIMILAR_Aisone']) + (kotcode_weight * FF['KOTCODE_MOSTSIMILAR_Aisone']) + (kotname_weight * FF['KOTNAME_MOSTSIMILAR_Aisone']) + (keccode_weight * FF['KECCODE_MOSTSIMILAR_Aisone']) + (kecname_weight * FF['KECNAME_MOSTSIMILAR_Aisone']) + (kelcode_weight * FF['KELCODE_MOSTSIMILAR_Aisone']) + (kelname_weight * FF['KELNAME_MOSTSIMILAR_Aisone'])
    #B=one A=many
    FF['SIMILAR_SCORE_Bisone'] = (provcode_weight * FF['PROVCODE_MOSTSIMILAR_Bisone']) + (provname_weight * FF['PROVNAME_MOSTSIMILAR_Bisone']) + (kotcode_weight * FF['KOTCODE_MOSTSIMILAR_Bisone']) + (kotname_weight * FF['KOTNAME_MOSTSIMILAR_Bisone']) + (keccode_weight * FF['KECCODE_MOSTSIMILAR_Bisone']) + (kecname_weight * FF['KECNAME_MOSTSIMILAR_Bisone']) + (kelcode_weight * FF['KELCODE_MOSTSIMILAR_Bisone']) + (kelname_weight * FF['KELNAME_MOSTSIMILAR_Bisone'])
    #total
    FF['SIMILAR_SCORE_TOTAL'] = FF['SIMILAR_SCORE_Aisone'] + FF['SIMILAR_SCORE_Bisone']
    
    return FF
    
def get_match(FF, minimum_similar_score=0):
    
    FF['IS_MATCH'] = False
    
    FF.loc[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score, 'IS_MATCH'] = FF[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score].groupby(['PROVCODE_A', 'KOTCODE_A', 'KECCODE_A', 'KELCODE_A', 'KELNAME_A'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score]['SIMILAR_SCORE_TOTAL']
    FF.loc[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score, 'IS_MATCH'] |= FF[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score].groupby(['PROVCODE_B', 'KOTCODE_B', 'KECCODE_B', 'KELCODE_B', 'KELNAME_B'])['SIMILAR_SCORE_TOTAL'].transform(max) == FF[FF.SIMILAR_SCORE_TOTAL>=minimum_similar_score]['SIMILAR_SCORE_TOTAL']
    
    return FF
    
def get_recomendation(FF):
    #default is drop & match
    FF['RECOMENDATION'] = 'drop'
    FF.loc[FF.IS_MATCH == True, 'RECOMENDATION'] = 'match'
    
    #cek A mekar ke B
    FFcalc = FF[FF.IS_MATCH == True].groupby(['PROVCODE_A', 'PROVNAME_A', 'KOTCODE_A', 'KOTNAME_A', 'KECCODE_A', 'KECNAME_A', 'KELCODE_A', 'KELNAME_A'])['IS_MATCH'].count()
    FFret = pd.DataFrame(FFcalc[FFcalc>1])
    FFret['RECOMENDATION'] = 'membelah'
    
    FFmerge = pd.merge(FF, FFret, how='left', on=['PROVCODE_A', 'PROVNAME_A', 'KOTCODE_A', 'KOTNAME_A', 'KECCODE_A', 'KECNAME_A', 'KELCODE_A', 'KELNAME_A'])
    FFmerge.loc[(FFmerge.IS_MATCH_y > 1) & (FFmerge.IS_MATCH_x == True), 'RECOMENDATION_x'] = FFmerge['RECOMENDATION_y']
    del FFmerge['IS_MATCH_y']
    del FFmerge['RECOMENDATION_y']
    FF2 = FFmerge.rename(columns = {'IS_MATCH_x':'IS_MATCH', 'RECOMENDATION_x':'RECOMENDATION'})
    
    
    #cek total A ke B
    FFcalc = FF2[FF2.IS_MATCH == True].groupby(['PROVCODE_B', 'PROVNAME_B', 'KOTCODE_B', 'KOTNAME_B', 'KECCODE_B', 'KECNAME_B', 'KELCODE_B', 'KELNAME_B'])['IS_MATCH'].count()
    FFret = pd.DataFrame(FFcalc[FFcalc>1])
    FFret['RECOMENDATION'] = 'bergabung'
    
    FFmerge = pd.merge(FF2, FFret, how='left', on=['PROVCODE_B', 'PROVNAME_B', 'KOTCODE_B', 'KOTNAME_B', 'KECCODE_B', 'KECNAME_B', 'KELCODE_B', 'KELNAME_B'])
    FFmerge.loc[(FFmerge.IS_MATCH_y > 1) & (FFmerge.IS_MATCH_x == True), 'RECOMENDATION_x'] = FFmerge['RECOMENDATION_y']
    del FFmerge['IS_MATCH_y']
    del FFmerge['RECOMENDATION_y']
    return FFmerge.rename(columns = {'IS_MATCH_x':'IS_MATCH', 'RECOMENDATION_x':'RECOMENDATION'})
    
    
    