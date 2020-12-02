import pandas as pd
import lib.kategori.n2similarities as n2

import time
start_time = time.time()




#read dan merge

#F1 = pd.read_excel('input kategori dummy1.xlsx')
#F2 = pd.read_excel('input kategori dummy2.xlsx')

F1 = pd.read_excel('input kategori 2008.xlsx')
F2 = pd.read_excel('input kategori 2011.xlsx')

FF = n2.get_standard_merged(F1, F2)

print("A--- %s seconds ---" % (time.time() - start_time))

FF.to_csv('process kategori 1 merge.csv', index = False)

print("B--- %s seconds ---" % (time.time() - start_time))






#get similarities value

FF = pd.read_csv('process kategori 1 merge.csv', dtype='str')

print("C--- %s seconds ---" % (time.time() - start_time))

FF = n2.get_similarities_value(FF, method_for_code='L', method_for_title='L', method_for_subcode='J', method_for_subtitle='JW')

print("D--- %s seconds ---" % (time.time() - start_time))

FF.to_csv('process kategori 2 similarities.csv', index = False)





#get similarity score

FF = pd.read_csv('process kategori 2 similarities.csv', dtype={
    'CODE_A':str, 'TITLE_A':str, 'SUBCODE_A':str, 'SUBTITLE_A':str,
    'CODE_B':str, 'TITLE_B':str, 'SUBCODE_B':str, 'SUBTITLE_B':str,
    'DEPTH':int, 'CODE_SIMILARITIES':float, 'TITLE_SIMILARITIES':float,
    'SUBCODE_SIMILARITIES':float, 'SUBTITLE_SIMILARITIES':float
    })

print("E--- %s seconds ---" % (time.time() - start_time))


FF = n2.get_most_similar(FF)

FF = n2.get_similarity_score(FF)

FF = n2.get_match(FF, 7)

FF.to_csv('process kategori 3 similarity score & match.csv', index = False)

print("F--- %s seconds ---" % (time.time() - start_time))

print('get similarity score & match done')



#get recomendation
FF = n2.get_recomendation(FF)

FF.to_csv('process kategori 4 get recomendation.csv', index = False)

print("G--- %s seconds ---" % (time.time() - start_time))

print('get recomendation done')





#create layout path

FF = pd.read_csv('process kategori 4 get recomendation.csv', dtype={
    'CODE_A':str, 'TITLE_A':str, 'SUBCODE_A':str, 'SUBTITLE_A':str,
    'CODE_B':str, 'TITLE_B':str, 'SUBCODE_B':str, 'SUBTITLE_B':str,
    'DEPTH':int, 'CODE_SIMILARITIES':float, 'TITLE_SIMILARITIES':float,
    'SUBCODE_SIMILARITIES':float, 'SUBTITLE_SIMILARITIES':float,
    'CODE_MOSTSIMILAR_Aisone':bool,'TITLE_MOSTSIMILAR_Aisone':bool,'SUBCODE_MOSTSIMILAR_Aisone':bool,'SUBTITLE_MOSTSIMILAR_Aisone':bool,
    'CODE_MOSTSIMILAR_Bisone':bool,'TITLE_MOSTSIMILAR_Bisone':bool,'SUBCODE_MOSTSIMILAR_Bisone':bool,'SUBTITLE_MOSTSIMILAR_Bisone':bool,
    'SIMILAR_SCORE_Aisone':int,'SIMILAR_SCORE_Bisone':int,'SIMILAR_SCORE_TOTAL':int,
    'IS_MATCH':bool,'RECOMENDATION':str
    })

FF['ACTION'] = FF['RECOMENDATION']

#yg drop tidak perlu disertakan
FFlayoutpath = FF[FF.ACTION.isin(['match', 'newB'])].filter(['CODE_A', 'TITLE_A', 'SUBCODE_A', 'SUBTITLE_A', 'CODE_B', 'TITLE_B', 'SUBCODE_B', 'SUBTITLE_B', 'ACTION'], axis=1)

FFlayoutpath.to_csv('process kategori 5 layout path.csv', index = False)

print("H--- %s seconds ---" % (time.time() - start_time))
print('create layout path done')




