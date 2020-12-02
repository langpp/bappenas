import pandas as pd
import lib.wilayah.n2similarities as n2

import time
start_time = time.time()




#read dan merge

F1 = pd.read_excel('input wilayah 2008.xlsx')
F2 = pd.read_excel('input wilayah 2011.xlsx')

FF = n2.get_standard_merged(F1, F2)

print("A--- %s seconds ---" % (time.time() - start_time))

FF.to_csv('process wilayah 1 merge.csv', index = False)

print("B--- %s seconds ---" % (time.time() - start_time))
print('read dan merge done')





#get similarities value

FF = pd.read_csv('process wilayah 1 merge.csv', dtype='str')

print("C--- %s seconds ---" % (time.time() - start_time))

FF = n2.get_similarities_value(FF, method_for_provcode='L', method_for_provname='L', method_for_kotcode='L', method_for_kotname='L', method_for_keccode='L', method_for_kecname='L', method_for_kelcode='L', method_for_kelname='L')

print("D--- %s seconds ---" % (time.time() - start_time))

FF.to_csv('process wilayah 2 similarities.csv', index = False)
print('get similarities value done')




#get similarity score & match

FF = pd.read_csv('process wilayah 2 similarities.csv', dtype={
    'PROVCODE_A':str, 'PROVNAME_A':str, 'KOTCODE_A':str, 'KOTNAME_A':str, 'KECCODE_A':str, 'KECNAME_A':str, 'KELCODE_A':str, 'KELNAME_A':str,
    'PROVCODE_B':str, 'PROVNAME_B':str, 'KOTCODE_B':str, 'KOTNAME_B':str, 'KECCODE_B':str, 'KECNAME_B':str, 'KELCODE_B':str, 'KELNAME_B':str,
    'PROVCODE_SIMILARITIES':float, 'PROVNAME_SIMILARITIES':float,
    'KOTCODE_SIMILARITIES':float, 'KOTNAME_SIMILARITIES':float,
    'KECCODE_SIMILARITIES':float, 'KECNAME_SIMILARITIES':float,
    'KELCODE_SIMILARITIES':float, 'KELNAME_SIMILARITIES':float
    })

print("E--- %s seconds ---" % (time.time() - start_time))


FF = n2.get_most_similar(FF)

FF = n2.get_similarity_score(FF)

FF = n2.get_match(FF, 13)

FF.to_csv('process wilayah 3 similarity score & match.csv', index = False)

print("F--- %s seconds ---" % (time.time() - start_time))

print('get similarity score & match done')



#get recomendation
FF = n2.get_recomendation(FF)

FF.to_csv('process wilayah 4 get recomendation.csv', index = False)

print("G--- %s seconds ---" % (time.time() - start_time))

print('get recomendation done')




#create layout path

FF = pd.read_csv('process wilayah 4 get recomendation.csv', dtype={
    'PROVCODE_A':str, 'PROVNAME_A':str, 'KOTCODE_A':str, 'KOTNAME_A':str, 'KECCODE_A':str, 'KECNAME_A':str, 'KELCODE_A':str, 'KELNAME_A':str,
    'PROVCODE_B':str, 'PROVNAME_B':str, 'KOTCODE_B':str, 'KOTNAME_B':str, 'KECCODE_B':str, 'KECNAME_B':str, 'KELCODE_B':str, 'KELNAME_B':str,
    'PROVCODE_SIMILARITIES':float, 'PROVNAME_SIMILARITIES':float,
    'KOTCODE_SIMILARITIES':float, 'KOTNAME_SIMILARITIES':float,
    'KECCODE_SIMILARITIES':float, 'KECNAME_SIMILARITIES':float,
    'KELCODE_SIMILARITIES':float, 'KELNAME_SIMILARITIES':float,
    'PROVCODE_MOSTSIMILAR_Aisone':bool, 'PROVNAME_MOSTSIMILAR_Aisone':bool,	 'KOTCODE_MOSTSIMILAR_Aisone':bool, 'KOTNAME_MOSTSIMILAR_Aisone':bool, 'KECCODE_MOSTSIMILAR_Aisone':bool, 'KECNAME_MOSTSIMILAR_Aisone':bool, 'KELCODE_MOSTSIMILAR_Aisone':bool, 'KELNAME_MOSTSIMILAR_Aisone':bool,
    'PROVCODE_MOSTSIMILAR_Bisone':bool, 'PROVNAME_MOSTSIMILAR_Bisone':bool, 'KOTCODE_MOSTSIMILAR_Bisone':bool, 'KOTNAME_MOSTSIMILAR_Bisone':bool, 'KECCODE_MOSTSIMILAR_Bisone':bool, 'KECNAME_MOSTSIMILAR_Bisone':bool, 'KELCODE_MOSTSIMILAR_Bisone':bool, 'KELNAME_MOSTSIMILAR_Bisone':bool,
    'SIMILAR_SCORE_Aisone':int,'SIMILAR_SCORE_Bisone':int,'SIMILAR_SCORE_TOTAL':int,
    'IS_MATCH':bool,'RECOMENDATION':str
    })

FF['ACTION'] = FF['RECOMENDATION']

#yg drop tidak perlu disertakan
FFlayoutpath = FF[FF.ACTION.isin(['match', 'membelah', 'membelah-keepvalue', 'bergabung', 'newB'])].filter(['PROVCODE_A', 'PROVNAME_A', 'KOTCODE_A', 'KOTNAME_A', 'KECCODE_A', 'KECNAME_A', 'KELCODE_A', 'KELNAME_A', 'PROVCODE_B', 'PROVNAME_B', 'KOTCODE_B', 'KOTNAME_B', 'KECCODE_B', 'KECNAME_B', 'KELCODE_B', 'KELNAME_B', 'ACTION'], axis=1)

FFlayoutpath.to_csv('process wilayah 5 layout path.csv', index = False)

print("H--- %s seconds ---" % (time.time() - start_time))
print('create layout path done')










