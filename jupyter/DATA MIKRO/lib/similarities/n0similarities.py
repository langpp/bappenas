#import step
import Levenshtein


#--------------------------------------------------------------------
# Levenshtein Distance
# 1 = sama persis. 0 = sangat berbeda

def get_levenshtein_similarity(s1, s2):
	return Levenshtein.ratio(s1, s2)


#-------------------------------------------------------------
# Jaro Distance
# 1 = sama persis. 0 = sangat berbeda

def get_jaro_similarity(s1, s2):
	return Levenshtein.jaro(s1, s2)


#-----------------------------------------------------------
# Jaro-Winkler Distance
# 1 = sama persis. 0 = sangat berbeda

def get_jaro_winkler_similarity(s1, s2):
	return Levenshtein.jaro_winkler(s1, s2)



