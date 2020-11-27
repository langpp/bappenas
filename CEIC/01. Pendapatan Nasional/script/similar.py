# -*- coding: utf-8 -*-
"""
Created on Wed Nov 18 16:19:25 2020

@author: ASUS
"""

import n0similarities as n0

a = n0.get_levenshtein_similarity('(Setop Rilis)Produk Domestik Bruto: SNA 1993: 2000p: Non Migas: Maluku Utara', 'Maluku Utara')

print(a)
#print(float(a))