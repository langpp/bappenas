import pandas as pd
from pandasql import sqldf


def transpose_wilayah(Fdata, Flayoutpath):
    
    #list column yg berisi nilai
    valuecols = list(Fdata.columns)
    valuecols.remove('IDDESA')
    valuecols.remove('KODE_PROV')
    valuecols.remove('NAMA_PROV')
    valuecols.remove('KODE_KAB')
    valuecols.remove('NAMA_KAB')
    valuecols.remove('KODE_KEC')
    valuecols.remove('NAMA_KEC')
    valuecols.remove('KODE_DESA')
    valuecols.remove('NAMA_DESA')
    
    #untuk output
    Ftransposed = pd.DataFrame(columns = Fdata.columns)
    
    #ACTION = match
    
    Ftmp = sqldf('''
    SELECT 
        Fdata.* 
    FROM 
        Fdata, Flayoutpath 
    WHERE 
        (Flayoutpath.ACTION = "match") AND
        (Fdata.KODE_PROV = Flayoutpath.PROVCODE_A) AND
        (Fdata.NAMA_PROV = Flayoutpath.PROVNAME_A) AND
        (Fdata.KODE_KAB = Flayoutpath.KOTCODE_A) AND
        (Fdata.NAMA_KAB = Flayoutpath.KOTNAME_A) AND
        (Fdata.KODE_KEC = Flayoutpath.KECCODE_A) AND
        (Fdata.NAMA_KEC = Flayoutpath.KECNAME_A) AND
        (Fdata.KODE_DESA = Flayoutpath.KELCODE_A) AND
        (Fdata.NAMA_DESA = Flayoutpath.KELNAME_A)
    ''')
    
    if len(Ftmp.index) > 0:
        Ftransposed = Ftransposed.append(Ftmp, ignore_index=True)
    
    
    #ACTION = membelah
    
    Ftmp = sqldf('''
    SELECT 
        '' || Flayoutpath.PROVCODE_B || Flayoutpath.KOTCODE_B || substr('000' || Flayoutpath.KECCODE_B, -3, 3) || substr('000' || Flayoutpath.KELCODE_B, -3, 3) AS IDDESA,
        Flayoutpath.PROVCODE_B AS KODE_PROV,
        Flayoutpath.PROVNAME_B AS NAMA_PROV,
        Flayoutpath.KOTCODE_B AS KODE_KAB,
        Flayoutpath.KOTNAME_B AS NAMA_KAB,
        Flayoutpath.KECCODE_B AS KODE_KEC,
        Flayoutpath.KECNAME_B AS NAMA_KEC,
        Flayoutpath.KELCODE_B AS KODE_DESA,
        Flayoutpath.KELNAME_B AS NAMA_DESA
    FROM 
        Fdata, Flayoutpath 
    WHERE 
        (Flayoutpath.ACTION = "membelah") AND
        (Fdata.KODE_PROV = Flayoutpath.PROVCODE_A) AND
        (Fdata.NAMA_PROV = Flayoutpath.PROVNAME_A) AND
        (Fdata.KODE_KAB = Flayoutpath.KOTCODE_A) AND
        (Fdata.NAMA_KAB = Flayoutpath.KOTNAME_A) AND
        (Fdata.KODE_KEC = Flayoutpath.KECCODE_A) AND
        (Fdata.NAMA_KEC = Flayoutpath.KECNAME_A) AND
        (Fdata.KODE_DESA = Flayoutpath.KELCODE_A) AND
        (Fdata.NAMA_DESA = Flayoutpath.KELNAME_A)
    ''')
    
    if len(Ftmp.index) > 0:
        Ftransposed = Ftransposed.append(Ftmp, ignore_index=True)
    
    
    #ACTION = membelah-keepvalue
    
    Fdatavaluecols = ['Fdata.' + s for s in valuecols]
    
    Ftmp = sqldf('''
    SELECT 
        '' || Flayoutpath.PROVCODE_B || Flayoutpath.KOTCODE_B || substr('000' || Flayoutpath.KECCODE_B, -3, 3) || substr('000' || Flayoutpath.KELCODE_B, -3, 3) AS IDDESA,
        Flayoutpath.PROVCODE_B AS KODE_PROV,
        Flayoutpath.PROVNAME_B AS NAMA_PROV,
        Flayoutpath.KOTCODE_B AS KODE_KAB,
        Flayoutpath.KOTNAME_B AS NAMA_KAB,
        Flayoutpath.KECCODE_B AS KODE_KEC,
        Flayoutpath.KECNAME_B AS NAMA_KEC,
        Flayoutpath.KELCODE_B AS KODE_DESA,
        Flayoutpath.KELNAME_B AS NAMA_DESA,
        ''' + ','.join(Fdatavaluecols) + '''
    FROM 
        Fdata, Flayoutpath 
    WHERE 
        (Flayoutpath.ACTION = "membelah-keepvalue") AND
        (Fdata.KODE_PROV = Flayoutpath.PROVCODE_A) AND
        (Fdata.NAMA_PROV = Flayoutpath.PROVNAME_A) AND
        (Fdata.KODE_KAB = Flayoutpath.KOTCODE_A) AND
        (Fdata.NAMA_KAB = Flayoutpath.KOTNAME_A) AND
        (Fdata.KODE_KEC = Flayoutpath.KECCODE_A) AND
        (Fdata.NAMA_KEC = Flayoutpath.KECNAME_A) AND
        (Fdata.KODE_DESA = Flayoutpath.KELCODE_A) AND
        (Fdata.NAMA_DESA = Flayoutpath.KELNAME_A)
    ''')
    
    if len(Ftmp.index) > 0:
        Ftransposed = Ftransposed.append(Ftmp, ignore_index=True)
    
    
    
    #ACTION = bergabung
    
    Fdatavaluecols = ['sum(Fdata.' + s + ') AS ' + s for s in valuecols]
    
    Ftmp = sqldf('''
    SELECT 
        '' || Flayoutpath.PROVCODE_B || Flayoutpath.KOTCODE_B || substr('000' || Flayoutpath.KECCODE_B, -3, 3) || substr('000' || Flayoutpath.KELCODE_B, -3, 3) AS IDDESA,
        Flayoutpath.PROVCODE_B AS KODE_PROV,
        Flayoutpath.PROVNAME_B AS NAMA_PROV,
        Flayoutpath.KOTCODE_B AS KODE_KAB,
        Flayoutpath.KOTNAME_B AS NAMA_KAB,
        Flayoutpath.KECCODE_B AS KODE_KEC,
        Flayoutpath.KECNAME_B AS NAMA_KEC,
        Flayoutpath.KELCODE_B AS KODE_DESA,
        Flayoutpath.KELNAME_B AS NAMA_DESA,
        ''' + ','.join(Fdatavaluecols) + '''
    FROM 
        Fdata, Flayoutpath 
    WHERE 
        (Flayoutpath.ACTION = "bergabung") AND
        (Fdata.KODE_PROV = Flayoutpath.PROVCODE_A) AND
        (Fdata.NAMA_PROV = Flayoutpath.PROVNAME_A) AND
        (Fdata.KODE_KAB = Flayoutpath.KOTCODE_A) AND
        (Fdata.NAMA_KAB = Flayoutpath.KOTNAME_A) AND
        (Fdata.KODE_KEC = Flayoutpath.KECCODE_A) AND
        (Fdata.NAMA_KEC = Flayoutpath.KECNAME_A) AND
        (Fdata.KODE_DESA = Flayoutpath.KELCODE_A) AND
        (Fdata.NAMA_DESA = Flayoutpath.KELNAME_A)
    GROUP BY
        Fdata.KODE_PROV, Fdata.KODE_KAB, Fdata.KODE_KEC, Flayoutpath.ACTION
    ''')
    
    if len(Ftmp.index) > 0:
        Ftransposed = Ftransposed.append(Ftmp, ignore_index=True)
    
    #ACTION = newB
    
    Ftmp = sqldf('''
        SELECT
            '' || Flayoutpath.PROVCODE_B || Flayoutpath.KOTCODE_B || substr('000' || Flayoutpath.KECCODE_B, -3, 3) || substr('000' || Flayoutpath.KELCODE_B, -3, 3) AS IDDESA,
            Flayoutpath.PROVCODE_B AS KODE_PROV,
            Flayoutpath.PROVNAME_B AS NAMA_PROV,
            Flayoutpath.KOTCODE_B AS KODE_KAB,
            Flayoutpath.KOTNAME_B AS NAMA_KAB,
            Flayoutpath.KECCODE_B AS KODE_KEC,
            Flayoutpath.KECNAME_B AS NAMA_KEC,
            Flayoutpath.KELCODE_B AS KODE_DESA,
            Flayoutpath.KELNAME_B AS NAMA_DESA
        FROM
            Flayoutpath
        WHERE
            Flayoutpath.ACTION = "newB"
    ''')
    
    if len(Ftmp.index) > 0:
        Ftransposed = Ftransposed.append(Ftmp, ignore_index=True)
    
    return Ftransposed
    
    
def transpose_kategori(Fdata, Flayoutpath):
    Fdata = Fdata.transpose()
    
    Fdata['kategori'] = Fdata.index
    
    #list column yg berisi nilai
    valuecols = list(Fdata.columns)
    valuecols.remove('kategori')
    
    #row wilayah
    
    Ftransposed = sqldf('''
    SELECT
        Fdata.*
    FROM
        Fdata
    WHERE
        (Fdata.kategori = 'IDDESA') OR
        (Fdata.kategori = 'KODE_PROV') OR
        (Fdata.kategori = 'NAMA_PROV') OR
        (Fdata.kategori = 'KODE_KAB') OR
        (Fdata.kategori = 'NAMA_KAB') OR
        (Fdata.kategori = 'KODE_KEC') OR
        (Fdata.kategori = 'NAMA_KEC') OR
        (Fdata.kategori = 'KODE_DESA') OR
        (Fdata.kategori = 'NAMA_DESA')
    ''')
    
    ##########################################################
    ##########################################################
    
    #ACTION = match
    
    Flayoutpath_match = sqldf('''
        SELECT * FROM Flayoutpath WHERE ACTION = 'match'
    ''')
    
    Fdata_match = sqldf('''
        SELECT 
            Fdata.*,
            Flayoutpath_match.CODE_B AS newkategori
        FROM 
            Fdata, Flayoutpath_match 
        WHERE
            Fdata.kategori = Flayoutpath_match.CODE_A
    ''')
    
    #newkategori_to_append = sqldf('''
    #    SELECT
    #        Flayoutpath_match.CODE_B AS kategori
    #    FROM
    #        Flayoutpath_match
    #    LEFT JOIN
    #        Ftransposed
    #    ON
    #        Flayoutpath_match.CODE_B = Ftransposed.kategori
    #    WHERE
    #        Ftransposed.kategori is NULL
    #''')
    
    #Ftransposed = Ftransposed.append(newkategori_to_append, ignore_index=True)
    
    #------------------------------------------------------
    
    ##untuk
    ##  layoutpath.subcode_A == (NA atau "-") dan
    ##  layoutpath.subcode_B == (NA atau "-")
    
    tmp = ['Fdata_match."' + str(s) + '" AS "' + str(s) +'"' for s in valuecols]
    
    Fdata_match_na_to_na = sqldf('''
        SELECT
            ''' + ','.join(tmp) + ''',
            Fdata_match.newkategori AS kategori
        FROM
            Fdata_match, Flayoutpath_match
        WHERE
            (Fdata_match.kategori = Flayoutpath_match.CODE_A) AND
            (Fdata_match.newkategori = Flayoutpath_match.CODE_B) AND
            (Flayoutpath_match.SUBCODE_A = '' OR Flayoutpath_match.SUBCODE_A = '-' OR Flayoutpath_match.SUBCODE_A IS NULL) AND
            (Flayoutpath_match.SUBCODE_B = '' OR Flayoutpath_match.SUBCODE_B = '-' OR Flayoutpath_match.SUBCODE_B IS NULL)
    ''')
    
    if len(Fdata_match_na_to_na.index) > 0:
        Ftransposed = Ftransposed.append(Fdata_match_na_to_na, ignore_index=True)
    
    ##untuk
    ##  layoutpath.subcode_A != (NA atau "-") dan
    ##  layoutpath.subcode_B == (NA atau "-")
    
    tmp = ['CASE Fdata_match."' + str(s) + '" WHEN Flayoutpath_match.SUBCODE_A THEN Flayoutpath_match.SUBCODE_A ELSE NULL END AS "' + str(s) +'"' for s in valuecols]
    
    Fdata_match_v_to_na1 = sqldf('''
        SELECT
            ''' + ','.join(tmp) + ''',
            Fdata_match.newkategori AS kategori
        FROM
            Fdata_match, Flayoutpath_match
        WHERE
            (Fdata_match.kategori = Flayoutpath_match.CODE_A) AND
            (Fdata_match.newkategori = Flayoutpath_match.CODE_B) AND
            (Flayoutpath_match.SUBCODE_A != '' AND Flayoutpath_match.SUBCODE_A != '-' AND Flayoutpath_match.SUBCODE_A IS NOT NULL) AND
            (Flayoutpath_match.SUBCODE_B = '' OR Flayoutpath_match.SUBCODE_B = '-' OR Flayoutpath_match.SUBCODE_B IS NULL)
    ''')
    
    tmp2 = ['MIN(Fdata_match_v_to_na1."'+str(s)+'") AS "'+str(s)+'" ' for s in valuecols]
    Fdata_match_v_to_na2 = sqldf('''
        SELECT
            ''' + ','.join(tmp2) + ''',
            Fdata_match_v_to_na1.kategori
        FROM
            Fdata_match_v_to_na1
        GROUP BY
            Fdata_match_v_to_na1.kategori
    ''')
    
    if len(Fdata_match_v_to_na2.index) > 0:
        Ftransposed = Ftransposed.append(Fdata_match_v_to_na2, ignore_index=True)
    
    
    ##untuk
    ##  layoutpath.subcode_A == (NA atau "-") dan
    ##  layoutpath.subcode_B != (NA atau "-")
    
    tmp = ['Flayoutpath_match.SUBCODE_B AS "' + str(s) +'"' for s in valuecols]
    
    Fdata_match_na_to_v = sqldf('''
        SELECT
            ''' + ','.join(tmp) + ''',
            Fdata_match.newkategori AS kategori
        FROM
            Fdata_match, Flayoutpath_match
        WHERE
            (Fdata_match.kategori = Flayoutpath_match.CODE_A) AND
            (Fdata_match.newkategori = Flayoutpath_match.CODE_B) AND
            (Flayoutpath_match.SUBCODE_A = '' OR Flayoutpath_match.SUBCODE_A = '-' OR Flayoutpath_match.SUBCODE_A IS NULL) AND
            (Flayoutpath_match.SUBCODE_B != '' AND Flayoutpath_match.SUBCODE_B != '-' AND Flayoutpath_match.SUBCODE_B IS NOT NULL)
    ''')
    
    if len(Fdata_match_na_to_v.index) > 0:
        Ftransposed = Ftransposed.append(Fdata_match_na_to_v, ignore_index=True)
    
    ##untuk
    ##  layoutpath.subcode_A != (NA atau "-") dan
    ##  layoutpath.subcode_B != (NA atau "-")
    
    tmp = ['CASE Fdata_match."' + str(s) + '" WHEN Flayoutpath_match.SUBCODE_A THEN Flayoutpath_match.SUBCODE_B ELSE NULL END AS "' + str(s) +'"' for s in valuecols]
    
    Fdata_match_v_to_v1 = sqldf('''
        SELECT
            ''' + ','.join(tmp) + ''',
            Fdata_match.newkategori AS kategori
        FROM
            Fdata_match, Flayoutpath_match
        WHERE
            (Fdata_match.kategori = Flayoutpath_match.CODE_A) AND
            (Fdata_match.newkategori = Flayoutpath_match.CODE_B) AND
            (Flayoutpath_match.SUBCODE_A != '' AND Flayoutpath_match.SUBCODE_A != '-' AND Flayoutpath_match.SUBCODE_A IS NOT NULL) AND
            (Flayoutpath_match.SUBCODE_B != '' AND Flayoutpath_match.SUBCODE_B != '-' AND Flayoutpath_match.SUBCODE_B IS NOT NULL)
    ''')
    
    tmp2 = ['MIN(Fdata_match_v_to_v1."'+str(s)+'") AS "'+str(s)+'" ' for s in valuecols]
    Fdata_match_v_to_v2 = sqldf('''
        SELECT
            ''' + ','.join(tmp2) + ''',
            Fdata_match_v_to_v1.kategori
        FROM
            Fdata_match_v_to_v1
        GROUP BY
            Fdata_match_v_to_v1.kategori
    ''')
    
    if len(Fdata_match_v_to_v2.index) > 0:
        Ftransposed = Ftransposed.append(Fdata_match_v_to_v2, ignore_index=True)
    
    ##########################################################
    ##########################################################
    
    #ACTION = newB
    
    Flayoutpath_newB = sqldf('''
        SELECT CODE_B as kategori FROM Flayoutpath WHERE ACTION = 'newB'
    ''')
    
    if len(Flayoutpath_newB.index) > 0:
        Ftransposed = Ftransposed.append(Flayoutpath_newB, ignore_index=True)
    
    ##########################################################
    ##########################################################
    
    
    Ftransposed.set_index('kategori', inplace=True)
    
    return Ftransposed.transpose()
    
    
    
    
    
    
    