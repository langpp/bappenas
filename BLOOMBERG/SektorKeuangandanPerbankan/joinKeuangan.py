import pandas as pd
import os.path
frames=[]
sheet=[0,3,2,3,1,3,2,4,1,1,2,5,2,2,1,1,3]
total=0

for i in range(1,len(sheet)):
	for j in range(1,sheet[i]+1):
		total+=1

now=0.0
for i in range(1,len(sheet)):
	for j in range(1,sheet[i]+1):
		if os.path.isfile('SektorKeuanganPerbankan%d_%d.csv'%(i,j))==False:
			continue
		df=pd.read_csv('SektorKeuanganPerbankan%d_%d.csv'%(i,j), header=0)
		df.columns = ['grafik','nama','date','value','frekuensi','satuan']
		df=df.dropna()
		now+=1
		print now/total*100, '% completed'
		frames.append(df)	
		fo = open("joinKeuanganProgress.txt", "w")
		fo.truncate(0)
		fo.seek(0)
		fo.write("%s"%(now/total*100))
		fo.close()
result=pd.concat(frames,ignore_index=True,axis=0)
result.to_csv('C:/Users/Administrator/Dropbox/robot/AllIsWell/SektorKeuanganPerbankanAll.csv')
print "completed"