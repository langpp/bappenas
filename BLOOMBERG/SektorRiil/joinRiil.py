import pandas as pd
import os.path
frames=[]
sheet=[0,1,1,2,6,6,8,4,4,8,8,8,9,9,1,2,1,4,6,4,10,34,34,8,1,34,34,34,34,7]
total=0

for i in range(1,len(sheet)):
	for j in range(1,sheet[i]+1):
		total+=1

print (total)

now=0.0
for i in range(1,len(sheet)):
	for j in range(1,sheet[i]+1):
		if os.path.isfile('SektorRiil%d_%d.csv'%(i,j))==False:
			continue
		df=pd.read_csv('SektorRiil%d_%d.csv'%(i,j), header=0)
		df.columns = ['grafik','nama','date','value','frekuensi','satuan']
		df=df.dropna()
		now+=1
		print (now/total*100, '% completed')
		frames.append(df)	
		fo = open("joinRiilProgress.txt", "w")
		fo.truncate(0)
		fo.seek(0)
		fo.write("%s"%(now/total*100))
		fo.close()
result=pd.concat(frames,ignore_index=True,axis=0)
result.to_csv('C:/Users/Administrator/Dropbox/robot/AllIsWell/SektorRiilAll.csv')
print ("completed")