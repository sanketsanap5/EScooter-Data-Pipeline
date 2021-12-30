import pandas as pd

df = pd.read_csv("/Users/Sanket/Documents/GitHub/Real-Time-Data-Pipeline/dataset/scooter.csv")
df.sample(5)

#Exploratery data analysis - EDA
df.describe()
df['month'].value_counts()
df['DURATION'].value_counts(dropna=False)*100

#Droping/Filling rows/columns
df.isnull().sum()
df.columns = [x.lower() for x in df.columns]
startValues = {'start_location_name':'Start'}
endValues = {'end_location_name':'End'}
newdf=df[df['start_location_name'].isnull()].fillna(value=startValues)
df.loc[newdf.index] = newdf
newdf=df[df['end_location_name'].isnull()].fillna(value=endValues)
df.loc[newdf.index] = newdf
df[df['end_location_name'].isnull()]
df[df['start_location_name'].isnull()]
newdf = df[df['duration'].isnull()].fillna(value="00:00:00")
df.loc[newdf.index] = newdf
df[df['duration'].isnull()]

#Creating/Modifying Columns
df.columns = [x.lower() for x in df.columns]
df.rename(columns={'DURATION':'duration'},inplace=True)
#df.loc[df['trip_id']==1613335,'newcol']='True'
#df[['trip_id','newcol']].head()
newdf=df['started_at'].head()
newdf.str.split(expand=True)
df.head()


df['started_at'] = pd.to_datetime(df['started_at'],format='%m/%d/%Y %H:%M')
df['ended_at'] = pd.to_datetime(df['ended_at'],format='%m/%d/%Y %H:%M')
df.dtypes

print(df.sample(10))
df.sample(10)
