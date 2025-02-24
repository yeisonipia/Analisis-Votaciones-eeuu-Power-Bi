import pandas as pd

data = pd.read_excel('counties.xlsx')
data.dropna()
data.drop_duplicates()

for d in data['county']:
    for i in range(len(d)):
        if d[i] in ['?', ' ', '_','-','/',',', '.', '(',')',':',';','[',']','{','}','&', '%', '#']:
            data['county'] = data['county'].str.replace(d[i], '')

data.to_excel('counties_clean.xlsx', index=False)

    
    