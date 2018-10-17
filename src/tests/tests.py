import pandas as pd
df = pd.DataFrame({"A": [1, 2, 3, 4, 5]})
s = pd.Series([1, 2, 3])
df[['c','d','e']] = df.apply(lambda x: s, axis=1)
u = df['A'].unique()
pass
