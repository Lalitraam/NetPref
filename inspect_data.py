import pandas as pd

df = pd.read_parquet("data/UNSW_NB15_training-set.parquet")
print(df.shape)
print(df.dtypes.to_string())
print(df.head(3).T.to_string())