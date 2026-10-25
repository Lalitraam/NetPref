import pandas as pd

from config import DATA_DIR

data_path = DATA_DIR / "UNSW_NB15_training-set.parquet"
if not data_path.exists():
    print(f"Data file not found at {data_path}. Please place the dataset in data/")
else:
    df = pd.read_parquet(data_path)
    print("Shape:", df.shape)
    print("\nData Types:\n", df.dtypes.to_string())
    print("\nSample Rows:\n", df.head(3).T.to_string())