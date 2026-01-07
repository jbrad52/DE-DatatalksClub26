#import our libraries
import sys
import pandas as pd

#Set month to 1st arguement and print
month = int(sys.argv[1])
print(f"Running pipeline for month: {month}")

#create and set dataframe
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
df['month'] = month
print(df.head())

#export to parquet format
df.to_parquet(f"output_month_{sys.argv[1]}.parquet")