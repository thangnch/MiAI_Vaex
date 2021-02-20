import pandas as pd
import numpy as np
import gc
import time


rows_count = 500000
columns_count = 500
rand_start = 0
rand_end = 1000

print("Making random DataFrame...")
np_matrix = np.random.randint(rand_start, rand_end, size=(rows_count, columns_count))
df = pd.DataFrame(np_matrix, columns=['column_%d' % i for i in range(columns_count)])
#
# print("Show first 10 record")
# print(df.head(10))

file_path = 'big_data.csv'
print("Save data frame to file ", file_path)
start = time.time()
df.to_csv(file_path, index=False)
print("Process time = ", time.time()-start)

print("Load from file to data frame ")
start = time.time()
df = pd.read_csv(file_path)
(df.head())
print("Process time = ", time.time()-start)

print("Try to multiply some columns")
start = time.time()
df['column_test']=df.column_1 * df.column_3
(df.head())
print("Process time = ", time.time()-start)

print("Try to filter by value")
start = time.time()
df = df[df['column_13']>0]
(df.head())
print("Process time = ", time.time()-start)

print("Try to group by column")
start = time.time()
df = df.groupby(by="column_13").mean()
(df.head())
print("Process time = ", time.time()-start)

df = None
gc.collect()


