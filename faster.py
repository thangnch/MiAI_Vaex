import vaex
import gc
import time

file_path = 'big_data.csv'
print("Read and convert to h5 file ", file_path)
start = time.time()
df = vaex.from_csv(file_path, convert=True)
print("Process time = ", time.time()-start)

print("Load from file to data frame ")
start = time.time()
df = vaex.open(file_path + ".hdf5")
print("Process time = ", time.time()-start)
#
# print("Show first 10 record")
# print(df.head(10))

print("Try to multiply some columns")
start = time.time()
df['column_test']=df.column_1 * df.column_3
print("Process time = ", time.time()-start)

print("Try to filter by value")
start = time.time()
df = df[df['column_13']>0]
print("Process time = ", time.time()-start)

print("Try to group by column")
start = time.time()
df = df.groupby(df.column_test, agg=vaex.agg.mean(df.column_1))
print("Process time = ", time.time()-start)

df = None
gc.collect()