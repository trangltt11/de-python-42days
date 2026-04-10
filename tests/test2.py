import pyarrow as pa

codes_sorted = pa.array([1, 2, 2, 5, 6])
x = 0
prev = pa.array([None], type=pa.int64())

while x < len(codes_sorted)-1:
    new = pa.array([codes_sorted[x].as_py()], type=pa.int64())
    print(new)
    prev = pa.concat_arrays([prev, new])
    x = x + 1

print(prev)