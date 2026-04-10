import pyarrow as pa
import pyarrow.compute as pc

arr = pa.array(["2026-01-10", "2026-01-11"])

# Case 1: loop trực tiếp
print("---- Loop Arrow Array ----")
for key in arr:
    print(key, type(key))

# Case 2: dùng to_pylist()
print("\n---- Loop Python list ----")
for key in arr.to_pylist():
    print(key, type(key))