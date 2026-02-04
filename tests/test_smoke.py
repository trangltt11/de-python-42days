from pathlib import Path
path:Path=Path(r"D:\python tutorial\de-python-42days\data\bad\day6_bad_records.jsonl")
path.parent.mkdir(parents=True, exist_ok=True)

file_path = path / "day6_bad_records.jsonl"

if file_path.exists() and file_path.is_file():
    print("File tồn tại:", file_path)
else:
    print("Không tồn tại")


from datetime import date

d = date(2026, 1, 13)
s = d.strftime("%Y-%m-%d")
print(s)  # "2026-01-13"
s = d.strftime("%Y-%m-%d")