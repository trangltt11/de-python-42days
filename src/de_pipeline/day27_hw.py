from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pajson
import pyarrow.parquet as pq
import pandas as pd
import shutil

def first_occurrence_indices(table: pa.Table, key_col: str) -> pa.Array:
    table = table.combine_chunks()
    key_arr = table[key_col].combine_chunks()
    # 1) encode key -> codes
    codes = pc.dictionary_encode(key_arr).indices
    print("------------------------codes--------------")
    print(codes)
    # 2) sort theo codes
    order = pc.sort_indices(codes)
    codes_sorted = pc.take(codes, order)

    # 3) first occurrence mask trên codes_sorted
    #prev = pc.shift(codes_sorted, 1)
    x = 0
    prev = pa.array([None], type=pa.int64())

    while x < len(codes_sorted)-1:
        new = pa.array([codes_sorted[x].as_py()], type=pa.int64())
        prev = pa.concat_arrays([prev, new])
        x = x + 1

    print("-----------------------prev---------------------")
    print (prev)
    print(codes_sorted) 
    is_first_sorted = pc.if_else(
    pc.is_null(prev),
    pa.scalar(True),
    pc.not_equal(codes_sorted, prev)
)
    print("------------------------------is_first_sorted------------")
    print(is_first_sorted)

    # 4) lấy index (row index) của các dòng first (theo bảng gốc)
    first_pos_sorted = pc.filter(order, is_first_sorted)
    print("------------------------------first_pos_sorted------------")
    print(first_pos_sorted)
    # 5) nếu muốn giữ đúng thứ tự xuất hiện ban đầu
    first_pos = pc.take(first_pos_sorted, pc.sort_indices(first_pos_sorted))
    return first_pos

def dedupe_keep_first(table: pa.Table, key_col: str) -> pa.Table:
    idx = first_occurrence_indices(table, key_col)
    return table.take(idx)
    
def main() -> None:
    root = Path(__file__).resolve().parents[2]
    in_path = root / "data" / "raw" / "day2_events.jsonl"

    # 1) Read JSONL -> Arrow Table
    # pajson.read_json đọc JSON Lines
    table = pajson.read_json(in_path)

    print("=== RAW SCHEMA ===")
    print(table.schema)

    # 2) Define target schema (schema chuẩn bạn muốn)
    target_schema = pa.schema([
        ("event_id", pa.string()),
        ("user_id", pa.string()),
        ("event", pa.string()),
        ("amount", pa.float64()),
        ("ts", pa.timestamp("ns", tz="Asia/Bangkok")),
        ("ingest_date", pa.string()),
    ])

    # 3) Normalize columns: event -> lower/trim, amount -> float, ts -> timestamp
    # 3.1 event: to string -> trim -> lower
    event_col = pc.ascii_lower(pc.utf8_trim_whitespace(pc.cast(table["event"], pa.string())))

    # 3.2 amount: cast to float64 (nếu lỗi -> null), rồi fill null = 0.0
    amount_float = pc.cast(table["amount"], pa.float64(), safe=False)  # safe=False: cố cast, lỗi -> null
    amount_float = pc.if_else(pc.is_null(amount_float), pa.scalar(0.0), amount_float)

    # 3.3 ts: cast sang string trước, rồi parse timestamp
    # JSON reader đôi khi đã parse, nhưng ta làm chắc chắn
    ts_str = pc.cast(table["ts"], pa.string())
    print("===ts_str ===")
    print(ts_str)
    ts_no_tz = pc.utf8_slice_codeunits(ts_str, 0, 19)

    ts_parsed = pc.strptime(
    ts_no_tz,
    format="%Y-%m-%dT%H:%M:%S",
    unit="ns",
    error_is_null=True
)
    # parse ISO8601 -> timestamp (nếu lỗi -> null)
    #ts_parsed = pc.strptime(ts_str,  format="%Y-%m-%dT%H:%M:%S%z", unit="ns", error_is_null=True)
    print("===ts_parsed ===")
    print(ts_parsed)
    # Convert timezone (strptime trả timestamp without tz or with offset handling tùy version)
    # Ta chuẩn hoá về Asia/Bangkok nếu tzinfo chưa có:
    # (nếu bạn thấy schema ts chưa có tz, có thể bỏ 2 dòng dưới và giữ timestamp("ns") thôi)
    # NOTE: Một số version pyarrow không hỗ trợ tz trực tiếp ở strptime như mong muốn.
    # Cách an toàn: giữ ts_parsed dạng timestamp("ns") rồi set tz khi viết schema.
    # Ở đây mình cast lại theo schema target (ts), nếu không phù hợp thì sẽ raise -> bạn sẽ thấy ngay.
    ts_final = pc.cast(ts_parsed, pa.timestamp("ns", tz="Asia/Bangkok"), safe=False)
    print("===ts_final ===")
    print(ts_final)
    ingest_date = pc.utf8_slice_codeunits(ts_str,0,10)
    # 4) Build table theo schema target
    out_table = pa.Table.from_arrays(
        [
            pc.cast(table["event_id"], pa.string()),
            pc.cast(table["user_id"], pa.string()),
            event_col,
            amount_float,
            ts_final,
            ingest_date
        ],
        schema=target_schema,
    )

    print("\n=== TARGET SCHEMA ===")
    print(out_table.schema)
    print("\n=== out_table ===")
    print(out_table)
    table_depu=dedupe_keep_first(out_table,"event_id") 

    print("----------------table_depu---------------------") 

    print(table_depu) 
   

    # 5) Dedupe event_id (giữ dòng đầu): Arrow không có drop_duplicates như pandas
    # Ta làm cách đơn giản: dùng pyarrow.compute để lấy unique mask.
    # Cách phổ biến: convert sang pandas để dedupe (vì dataset nhỏ).
    df = out_table.to_pandas()
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    out_dir = root / "data" / "processed" / "day27"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "events_clean.parquet"

    # 6) Write Parquet với schema ổn định
    pq.write_table(pa.Table.from_pandas(df, schema=target_schema, preserve_index=False), out_file)

    print("\nWrote:", out_file)
    print("Rows:", len(df))

    """Level 2 (vừa)

    Sửa schema: thêm cột ingest_date (string) = YYYY-MM-DD từ ts.
    Gợi ý: dùng pandas hoặc arrow compute strftime."""
    print("\n=== table_depu ===")
    print (table_depu["ts"])
    """Level 3 (vừa → khó)

        Không dùng pandas để dedupe nữa:

        Tự viết dedupe bằng Arrow (gợi ý: dùng dictionary encode + first occurrence mask).
        (Nếu bạn muốn, mình sẽ đưa version arrow-only.)"""
    ts_col = pc.cast(table_depu["ts"], pa.timestamp("ns"))
    print("\n=== ts_col ===")
    print(table_depu["ts"])

    not_null_ts = pc.invert(pc.is_null(ts_col))
    table_depu = table_depu.filter(not_null_ts)
    print("\n=== table_depu ===")
    print (table_depu)
    ts_col = pc.filter(ts_col, not_null_ts)

    print("\n=== ts_col new ===")
    print(ts_col)

    year = pc.utf8_slice_codeunits(ts_str, 0, 4)
    month = pc.utf8_slice_codeunits(ts_str, 5, 7)
    day = pc.utf8_slice_codeunits(ts_str, 8, 10)

    ymd = pc.binary_join_element_wise(year, month, day, "-")
    print("\n=== ymd ===")
    print(ymd)

    unique_keys = pc.unique(ymd)

    # 7) Loop từng partition và write parquet (schema luôn giống nhau)
    for key in unique_keys.to_pylist():
        mask = pc.equal(ymd, pa.scalar(key))
        part = out_table.filter(mask)

        y, m, d = key.split("-")
        part_dir = out_dir / f"year={y}" / f"month={m}" / f"day={d}"

        # idempotent (tuỳ chọn): xoá partition cũ rồi ghi lại
        if part_dir.exists():
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)

        out_file = part_dir / "events.parquet"
        pq.write_table(part, out_file)  # part đã đúng target_schema

        print(f"Wrote {part.num_rows} rows -> {out_file}")

    print("DONE. Output root:", out_dir)

if __name__ == "__main__":
    main()


# doc du lieu file parquet
table = pq.read_table("data/processed/day27/year=2026/month=01/day=11/events.parquet")
print(table)