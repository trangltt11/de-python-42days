from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as pajson
import pyarrow.parquet as pq
import pandas as pd

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
    # parse ISO8601 -> timestamp (nếu lỗi -> null)
    ts_parsed = pc.strptime(ts_str, format="%Y-%m-%dT%H:%M:%S%z", unit="ns", error_is_null=True)

    # Convert timezone (strptime trả timestamp without tz or with offset handling tùy version)
    # Ta chuẩn hoá về Asia/Bangkok nếu tzinfo chưa có:
    # (nếu bạn thấy schema ts chưa có tz, có thể bỏ 2 dòng dưới và giữ timestamp("ns") thôi)
    # NOTE: Một số version pyarrow không hỗ trợ tz trực tiếp ở strptime như mong muốn.
    # Cách an toàn: giữ ts_parsed dạng timestamp("ns") rồi set tz khi viết schema.
    # Ở đây mình cast lại theo schema target (ts), nếu không phù hợp thì sẽ raise -> bạn sẽ thấy ngay.
    ts_final = pc.cast(ts_parsed, pa.timestamp("ns", tz="Asia/Bangkok"), safe=False)
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
    print("\n=== df ===")
    print (df)
    """Level 3 (vừa → khó)

        Không dùng pandas để dedupe nữa:

        Tự viết dedupe bằng Arrow (gợi ý: dùng dictionary encode + first occurrence mask).
        (Nếu bạn muốn, mình sẽ đưa version arrow-only.)"""
    

if __name__ == "__main__":
    main()