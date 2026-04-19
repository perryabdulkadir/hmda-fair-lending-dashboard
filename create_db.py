import duckdb
import os

conn = duckdb.connect("data/hmda_2024.duckdb")

tables = {
    "agg_approval_by_race":    "data/agg_approval_by_race.csv",
    "agg_loan_metrics":        "data/agg_loan_metrics.csv",
    "agg_denial_reasons":      "data/agg_denial_reasons.csv",
    "agg_income_ami":          "data/agg_income_ami.csv",
}

for table_name, csv_path in tables.items():
    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"{table_name}: {count} rows loaded")

conn.close()
print("Done. hmda_2024.duckdb created.")