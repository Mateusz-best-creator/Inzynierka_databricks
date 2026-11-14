import csv
import json
import pandas as pd
from datetime import datetime, date
from utils import format_csv_value


def write_csv_file(destination: str, data: list):
    if not data:
        return
    field_names = data[0].keys()
    with open(destination, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        for row in data:
            writer.writerow(
                {key: format_csv_value(value) for key, value in row.items()}
            )


def write_json_file(destination: str, data: list):
    if not data:
        return
    serialized_data = []
    for row in data:
        new_row = {}
        for k, v in row.items():
            if isinstance(v, (datetime, date)):
                new_row[k] = (
                    v.strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(v, datetime)
                    else v.strftime("%Y-%m-%d")
                )
            else:
                new_row[k] = v
        serialized_data.append(new_row)

    with open(destination, "w", encoding="utf-8") as file:
        json.dump(serialized_data, file, ensure_ascii=False)


def write_parquet_file(destination: str, data: list):
    if not data:
        return
    df = pd.DataFrame(data)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype("datetime64[us]")

    df.to_parquet(destination, index=False)
