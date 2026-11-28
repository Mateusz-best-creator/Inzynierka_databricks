import csv
from datetime import date, datetime
import json
import pandas as pd
from utils import format_csv_value


def _to_dataframe(data: list[dict]) -> pd.DataFrame:
    """Converts a list of record dictionaries into a DataFrame.

    Casts datetime columns to microsecond precision (datetime64[us]) to ensure
    compatibility with Apache Spark and Parquet/Excel writers.

    Args:
        data: List of dictionaries representing table rows.

    Returns:
        DataFrame containing the provided data with appropriate datetime casting.
    """
    df = pd.DataFrame(data)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype("datetime64[us]")
    return df


def write_csv_file(destination: str, data: list[dict]) -> None:
    """Writes a list of record dictionaries to a CSV file.

    Args:
        destination: Output file path for the generated CSV file.
        data: List of dictionaries representing rows to write.
    """
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


def write_json_file(destination: str, data: list[dict]) -> None:
    """Serializes a list of record dictionaries to a JSON file.

    Formats datetime and date objects into ISO-like string representations
    before writing to disk.

    Args:
        destination: Output file path for the generated JSON file.
        data: List of dictionaries representing records to serialize.
    """
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


def write_parquet_file(destination: str, data: list[dict]) -> None:
    """Writes a list of record dictionaries to a Parquet file.

    Args:
        destination: Output file path for the generated Parquet file.
        data: List of dictionaries representing rows to export.
    """
    if not data:
        return
    df = _to_dataframe(data)
    df.to_parquet(destination, index=False)


def write_excel_file(destination: str, data: list[dict]) -> None:
    """Writes a list of record dictionaries to an Excel workbook.

    Args:
        destination: Output file path for the generated Excel file.
        data: List of dictionaries representing rows to export.
    """
    if not data:
        return
    df = _to_dataframe(data)
    df.to_excel(destination, index=False)
