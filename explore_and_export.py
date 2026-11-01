import os
import duckdb


def main():
    warehouse_dir = os.path.abspath("./local_warehouse")

    if not os.path.exists(warehouse_dir):
        print(
            f"Błąd: Nie znaleziono folderu {warehouse_dir}. Uruchom najpierw potok ETL!"
        )
        return

    # Inicjalizacja połączenia DuckDB
    con = duckdb.connect()

    # Ładowanie rozszerzenia do obsługi Delta Lake
    con.sql("INSTALL delta; LOAD delta;")

    # Automatyczne wykrycie baz/katalogów (np. ecommerce_silver.db, ecommerce_gold.db)
    print("--- WYKRYTE TABELE W LOCAL WAREHOUSE ---")
    schemas = [
        d
        for d in os.listdir(warehouse_dir)
        if os.path.isdir(os.path.join(warehouse_dir, d))
    ]

    for schema in schemas:
        schema_path = os.path.join(warehouse_dir, schema)
        layer_name = schema.replace(".db", "")
        print(f"\n[Warstwy/Baza: {layer_name}]")

        tables = [
            t
            for t in os.listdir(schema_path)
            if os.path.isdir(os.path.join(schema_path, t))
        ]
        for table in tables:
            table_full_path = os.path.join(schema_path, table).replace("\\", "/")
            # Rejestracja jako widok w DuckDB, np. silver_orders, gold_fact_sales
            view_name = f"{layer_name}_{table}"
            con.sql(
                f"CREATE VIEW {view_name} AS SELECT * FROM delta_scan('{table_full_path}')"
            )
            print(f"  - Tabela dostępna w SQL jako: {view_name}")

    # Automatyczny eksport warstwy Gold do Power BI (format Parquet)
    gold_export_dir = os.path.abspath("./powerbi_ready")
    os.makedirs(gold_export_dir, exist_ok=True)

    print("\n--- EKSPORT TABEL GOLD DO POWER BI (FORMAT PARQUET) ---")
    gold_schema_dir = os.path.join(warehouse_dir, "ecommerce_gold.db")
    if os.path.exists(gold_schema_dir):
        for table in os.listdir(gold_schema_dir):
            t_path = os.path.join(gold_schema_dir, table).replace("\\", "/")
            if os.path.isdir(t_path):
                out_file = os.path.join(gold_export_dir, f"{table}.parquet").replace(
                    "\\", "/"
                )
                con.sql(
                    f"COPY (SELECT * FROM delta_scan('{t_path}')) TO '{out_file}' (FORMAT PARQUET)"
                )
                print(f"Wyeksportowano: {table}.parquet -> ./powerbi_ready/")

    print("\n" + "=" * 60)
    print(
        "GOTOWE! Możesz teraz pisać zapytania SQL (np. 'SELECT * FROM gold_fact_sales LIMIT 5;')"
    )
    print("Wpisz 'exit', aby zakończyć.")
    print("=" * 60 + "\n")

    # Interaktywna konsola SQL
    while True:
        try:
            query = input("SQL> ").strip()
            if not query:
                continue
            if query.lower() in ("exit", "quit"):
                break

            # Wykonanie zapytania użytkownika
            result = con.sql(query)
            if result:
                result.show()
        except Exception as e:
            print(f"Błąd SQL: {e}")


if __name__ == "__main__":
    main()
