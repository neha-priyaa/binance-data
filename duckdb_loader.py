import duckdb
import os
import zipfile
import tempfile
from pathlib import Path


def push_to_database(directory: str, db_path: str = "binance_data.db"):
    """
    Push data from a directory into DuckDB.

    Args:
        directory: Path to the directory containing data files (zip files with CSVs)
        db_path: Path where the DuckDB database file will be created
    """
    con = duckdb.connect(db_path)

    zip_files = list(Path(directory).rglob("*.zip"))

    for zip_file in zip_files:
        try:
            # Extract timeframe from zip filename (e.g., "IOTAUSDT-1h-2025-01.zip" -> "1h")
            zip_stem = zip_file.stem  # removes .zip
            # Pattern: SYMBOL-TIMEFRAME-DATE
            parts = zip_stem.split('-')
            timeframe = parts[1] if len(parts) >= 2 else 'unknown'

            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.csv'):
                        symbol = zip_file.parent.name
                        table_name = symbol.replace('-', '_')

                        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
                            tmp_file.write(zip_ref.read(file_name))
                            tmp_path = tmp_file.name

                        try:
                            # Check if table exists
                            table_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0] > 0

                            if not table_exists:
                                con.execute(f"""
                                    CREATE TABLE {table_name} AS
                                    SELECT *,
                                        '{symbol}' as symbol,
                                        '{timeframe}' as timeframe
                                    FROM read_csv_auto(
                                        '{tmp_path}',
                                        columns={{
                                            'open_time': 'BIGINT',
                                            'open': 'DOUBLE',
                                            'high': 'DOUBLE',
                                            'low': 'DOUBLE',
                                            'close': 'DOUBLE',
                                            'volume': 'DOUBLE',
                                            'close_time': 'BIGINT',
                                            'quote_volume': 'DOUBLE',
                                            'trades': 'BIGINT',
                                            'taker_buy_volume': 'DOUBLE',
                                            'taker_buy_quote_volume': 'DOUBLE',
                                            'ignore': 'DOUBLE'
                                        }},
                                        header=False
                                    )
                                """)
                            else:
                                con.execute(f"""
                                    INSERT INTO {table_name}
                                    SELECT *,
                                        '{symbol}' as symbol,
                                        '{timeframe}' as timeframe
                                    FROM read_csv_auto(
                                        '{tmp_path}',
                                        columns={{
                                            'open_time': 'BIGINT',
                                            'open': 'DOUBLE',
                                            'high': 'DOUBLE',
                                            'low': 'DOUBLE',
                                            'close': 'DOUBLE',
                                            'volume': 'DOUBLE',
                                            'close_time': 'BIGINT',
                                            'quote_volume': 'DOUBLE',
                                            'trades': 'BIGINT',
                                            'taker_buy_volume': 'DOUBLE',
                                            'taker_buy_quote_volume': 'DOUBLE',
                                            'ignore': 'DOUBLE'
                                        }},
                                        header=False
                                    )
                                """)
                        finally:
                            os.unlink(tmp_path)
            print(f"Loaded: {zip_file}")
        except Exception as e:
            print(f"Error loading {zip_file}: {e}")

    con.close()
    print(f"Database created at: {db_path}")


if __name__ == "__main__":
    push_to_database("data")
