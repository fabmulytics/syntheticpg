from __future__ import annotations
import random, string
from datetime import datetime, timezone

import polars as pl
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import Settings
from hashing import dv2_hash


class MockSourcePg:
    """
    Mocks source landing into Bronze, then computes Bronze-Hash.
    Tables:
      bronze.<table_name>            (raw simulated)
      bronze.<table_name>_hash       (raw + bk_hash/row_hash + meta)
    """
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.engine: Engine = create_engine(cfg.url, future=True)
        self.schema = cfg.pg_schema
        self.tbl_bronze = cfg.table_name
        self.tbl_bhash = f"{cfg.table_name}_hash"

        self.columns = ["col1_bk","col2_bk","col3_fk","col4_fk","col5","col6","col7","col8"]
        self.seed = 42

    # ---------- public API ----------
    def create_db(self) -> None:
        """Ensure schema exists; tables are created on first write."""
        with self.engine.begin() as con:
            con.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}";'))

    def populate_db(self) -> int:
        """Generate a DF via _create_df() and persist to bronze.<table_name>."""
        df = self._create_df(self.cfg.row_count)
        self._write_df(df, self.tbl_bronze, pk=None)   # no PK on pure bronze
        return df.height

    def create_bronze_hash(self) -> int:
        """
        Read bronze.<table_name>, compute bk_hash/row_hash + meta,
        and materialize bronze.<table_name>_hash (idempotent on (bk_hash,row_hash)).
        """
        bronze_fqtn = f'"{self.schema}"."{self.tbl_bronze}"'
        # Pull bronze using pandas (fast path) then convert to Polars for vector ops
        pdf = pd.read_sql_query(f"SELECT * FROM {bronze_fqtn};", self.engine)
        if pdf.empty:
            return 0
        df = pl.from_pandas(pdf)

        df = df.with_columns([
            pl.struct(["col1_bk", "col2_bk"]).map_elements(
                lambda r: dv2_hash([r["col1_bk"], r["col2_bk"]])
            ).alias("bk_hash"),
            pl.struct(self.columns).map_elements(
                lambda r: dv2_hash([r[c] for c in self.columns])
            ).alias("row_hash"),
            pl.lit(datetime.now(timezone.utc)).alias("load_dts"),
            pl.lit(self.cfg.record_source).alias("record_source"),
        ])

        # Persist with composite PK (bk_hash,row_hash), ON CONFLICT DO NOTHING
        self._write_df(df, self.tbl_bhash, pk=("bk_hash","row_hash"))
        return df.height

    def _create_df(self, n_rows: int) -> pl.DataFrame:
        '''
        creates polars df, simulating the bronze table
        will be used to populate the mock table
        '''
        rng = random.Random(self.seed)
        def rnd(n=8): return "".join(rng.choice(string.ascii_letters + string.digits) for _ in range(n))
        def fk1(): return f"HUB-{rng.randint(100, 999)}" if rng.random()>0.1 else None
        def fk2(): return f"HUB-{rng.randint(1000,1999)}" if rng.random()>0.2 else None

        return pl.DataFrame({
            "col1_bk": [f"CUST-{i:06d}" for i in range(n_rows)],
            "col2_bk": [f"SRC-{rng.randint(1,5)}" for _ in range(n_rows)],
            "col3_fk": [fk1() for _ in range(n_rows)],
            "col4_fk": [fk2() for _ in range(n_rows)],
            "col5":    [rnd(10) for _ in range(n_rows)],
            "col6":    [str(rng.randint(0, 10_000)) for _ in range(n_rows)],
            "col7":    [rnd(5) if rng.random()>0.3 else None for _ in range(n_rows)],
            "col8":    [rnd(12) if rng.random()>0.5 else None for _ in range(n_rows)],
        })

    def _write_df(self, df: pl.DataFrame, table: str, pk: tuple[str,str] | None) -> None:

        pdf: pd.DataFrame = df.to_pandas()  # drop use_pyarrow_extension_array for simplicity
        schema = self.schema
        fqtn = f'"{schema}"."{table}"'
        cols = [f'"{c}"' for c in pdf.columns]
        col_csv = ", ".join(cols)

        with self.engine.begin() as con:
            # 1) Ensure base table exists (use SQLAlchemy Connection, NOT raw DBAPI)
            pdf.head(0).to_sql(table, con, schema=schema, if_exists="append", index=False)

            # 2) Create TEMP staging table (NO schema qualification for temp)
            temp = f'_{table}_stage'
            con.execute(text(f'DROP TABLE IF EXISTS {temp};'))
            con.execute(text(f'CREATE TEMP TABLE {temp} AS SELECT {col_csv} FROM {fqtn} WHERE FALSE;'))

            # 3) Load into temp (again: pass SQLAlchemy Connection; do NOT pass schema for temp)
            pdf.to_sql(temp, con, if_exists="append", index=False)

            # 4) Optional: add PK once for bronze-hash
            if pk:
                pk_name = f'pk_{schema}_{table}'
                con.execute(text(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint WHERE conname = '{pk_name}'
                        ) THEN
                            ALTER TABLE {fqtn}
                            ADD CONSTRAINT {pk_name} PRIMARY KEY ("{pk[0]}","{pk[1]}");
                        END IF;
                    END$$;
                """))
                con.execute(text(f"""
                    INSERT INTO {fqtn} ({col_csv})
                    SELECT {col_csv} FROM {temp}
                    ON CONFLICT ("{pk[0]}","{pk[1]}") DO NOTHING;
                """))
            else:
                con.execute(text(f'INSERT INTO {fqtn} ({col_csv}) SELECT {col_csv} FROM {temp};'))

