from config import Settings
from mock_source_pg import MockSourcePg

def main():
    cfg = Settings()
    svc = MockSourcePg(cfg)
    svc.create_db()
    n_raw = svc.populate_db()
    n_hash = svc.create_bronze_hash()
    print(f"bronze.{cfg.table_name}: +{n_raw} rows")
    print(f"bronze.{cfg.table_name}_hash: +{n_hash} rows")

if __name__ == "__main__":
    main()
