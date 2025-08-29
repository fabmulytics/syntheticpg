from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "warehouse"
    pg_user: str = "postgres"
    pg_password: str = "postgres"
    pg_schema: str = "bronze"
    record_source: str = "GEN_SYNTH"
    row_count: int = 1000
    table_name: str = "orders"

    class Config:
        env_file = ".env"

    @property
    def url(self) -> str:
        out = f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        return out