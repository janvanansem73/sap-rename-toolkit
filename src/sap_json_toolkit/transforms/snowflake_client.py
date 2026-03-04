import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class SnowflakeClient:
    """
    Minimal Snowflake client using key-pair authentication.
    Loads config from .env in the repo root via python-dotenv.
    """

    def __init__(self):
        load_dotenv()

        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")

        self.private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        self.private_key_passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

        self._validate_env()

    def _validate_env(self):
        required = {
            "SNOWFLAKE_ACCOUNT": self.account,
            "SNOWFLAKE_USER": self.user,
            "SNOWFLAKE_ROLE": self.role,
            "SNOWFLAKE_WAREHOUSE": self.warehouse,
            "SNOWFLAKE_DATABASE": self.database,
            "SNOWFLAKE_SCHEMA": self.schema,
            "SNOWFLAKE_PRIVATE_KEY_PATH": self.private_key_path,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(f"Missing required .env settings: {', '.join(missing)}")

    def _load_private_key_der(self) -> bytes:
        key_file = Path(self.private_key_path)

        if not key_file.exists():
            raise FileNotFoundError(f"Private key file not found: {key_file}")

        password = None
        if self.private_key_passphrase:
            password = self.private_key_passphrase.encode("utf-8")

        with key_file.open("rb") as f:
            pem_data = f.read()

        p_key = serialization.load_pem_private_key(
            pem_data,
            password=password,
            backend=default_backend()
        )

        # Snowflake connector expects DER-encoded PKCS8 bytes
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

    def connect(self):
        private_key_der = self._load_private_key_der()

        # Snowflake connector supports connecting and executing SQL via cursor. [3](https://teams.microsoft.com/l/meeting/details?eventId=AAMkAGI3NjMyMWZmLTNiMTMtNDhjNy04NjQ1LTM0MzRiZDM0NGIyNwFRAAgI3mzuU-iAAEYAAAAAxC_7SY5mxEas8FmKoNO2MAcAIiGuSLnixk_aNsPcOCS0WgAAAAABDQAAIiGuSLnixk_aNsPcOCS0WgAFpFHC0wAAEA%3d%3d)
        return snowflake.connector.connect(
            user=self.user,
            account=self.account,
            private_key=private_key_der,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )

    def query(self, sql: str, params: dict | None = None):
        """
        Returns (columns, rows)
        """
        ctx = self.connect()
        try:
            cur = ctx.cursor()
            try:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description] if cur.description else []
                return cols, rows
            finally:
                cur.close()
        finally:
            ctx.close()