import os
import base64
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_der_private_key


class SnowflakeClient:
    """
    Minimal Snowflake client using key-pair authentication.
    Loads config from .env in the repo root via python-dotenv.
    """

    def __init__(self):
        # override=True ensures .env values win over any environment variables you might have set earlier
        load_dotenv(override=True)

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
        """
        Returns DER-encoded PKCS8 bytes for Snowflake connector.
        Supports:
          1) PEM (with BEGIN/END lines)
          2) DER (binary PKCS8)
          3) Base64 DER text (starts with MIIF..., no BEGIN/END)
        """
        key_file = Path(self.private_key_path)

        if not key_file.exists():
            raise FileNotFoundError(f"Private key file not found: {key_file}")

        password = None
        if self.private_key_passphrase:
            password = self.private_key_passphrase.encode("utf-8")

        raw = key_file.read_bytes()

        # 1) Try PEM
        try:
            p_key = serialization.load_pem_private_key(
                raw,
                password=password,
                backend=default_backend(),
            )
            return p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception:
            pass

        # 2) Try DER (binary PKCS8)
        try:
            p_key = load_der_private_key(
                raw,
                password=password,
                backend=default_backend(),
            )
            return p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception:
            pass

        # 3) Try base64 text (MIIF... style)
        try:
            b64 = b"".join(raw.split())
            der = base64.b64decode(b64)
            p_key = load_der_private_key(
                der,
                password=password,
                backend=default_backend(),
            )
            return p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        except Exception as e:
            raise ValueError(
                "Could not load private key. Ensure it is PEM (with BEGIN/END lines) "
                "or DER PKCS8, and the passphrase (if encrypted) is correct."
            ) from e

    def connect(self):
        private_key_der = self._load_private_key_der()
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
        Execute a SQL query and return (columns, rows).
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