from sap_json_toolkit.snowflake_client import SnowflakeClient


def main():
    sf = SnowflakeClient()

    # 1) Minimal sanity check
    cols, rows = sf.query("SELECT 1 AS OK")
    print("Query 1:", cols, rows)

    # 2) Show session context
    cols, rows = sf.query("""
        SELECT
          CURRENT_VERSION() AS SNOWFLAKE_VERSION,
          CURRENT_ROLE() AS CURRENT_ROLE,
          CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE,
          CURRENT_DATABASE() AS CURRENT_DATABASE,
          CURRENT_SCHEMA() AS CURRENT_SCHEMA
    """)
    print("Session context columns:", cols)
    print("Session context row:", rows[0])


if __name__ == "__main__":
    main()