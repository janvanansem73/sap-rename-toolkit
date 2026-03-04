import json
from collections import OrderedDict
from pathlib import Path

from sap_rename_toolkit.snowflake_client import SnowflakeClient


def generate_mapping(table_name: str, output_dir: Path = Path("mappings")) -> Path:
    table = table_name.upper().strip()

    sf = SnowflakeClient()

    # 1) Get table logical name
    sql_table = """
    SELECT LOGICAL_TABLE_NAME
    FROM SAP_CORE.FIVEDP_SAPHANADB.VW_SAP_LOGICAL_TABLE_NAME
    WHERE TABLE_NAME = %(table)s
    """
    cols, rows = sf.query(sql_table, {"table": table})
    table_logical = rows[0][0] if rows else None
    if not table_logical:
        raise RuntimeError(f"No logical table name found for TABLE_NAME='{table}'")

    # 2) Get field mappings in column order
    sql_fields = """
    SELECT COLUMN_NAME, LOGICAL_FIELD_NAME
    FROM SAP_CORE.FIVEDP_SAPHANADB.VW_SAP_LOGICAL_FIELD_NAME
    WHERE TABLE_NAME = %(table)s
    ORDER BY COLUMN_POSITION
    """
    cols, rows = sf.query(sql_fields, {"table": table})

    if not rows:
        raise RuntimeError(f"No field mappings found for TABLE_NAME='{table}'")

    # Maintain order explicitly
    fields = OrderedDict()
    for col_name, logical_field in rows:
        fields[str(col_name)] = str(logical_field) if logical_field is not None else ""

    mapping = OrderedDict()
    mapping["table"] = table
    mapping["table_logical_name"] = str(table_logical)
    mapping["fields"] = fields

    # Write output
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{table}.mapping.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    return out_path