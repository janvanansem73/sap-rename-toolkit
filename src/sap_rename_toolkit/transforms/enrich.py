import json
from pathlib import Path
from typing import Dict, Any, Tuple


def load_table_mapping(mappings_dir: Path, table_name: str) -> Tuple[str, Dict[str, str]]:
    """
    Loads mappings/tables/<TABLE>.json and returns:
      (table_logical_name, fields_map)
    """
    mapping_path = mappings_dir / "tables" / f"{table_name}.json"
    if not mapping_path.exists():
        return ("", {})  # no mapping found

    with mapping_path.open("r", encoding="utf-8") as f:
        m = json.load(f)

    return (m.get("table_logical_name", ""), m.get("fields", {}) or {})


def heuristic_fallback(tech_name: str) -> str:
    """
    Fallback when we don't have a mapping:
    - If it looks like a technical name, label as unmapped
    - Keep it stable (important for diffing)
    """
    return f"Unmapped ({tech_name})"


def enrich_table_json(doc: Dict[str, Any], mappings_dir: Path) -> Dict[str, Any]:
    """
    Enriches all nodes in the JSON:
    - Determines table name from node.data.name
    - Applies per-table mappings if present
    - Updates attribute.logical_name when:
        - missing, or
        - same as technical field name
    """
    nodes = doc.get("nodes", [])
    for node in nodes:
        data = node.get("data", {})
        table = data.get("name")
        if not table:
            continue

        table_logical_name, field_map = load_table_mapping(mappings_dir, table)

        # Update table/entity logical name if mapping exists
        if table_logical_name:
            data["logical_name"] = table_logical_name

        # Update attribute logical names
        attrs = data.get("attributes", [])
        for attr in attrs:
            tech = attr.get("name")
            if not tech:
                continue

            current = attr.get("logical_name", "")
            should_replace = (not current) or (current.strip() == tech)

            if should_replace:
                if tech in field_map:
                    attr["logical_name"] = field_map[tech]
                else:
                    attr["logical_name"] = heuristic_fallback(tech)

    return doc


def enrich_file(input_path: Path, output_path: Path, mappings_dir: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as f:
        doc = json.load(f)

    enriched = enrich_table_json(doc, mappings_dir)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    import sys
    from pathlib import Path

    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m sap_rename_toolkit.transforms.enrich <input_json>"
        )

    input_path = Path(sys.argv[1])

    # ✅ deterministic output location
    output_path = Path("output") / f"{input_path.stem}.enriched.json"

    # ✅ mappings location (adjust if needed)
    mappings_dir = Path("mappings")

    print(f"Input : {input_path.resolve()}")
    print(f"Output: {output_path.resolve()}")

    enrich_file(input_path, output_path, mappings_dir)