import json
import sys
from pathlib import Path

# --- Configure your paths ---
def get_paths():
    if len(sys.argv) < 2:
        print("Usage: python src/enrich_logical_names.py <input_json> [output_json]")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("out") / (input_path.stem + ".enriched.json")
    return input_path, output_path

# --- A starter mapping (extend over time) ---
# Keys are technical field names, values are descriptive logical names.
FIELD_LOGICAL_NAME_MAP = {
    "MANDT": "Client",
    "VBELN": "Sales Document Number",
    "AUART": "Sales Document Type",
    "AUDAT": "Document Date",
    "ERDAT": "Creation Date",
    "ERNAM": "Created By User",
    "KUNNR": "Sold-To Party",
    "VKORG": "Sales Organization",
    "VTWEG": "Distribution Channel",
    "SPART": "Division",
    "NETWR": "Net Value of Sales Document",
    "WAERK": "Document Currency",
    "BSTNK": "Customer Purchase Order Number",
    "BSTDK": "Customer Purchase Order Date",
    "FAKSK": "Billing Block",
    "LIFSK": "Delivery Block",
    "CMGST": "Overall Credit Status",
    "KNUMV": "Pricing Document Number",
}

def enrich_vbak_json(data: dict) -> dict:
    """
    Walks the expected structure:
      data["nodes"][i]["data"]["attributes"][j]
    and updates attributes[j]["logical_name"] if a mapping exists.
    """
    nodes = data.get("nodes", [])
    for node in nodes:
        node_data = node.get("data", {})
        attrs = node_data.get("attributes", [])
        for attr in attrs:
            tech_name = attr.get("name")
            if not tech_name:
                continue

            # If we know a better logical name, set it
            if tech_name in FIELD_LOGICAL_NAME_MAP:
                attr["logical_name"] = FIELD_LOGICAL_NAME_MAP[tech_name]
            else:
                # Heuristic fallback for unknowns:
                # Keep existing logical_name if it’s already descriptive,
                # otherwise mark it as "Unmapped (technical)".
                current = attr.get("logical_name")
                if not current or current.strip() == tech_name:
                    attr["logical_name"] = f"Unmapped ({tech_name})"

    # Optional: ensure entity logical_name is nice
    for node in nodes:
        if node.get("data", {}).get("name") == "VBAK":
            node["data"]["logical_name"] = "Sales Document Header"

    return data

def main():
    input_path, output_path = get_paths()

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    enriched = enrich_vbak_json(data)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote enriched JSON to: {output_path}")

if __name__ == "__main__":
    main()