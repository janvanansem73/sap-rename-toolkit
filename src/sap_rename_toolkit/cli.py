import argparse
from pathlib import Path

from sap_rename_toolkit.generate_mapping import generate_mapping


def main():
    parser = argparse.ArgumentParser(prog="sap-rename-toolkit")
    sub = parser.add_subparsers(dest="command", required=True)

    # generate_mapping <TABLE>
    p_gen = sub.add_parser("generate_mapping", help="Generate mapping JSON for a SAP table")
    p_gen.add_argument("table_name", help="SAP table name, e.g. VBAP")
    
    args = parser.parse_args()

    if args.command == "generate_mapping":
        out_path = generate_mapping(args.table_name)
        print(f"✅ Mapping written to: {out_path.resolve()}")


if __name__ == "__main__":
    main()