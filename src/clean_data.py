import argparse
import logging
import re
from datetime import datetime

import pandas as pd
from dateutil import parser as dateparser
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

REQUIRED_COLS = ["id", "name", "email", "role", "start_date", "active"]

def read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Fehlende Spalten: {missing}")
    logging.info(f"Eingelesen: {df.shape[0]} Zeilen, {df.shape[1]} Spalten")
    return df

def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    # Trim Whitespaces
    for col in ["name", "email", "role"]:
        df[col] = df[col].astype(str).str.strip().replace({"": None})
        # Mehrfach-Leerzeichen in Namen entfernen
        if col == "name":
            df[col] = df[col].str.replace(r"\s{2,}", " ", regex=True)
    # id als String, getrimmt
    df["id"] = df["id"].astype(str).str.strip()
    return df

def parse_bool(val):
    if pd.isna(val):
        return None
    s = str(val).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None

def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    # active zu bool
    df["active"] = df["active"].apply(parse_bool)
    # start_date robust parsen (verschiedene Formate erlauben)
    def parse_date(x):
        if pd.isna(x) or str(x).strip() == "":
            return None
        try:
            dt = dateparser.parse(str(x), dayfirst=True)  # 15.05.2023 ok
            return dt.date().isoformat()
        except Exception:
            return None
    df["start_date"] = df["start_date"].apply(parse_date)
    return df

def validate_rows(df: pd.DataFrame):
    """Gibt zwei DataFrames zurück: valid, invalid (mit Fehlergrund)."""
    errors = []

    def row_errors(row):
        errs = []
        if not row["id"] or str(row["id"]).strip() == "":
            errs.append("missing_id")
        if not row["name"]:
            errs.append("missing_name")
        if not row["email"] or not EMAIL_RE.match(str(row["email"])):
            errs.append("invalid_email")
        if not row["role"]:
            errs.append("missing_role")
        if not row["start_date"]:
            errs.append("invalid_start_date")
        if row["active"] is None:
            errs.append("invalid_active")
        return ";".join(errs)

    df = df.copy()
    df["error"] = df.apply(row_errors, axis=1)

    invalid = df[df["error"] != ""].copy()
    valid = df[df["error"] == ""].copy()

    # Dubletten nach id in valid entfernen (erste behalten)
    before = len(valid)
    valid = valid.drop_duplicates(subset=["id"], keep="first")
    dropped = before - len(valid)
    if dropped:
        logging.info(f"Dubletten entfernt (id): {dropped}")

    return valid.drop(columns=["error"]), invalid

def save_sqlite(df: pd.DataFrame, db_url: str, table: str):
    engine = create_engine(db_url)
    df.to_sql(table, engine, if_exists="replace", index=False)
    logging.info(f"{len(df)} Zeilen nach SQLite geschrieben → Tabelle '{table}'")

def write_report(valid: pd.DataFrame, invalid: pd.DataFrame, path_md: str):
    lines = [
        "# Stammdaten-Report",
        "",
        f"- Gültige Datensätze: **{len(valid)}**",
        f"- Ungültige Datensätze: **{len(invalid)}**",
        "",
        "## Fehlerübersicht (Top 5)",
    ]
    if not invalid.empty:
        top_errors = (
            invalid["error"]
            .str.split(";", expand=True)
            .stack()
            .value_counts()
            .head(5)
            .to_dict()
        )
        for k, v in top_errors.items():
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- keine")

    with open(path_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logging.info(f"Report geschrieben → {path_md}")

def main():
    parser = argparse.ArgumentParser(description="Stammdaten bereinigen & speichern")
    parser.add_argument("--input", default="data/sample_raw.csv")
    parser.add_argument("--db", default="sqlite:///stammdaten.db")
    parser.add_argument("--table", default="stammdaten_clean")
    parser.add_argument("--valid_csv", default="data/clean.csv")
    parser.add_argument("--invalid_csv", default="data/rejects.csv")
    parser.add_argument("--report_md", default="docs/report.md")
    args = parser.parse_args()

    df = read_csv(args.input)
    df = normalize_strings(df)
    df = normalize_types(df)
    valid, invalid = validate_rows(df)

    logging.info(f"Valid: {len(valid)} | Invalid: {len(invalid)}")
    valid.to_csv(args.valid_csv, index=False)
    invalid.to_csv(args.invalid_csv, index=False)
    save_sqlite(valid, args.db, args.table)
    write_report(valid, invalid, args.report_md)

    print("\nKPI:")
    print(f"- gültig: {len(valid)}")
    print(f"- ungültig: {len(invalid)}")
    print(f"- Tabelle: {args.table} in {args.db.replace('sqlite:///','')}")
    print(f"- Report: {args.report_md}")

if __name__ == "__main__":
    main()