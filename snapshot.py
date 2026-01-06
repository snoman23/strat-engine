# snapshot.py

import os
import json
import pandas as pd


def _atomic_write_text(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        f.write(text)
    os.replace(tmp, path)


def write_snapshot(rows, out_dir="cache/results"):
    os.makedirs(out_dir, exist_ok=True)

    json_path = os.path.join(out_dir, "latest.json")
    csv_path = os.path.join(out_dir, "latest.csv")

    # JSON (atomic)
    json_text = json.dumps(rows, ensure_ascii=False, indent=2, default=str)
    _atomic_write_text(json_path, json_text)

    # CSV (atomic)
    df = pd.DataFrame(rows) if rows else pd.DataFrame([])
    csv_text = df.to_csv(index=False)
    _atomic_write_text(csv_path, csv_text)
