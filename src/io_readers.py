from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _try_read_csv(path: Path) -> pd.DataFrame:
    """
    Leitura resiliente de CSV (ambiente BR):
    - tenta separador ; , e tab
    - tenta encodings comuns
    - lê tudo como texto para evitar quebra de tipagem
    """
    last_err: Optional[Exception] = None

    for enc in ("utf-8-sig", "cp1252", "latin1"):
        for sep in (";", ",", "\t"):
            try:
                return pd.read_csv(
                    path,
                    sep=sep,
                    encoding=enc,
                    dtype=str,
                    engine="python",
                )
            except Exception as e:
                last_err = e
                continue

    raise RuntimeError(f"Falha lendo CSV: {path} | {last_err}")


def _try_read_excel(path: Path) -> pd.DataFrame:
    """
    Lê o primeiro sheet do Excel.
    Se você tiver aba fixa (ex: 'Planilha1'), eu ajusto.
    """
    return pd.read_excel(path, dtype=str, engine="openpyxl")


def read_any(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        return _try_read_csv(path)

    if suffix in (".xlsx", ".xlsm", ".xls"):
        return _try_read_excel(path)

    raise ValueError(f"Extensão não suportada: {path.name}")


def list_data_files(folder: Path) -> list[Path]:
    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")

    files: list[Path] = []
    for ext in ("*.csv", "*.xlsx", "*.xlsm", "*.xls"):
        files.extend(folder.glob(ext))

    return sorted(files, key=lambda p: p.name.lower())
