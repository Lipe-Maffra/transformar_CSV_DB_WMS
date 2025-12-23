from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from .io_readers import list_data_files, read_any


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        c2 = "" if c is None else str(c)
        c2 = c2.strip().replace("\n", " ").replace("\r", " ")
        c2 = " ".join(c2.split())
        c2 = c2.replace("/", "_").replace("-", "_")
        c2 = c2.replace("(", "").replace(")", "")
        c2 = c2.replace(".", "")
        c2 = c2.replace(" ", "_")
        cols.append(c2)

    out = df.copy()
    out.columns = cols
    return out


def fix_empty_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    SQLite/Pandas não aceita coluna sem nome.
    Aqui renomeia:
      '' / None / 'nan' => col_001, col_002, ...
    e também resolve duplicadas.
    """
    out = df.copy()

    new_cols = []
    used = set()
    seq = 1

    for c in out.columns:
        name = "" if c is None else str(c).strip()
        if name.lower() == "nan":
            name = ""

        if name == "":
            name = f"col_{seq:03d}"
            seq += 1

        # garante unicidade
        base = name
        k = 2
        while name in used:
            name = f"{base}_{k}"
            k += 1

        used.add(name)
        new_cols.append(name)

    out.columns = new_cols

    # opcional: remove colunas totalmente vazias (só NaN/None/"")
    # ajuda a reduzir peso e evita colunas inúteis
    def _is_empty_series(s: pd.Series) -> bool:
        # considera vazio: NaN ou string vazia/espacos
        s2 = s.fillna("").astype(str).str.strip()
        return (s2 == "").all()

    empties = [c for c in out.columns if _is_empty_series(out[c])]
    if empties:
        out = out.drop(columns=empties)

    return out


def add_audit_columns(df: pd.DataFrame, arquivo: Path, tabela_origem: str) -> pd.DataFrame:
    out = df.copy()
    out["arquivo_origem"] = arquivo.name
    out["tabela_origem"] = tabela_origem
    out["dt_carga"] = _ts()
    return out


def load_folder_to_table(
    conn: sqlite3.Connection,
    folder: Path,
    table_name: str,
    add_audit_cols: bool = True,
    chunksize: int = 50_000,
    drop_duplicates: bool = True,
) -> dict:
    files = list_data_files(folder)
    if not files:
        log(f"[{table_name}] Pasta vazia: {folder}")
        return {"table": table_name, "files_ok": 0, "files_fail": 0, "rows": 0, "status": "empty"}

    frames: list[pd.DataFrame] = []
    ok = 0
    fail = 0
    total_rows = 0

    log(f"[{table_name}] Iniciando carga. Pasta: {folder}")
    log(f"[{table_name}] Arquivos encontrados: {len(files)}")

    for i, f in enumerate(files, start=1):
        t0 = time.time()
        log(f"[{table_name}] ({i}/{len(files)}) Lendo: {f.name}")

        try:
            df = read_any(f)
            df = normalize_columns(df)
            df = fix_empty_column_names(df)

            if add_audit_cols:
                df = add_audit_columns(df, f, table_name)

            frames.append(df)
            ok += 1
            total_rows += len(df)

            dt = time.time() - t0
            log(f"[{table_name}] OK: {f.name} | linhas={len(df)} colunas={len(df.columns)} | {dt:.1f}s")

        except KeyboardInterrupt:
            log(f"[{table_name}] Interrompido pelo usuário (Ctrl+C).")
            raise

        except Exception as e:
            fail += 1
            log(f"[{table_name}] ERRO: {f.name} | {e}")

            err_df = pd.DataFrame(
                {
                    "arquivo_origem": [f.name],
                    "tabela_origem": [table_name],
                    "dt_carga": [_ts()],
                    "erro_leitura": [str(e)],
                }
            )
            frames.append(err_df)
            total_rows += len(err_df)

    log(f"[{table_name}] Consolidando {len(frames)} dataframes...")
    big = pd.concat(frames, ignore_index=True)

    if drop_duplicates:
        before = len(big)
        audit_cols = {"arquivo_origem", "tabela_origem", "dt_carga"}
        subset = [c for c in big.columns if c not in audit_cols]
        big = big.drop_duplicates(subset=subset, ignore_index=True)
        removed = before - len(big)
        if removed > 0:
            log(f"[{table_name}] Removidas {removed} linhas duplicadas.")

    effective_chunksize = chunksize
    if isinstance(conn, sqlite3.Connection):
        try:
            # SQLite has a hard limit on the number of bound variables per statement.
            max_vars = conn.execute("PRAGMA max_variable_number").fetchone()[0]
            max_vars = int(max_vars) if max_vars else 999
        except Exception:
            max_vars = 999

        cols = max(1, len(big.columns))
        max_rows_per_stmt = max(1, max_vars // cols)
        if max_rows_per_stmt < effective_chunksize:
            log(
                f"[{table_name}] Ajustando chunksize de {effective_chunksize} para {max_rows_per_stmt} "
                f"(limite {max_vars} variaveis por comando)."
            )
            effective_chunksize = max_rows_per_stmt

    log(
        f"[{table_name}] Gravando no SQLite: {table_name} | linhas={len(big)} | chunksize={effective_chunksize}"
    )
    big.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=effective_chunksize, method="multi")

    log(f"[{table_name}] Finalizado.")
    return {
        "table": table_name,
        "files_ok": ok,
        "files_fail": fail,
        "rows": int(len(big)),
        "status": "ok",
    }


def ensure_indexes(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    stmts = [
        "CREATE INDEX IF NOT EXISTS ix_fato_entrada_arquivo ON fato_entrada(arquivo_origem)",
        "CREATE INDEX IF NOT EXISTS ix_fato_saida_arquivo ON fato_saida(arquivo_origem)",
        "CREATE INDEX IF NOT EXISTS ix_fato_picking_arquivo ON fato_picking(arquivo_origem)",
        "CREATE INDEX IF NOT EXISTS ix_fato_entrada_dt ON fato_entrada(dt_carga)",
        "CREATE INDEX IF NOT EXISTS ix_fato_saida_dt ON fato_saida(dt_carga)",
        "CREATE INDEX IF NOT EXISTS ix_fato_picking_dt ON fato_picking(dt_carga)",
    ]
    for s in stmts:
        try:
            cur.execute(s)
        except sqlite3.OperationalError:
            continue
    conn.commit()
