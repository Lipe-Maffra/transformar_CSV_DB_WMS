from __future__ import annotations

import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
from dateutil import parser as date_parser

from .io_readers import list_data_files, read_data_file as read_any


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _connect_sqlite(db_path: Path, timeout: int = 60) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), timeout=timeout)
    # PRAGMAs mais seguros pra ambiente corporativo/rede
    conn.execute("PRAGMA journal_mode=DELETE;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _copy_replace(src: Path, dst: Path, log: Callable[[str], None]) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Em rede, as vezes o replace falha por lock momentaneo - retry curto.
    for attempt in range(1, 6):
        try:
            tmp = dst.with_suffix(".tmp")
            shutil.copy2(src, tmp)
            # replace atomico (o melhor que da no Windows)
            tmp.replace(dst)
            log(f"[sqlite] Publicado com sucesso em {dst}")
            return
        except PermissionError:
            log(f"[sqlite] Lock/Permissao ao publicar (tentativa {attempt}/5). Aguardando...")
            time.sleep(1.5)

    raise PermissionError(f"Nao foi possivel publicar o DB final (lock persistente): {dst}")


def _parse_datetime_series(values: pd.Series) -> pd.Series:
    s = values.apply(lambda v: v.strip() if isinstance(v, str) else v)
    s = s.replace({"": None, "None": None, "nan": None, "NaT": None, "NULL": None})
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)

    if dt.isna().any():
        mask = dt.isna() & s.notna()
        if mask.any():
            parsed = []
            for v in s[mask]:
                try:
                    parsed.append(date_parser.parse(str(v), dayfirst=True))
                except Exception:
                    parsed.append(pd.NaT)
            dt.loc[mask] = parsed

    return dt


def _dt_series_to_iso(values: pd.Series, parsed: pd.Series) -> pd.Series:
    iso = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
    return iso.where(parsed.notna(), values)


def _is_missing_text(values: pd.Series) -> pd.Series:
    missing = values.isna()
    text = values.astype(str).str.strip()
    missing |= text.eq("")
    missing |= text.str.lower().isin({"none", "nan", "nat", "null"})
    return missing


def _apply_cross_date_fallback(
    df: pd.DataFrame, table_name: str, log: Callable[[str], None]
) -> None:
    if "Data inicial" not in df.columns or "Data final" not in df.columns:
        return

    di = df["Data inicial"]
    dfinal = df["Data final"]
    missing_di = _is_missing_text(di)
    missing_df = _is_missing_text(dfinal)

    fill_di = missing_di & ~missing_df
    fill_df = missing_df & ~missing_di

    count_di = int(fill_di.sum())
    count_df = int(fill_df.sum())

    if count_di:
        df.loc[fill_di, "Data inicial"] = dfinal[fill_di]
    if count_df:
        df.loc[fill_df, "Data final"] = di[fill_df]

    log(
        f"[{table_name}] Fallback datas | inicial<-final={count_di} | final<-inicial={count_df}"
    )


def _compute_dt_ref_dt(df: pd.DataFrame) -> pd.Series:
    di_dt = _parse_datetime_series(df["Data inicial"]) if "Data inicial" in df.columns else None
    df_dt = _parse_datetime_series(df["Data final"]) if "Data final" in df.columns else None

    if di_dt is not None and df_dt is not None:
        return di_dt.where(di_dt.notna(), df_dt)
    if di_dt is not None:
        return di_dt
    if df_dt is not None:
        return df_dt
    return pd.Series([pd.NaT] * len(df))


def load_folder_to_table(
    *,
    folder: Path,
    sqlite_path: Path,
    table_name: str,
    log: Callable[[str], None],
    recursive: bool = True,
    drop_duplicates: bool = True,
    add_audit_cols: bool = True,
    chunksize: int = 50_000,
) -> dict:
    log(f"[{table_name}] Pasta: {folder}")
    log(f"[{table_name}] Modo recursivo: {recursive}")

    files = list_data_files(folder, recursive=recursive)
    log(f"[{table_name}] Arquivos encontrados: {len(files)}")

    ok, fail = 0, 0
    frames: list[pd.DataFrame] = []

    for fp in files:
        try:
            log(f"[{table_name}] Lendo arquivo: {fp}")
            df = read_any(fp, log=log)

            if add_audit_cols:
                df["arquivo_origem"] = fp.name
                df["dt_carga"] = _ts()
                df["_source_file"] = fp.name
                df["_source_mtime"] = datetime.fromtimestamp(fp.stat().st_mtime).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                df["_loaded_at"] = _ts()
                df["_source_sheet"] = df.attrs.get("source_sheet")

            dt_ref_dt = _compute_dt_ref_dt(df)
            dt_ref_nat = int(dt_ref_dt.isna().sum())
            dt_ref_total = int(len(dt_ref_dt))
            dt_ref_pct = (dt_ref_nat / dt_ref_total * 100.0) if dt_ref_total else 0.0
            log(
                f"[{table_name}] dt_ref NaT por arquivo: {dt_ref_nat}/{dt_ref_total} "
                f"({dt_ref_pct:.2f}%)"
            )

            df["dt_ref"] = dt_ref_dt.dt.strftime("%Y-%m-%d %H:%M:%S")

            log(f"[{table_name}] Linhas lidas do arquivo: {len(df)}")

            frames.append(df)
            ok += 1
        except Exception as e:
            fail += 1
            log(f"[{table_name}] Falha ao ler {fp.name}: {e}")

    log(f"[{table_name}] OK={ok} | FAIL={fail}")

    if not frames:
        return {"table": table_name, "files_ok": 0, "files_fail": 0, "rows": 0, "status": "empty"}

    log(f"[{table_name}] Consolidando {len(frames)} dataframes...")
    big = pd.concat(frames, ignore_index=True)

    def _normalize_dt_cols(df: pd.DataFrame, cols: list[str]) -> None:
        for c in cols:
            if c in df.columns:
                parsed = _parse_datetime_series(df[c])
                df[c] = _dt_series_to_iso(df[c], parsed)

    _normalize_dt_cols(big, ["Data inicial", "Data final", "Data tarefa"])
    _apply_cross_date_fallback(big, table_name, log)

    dt_ref_dt = _compute_dt_ref_dt(big)
    big["dt_ref"] = dt_ref_dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    if dt_ref_dt.notna().any():
        log(f"[{table_name}] dt_ref min={dt_ref_dt.min()} | max={dt_ref_dt.max()}")
    year_counts = dt_ref_dt.dt.year.value_counts(dropna=True).sort_index()
    if not year_counts.empty:
        log(f"[{table_name}] Contagem por ano (dt_ref): {year_counts.to_dict()}")

    log(f"[{table_name}] Linhas consolidadas (antes dedup): {len(big)}")
    if drop_duplicates:
        before = len(big)
        dedup_cols = [
            "Tarefa",
            "Tipo",
            "Onda",
            "Data inicial",
            "Data final",
            "Item",
            "Origem",
            "Destino",
            "Qtd.",
            "Almoxarifado",
            "Carga",
            "Unitizador",
        ]
        dedup_cols = [c for c in dedup_cols if c in big.columns]
        if dedup_cols:
            big = big.drop_duplicates(subset=dedup_cols)
        else:
            big = big.drop_duplicates()
        removed = before - len(big)
        log(f"[{table_name}] Duplicadas removidas: {removed}")
        log(f"[{table_name}] Linhas finais (apos dedup): {len(big)}")
        if before > 0 and (removed / before) > 0.02:
            log(
                f"[{table_name}] WARNING: dedup removeu {removed} "
                f"linhas ({removed / before:.2%})"
            )

    log(
        f"[{table_name}] Gravando no SQLite: {table_name} | linhas={len(big)} | chunksize={chunksize}"
    )

    conn = _connect_sqlite(sqlite_path, timeout=60)
    try:
        big.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=chunksize)
        conn.commit()
    finally:
        conn.close()

    log(f"[{table_name}] Finalizado.")
    return {"table": table_name, "files_ok": ok, "files_fail": fail, "rows": int(len(big)), "status": "ok"}


def ensure_indexes(sqlite_path: Path, log: Callable[[str], None]) -> None:
    conn = _connect_sqlite(sqlite_path, timeout=60)
    try:
        cur = conn.cursor()
        stmts = [
            "CREATE INDEX IF NOT EXISTS ix_fato_saida_tarefa ON fato_saida(Tarefa)",
            "CREATE INDEX IF NOT EXISTS ix_fato_saida_dt ON fato_saida(dt_carga)",
            "CREATE INDEX IF NOT EXISTS ix_fato_saida_dtref ON fato_saida(dt_ref)",
            "CREATE INDEX IF NOT EXISTS ix_fato_picking_dt ON fato_picking(dt_carga)",
        ]
        for s in stmts:
            try:
                cur.execute(s)
            except sqlite3.OperationalError:
                continue
        conn.commit()
        log("[sqlite] Indices garantidos.")
    finally:
        conn.close()


def publish_sqlite_stage_to_final(*, stage_path: Path, final_path: Path, log: Callable[[str], None]) -> None:
    if not stage_path.exists():
        raise FileNotFoundError(f"DB stage nao encontrado: {stage_path}")
    _copy_replace(stage_path, final_path, log)
