from __future__ import annotations

from pathlib import Path
from datetime import datetime

from src.config import get_config
from src.loader import (
    ensure_indexes,
    load_folder_to_table,
    publish_sqlite_stage_to_final,
)


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def main() -> None:
    cfg = get_config()

    # Stage local
    cfg.sqlite_path_stage.parent.mkdir(parents=True, exist_ok=True)
    if cfg.sqlite_path_stage.exists():
        try:
            cfg.sqlite_path_stage.unlink()
        except Exception:
            pass

    # 1) carrega tudo no DB stage (local)
    r1 = load_folder_to_table(
        folder=cfg.pasta_entrada,
        sqlite_path=cfg.sqlite_path_stage,
        table_name="fato_entrada",
        log=log,
        recursive=True,
        drop_duplicates=True,
        add_audit_cols=cfg.add_audit_cols,
        chunksize=50_000,
    )

    r2 = load_folder_to_table(
        folder=cfg.pasta_saida,
        sqlite_path=cfg.sqlite_path_stage,
        table_name="fato_saida",
        log=log,
        recursive=True,
        drop_duplicates=True,
        add_audit_cols=cfg.add_audit_cols,
        chunksize=50_000,
    )

    r3 = load_folder_to_table(
        folder=cfg.pasta_picking,
        sqlite_path=cfg.sqlite_path_stage,
        table_name="fato_picking",
        log=log,
        recursive=True,
        drop_duplicates=True,
        add_audit_cols=cfg.add_audit_cols,
        chunksize=50_000,
    )

    # 2) índices no stage (local)
    ensure_indexes(cfg.sqlite_path_stage, log=log)

    # 3) publicar pro destino final (rede) — rápido, com retry
    publish_sqlite_stage_to_final(
        stage_path=cfg.sqlite_path_stage,
        final_path=cfg.sqlite_path_final,
        log=log,
    )

    log("Carga concluída:")
    print(r1)
    print(r2)
    print(r3)


if __name__ == "__main__":
    main()
