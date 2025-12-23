from __future__ import annotations

import sqlite3

from src.config import get_config
from src.loader import ensure_indexes, load_folder_to_table


def main() -> None:
    cfg = get_config()

    cfg.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(cfg.sqlite_path) as conn:
        r1 = load_folder_to_table(
            conn=conn,
            folder=cfg.pasta_entrada,
            table_name="fato_entrada",
            add_audit_cols=cfg.add_audit_cols,
        )
        r2 = load_folder_to_table(
            conn=conn,
            folder=cfg.pasta_saida,
            table_name="fato_saida",
            add_audit_cols=cfg.add_audit_cols,
        )
        r3 = load_folder_to_table(
            conn=conn,
            folder=cfg.pasta_picking,
            table_name="fato_picking",
            add_audit_cols=cfg.add_audit_cols,
        )

        ensure_indexes(conn)

    print("Carga concluída:")
    print(r1)
    print(r2)
    print(r3)


if __name__ == "__main__":
    main()
