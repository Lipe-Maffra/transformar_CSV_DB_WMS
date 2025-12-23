from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class JobConfig:
    pasta_entrada: Path
    pasta_saida: Path
    pasta_picking: Path
    sqlite_path: Path
    add_audit_cols: bool = True


def get_config() -> JobConfig:
    return JobConfig(
        pasta_entrada=Path(
            r"P:\Logística\PASTAS INDIVIDUAIS\FELIPE\1 - ROTINA\01 - RELATÓRIOS DIARIOS\02 - Movimentações\Dados\01 - Entrada"
        ),
        pasta_saida=Path(
            r"P:\Logística\PASTAS INDIVIDUAIS\FELIPE\1 - ROTINA\01 - RELATÓRIOS DIARIOS\02 - Movimentações\Dados\02 - Saídas"
        ),
        pasta_picking=Path(
            r"P:\Logística\PASTAS INDIVIDUAIS\FELIPE\1 - ROTINA\01 - RELATÓRIOS DIARIOS\02 - Movimentações\Dados\03 - Picking"
        ),
        sqlite_path=Path(
            r"P:\Logística\MÉTODOS & PROCESSOS\010 – EXTRAÇÕES (WMS , TMS, ERP)\FATO\TabelasWMS.db"
        ),
        add_audit_cols=True,
    )
