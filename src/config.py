from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class JobConfig:
    pasta_entrada: Path
    pasta_saida: Path
    pasta_picking: Path

    # destino final (rede)
    sqlite_path_final: Path

    # staging local (evita disk I/O error em P:\)
    sqlite_path_stage: Path

    add_audit_cols: bool = True


def get_config() -> JobConfig:
    return JobConfig(
        pasta_entrada=Path(
            r"C:\Users\felipe.maffra\Desktop\Python\xlsx para csv\output\entrada"
        ),
        pasta_saida=Path(
            r"C:\Users\felipe.maffra\Desktop\Python\xlsx para csv\output\saida"
        ),
        pasta_picking=Path(
            r"C:\Users\felipe.maffra\Desktop\Python\xlsx para csv\output\picking"
        ),
        sqlite_path_final=Path(
            r"P:\Logística\MÉTODOS & PROCESSOS\010 – EXTRAÇÕES (WMS , TMS, ERP)\FATO\TabelasWMS.db"
        ),
        sqlite_path_stage=Path(
            r"C:\Temp\TabelasWMS_stage.db"
        ),
        add_audit_cols=True,
    )
