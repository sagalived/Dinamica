"""
Serviço de leitura dos arquivos brutos do SIENGE em data/sienge_raw/.

Prioridade: Excel (data/sienge_raw/excel/) → CSV/TXT (separador ';') → fallback (cria template).
Execute scripts/pdf_to_excel.py uma vez para gerar os arquivos Excel a partir dos PDFs.
"""
from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore
    _PANDAS_OK = True
except ImportError:
    _PANDAS_OK = False

from backend.config import BASE_DIR

RAW_DIR = Path(BASE_DIR) / "data" / "sienge_raw"
PDF_DIR = RAW_DIR / "pdf"
EXCEL_DIR = RAW_DIR / "excel"


# ─── helpers gerais ───────────────────────────────────────────────────────────

def _clean(v: Any) -> str:
    """Limpa valor de célula: remove \n, espaços extras."""
    return re.sub(r"\s+", " ", str(v or "")).strip()


def _parse_semicolon_file(path: Path) -> list[dict[str, Any]]:
    """Lê arquivo CSV/TXT com cabeçalho na primeira linha e separador ';'."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    text = None
    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        return []

    lines = [line for line in text.splitlines() if line.strip()]
    # Pula linhas de metadados que começam com "Fonte:" ou similares
    data_lines = [l for l in lines if not l.startswith("Fonte:") and not l.startswith("#")]
    if not data_lines:
        return []

    reader = csv.DictReader(io.StringIO("\n".join(data_lines)), delimiter=";")
    rows: list[dict[str, Any]] = []
    for row in reader:
        rows.append({k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()})
    return rows


def _ensure_fallback(categoria: str) -> Path:
    """
    Cria data/sienge_raw/{categoria}/{categoria}.txt com template padrão
    se a pasta ainda não existir.  Retorna o caminho do arquivo criado.
    """
    folder = RAW_DIR / categoria
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{categoria}.txt"
    if not target.exists():
        target.write_text(
            f"CATEGORIA;DESCRICAO;VALOR\n"
            f"# Arquivo de template para '{categoria}' — preencha os dados abaixo\n",
            encoding="utf-8",
        )
    return target


def _find_raw_file(names: list[str]) -> Path | None:
    """Procura por qualquer nome na lista dentro de RAW_DIR."""
    for name in names:
        p = RAW_DIR / name
        if p.exists():
            return p
    return None


# ─── leitor Excel (gerado pelo scripts/pdf_to_excel.py) ──────────────────────

def _read_excel(nome: str, tipo: str = "") -> list[dict[str, Any]]:
    """
    Lê data/sienge_raw/excel/{nome}.xlsx via pandas e retorna lista de dicts.
    Se o arquivo não existir, retorna lista vazia.
    `tipo` é adicionado em todos os registros quando fornecido.
    """
    if not _PANDAS_OK:
        return []
    path = EXCEL_DIR / f"{nome}.xlsx"
    if not path.exists():
        return []
    try:
        df = pd.read_excel(path, dtype=str)
        df = df.fillna("")
        rows = df.to_dict(orient="records")
        if tipo:
            for r in rows:
                r.setdefault("TIPO", tipo)
        return rows
    except Exception:
        return []


# ─── parsers de PDF (legado — não utilizados quando Excel existir) ─────────────

def _empresa_from_text(text: str) -> str:
    """Extrai nome da empresa de linha tipo 'EmpresaX - NOME LTDA'."""
    m = re.search(r"Empresa\d*\s*[-–]?\s*(.+?)(?:\s+Período|\s*$)", text)
    return m.group(1).strip() if m else ""


def _skip_row(row: list[Any], skip_starts: tuple[str, ...]) -> bool:
    c0 = _clean(row[0]) if row else ""
    return not c0 or c0.startswith(skip_starts)



# ─── leitores específicos (PDF-first → TXT fallback) ──────────────────────────

def read_obras_ativas() -> list[dict[str, Any]]:
    path = _find_raw_file(["obrasativas.txt", "obras_ativas.txt", "obras-ativas.txt"])
    if not path:
        _ensure_fallback("obras_ativas")
        return []
    return _parse_semicolon_file(path)


def read_contas_abertas() -> list[dict[str, Any]]:
    """Excel-first: Contas a Pagar + Contas a Receber. Fallback: TXT."""
    excel_pagar = _read_excel("contas_a_pagar", tipo="PAGAR")
    excel_receber = _read_excel("contas_a_receber", tipo="RECEBER")
    if excel_pagar or excel_receber:
        return excel_pagar + excel_receber
    path = _find_raw_file(["contasabertas_sienge.txt", "contas_abertas.txt"])
    if not path:
        _ensure_fallback("contas_abertas")
        return []
    return _parse_semicolon_file(path)


def read_contas_baixadas() -> list[dict[str, Any]]:
    """Excel-first: Contas Pagas + Recebidas. Fallback: TXT."""
    excel_pagas = _read_excel("contas_pagas", tipo="PAGAR_BAIXADO")
    excel_recebidas = _read_excel("contas_recebidas", tipo="RECEBER_BAIXADO")
    if excel_pagas or excel_recebidas:
        return excel_pagas + excel_recebidas
    path = _find_raw_file(["contasbaixadas_sienge.txt", "contas_baixadas.txt"])
    if not path:
        _ensure_fallback("contas_baixadas")
        return []
    return _parse_semicolon_file(path)


def read_faturamento() -> list[dict[str, Any]]:
    """Excel-first: faturamento de todas as empresas."""
    return _read_excel("faturamento")


def read_margem() -> list[dict[str, Any]]:
    path = _find_raw_file(["Margem2.csv", "Margem.csv", "Margem2.txt", "Margem.txt"])
    if not path:
        _ensure_fallback("margem")
        return []
    return _parse_semicolon_file(path)


def read_mc_geral() -> list[dict[str, Any]]:
    path = _find_raw_file(["MC_geral.csv", "MC_geral2.csv", "MC_geral.txt", "MC_geral2.txt"])
    if not path:
        _ensure_fallback("mc_geral")
        return []
    return _parse_semicolon_file(path)


def read_receita_total() -> list[dict[str, Any]]:
    path = _find_raw_file(["receita_total.csv", "receita_total.txt", "Receita.txt"])
    if not path:
        _ensure_fallback("receita_total")
        return []
    return _parse_semicolon_file(path)


def read_recebidas_por_obra() -> list[dict[str, Any]]:
    path = _find_raw_file(["recebidas_por_obra_sienge.txt", "recebidas_por_obra.txt"])
    if not path:
        _ensure_fallback("recebidas_por_obra")
        return []
    return _parse_semicolon_file(path)


def read_divergencias() -> list[dict[str, Any]]:
    path = _find_raw_file(["divergencias_contas_sienge_db.txt", "divergencias.txt"])
    if not path:
        _ensure_fallback("divergencias")
        return []
    return _parse_semicolon_file(path)


# ─── empresas ─────────────────────────────────────────────────────────────────

def list_empresas() -> list[str]:
    """Lista nomes das empresas disponíveis (arquivos CSV em data/sienge_raw/empresas/)."""
    folder = RAW_DIR / "empresas"
    if not folder.exists():
        return []
    return sorted(
        p.stem for p in folder.iterdir()
        if p.suffix.lower() == ".csv"
    )


def read_empresa(nome: str) -> list[dict[str, Any]]:
    """
    Lê dados de uma empresa pelo nome (sem extensão).
    Se não encontrar, cria pasta/arquivo de fallback.
    """
    folder = RAW_DIR / "empresas"
    if folder.exists():
        # busca case-insensitive
        for p in folder.iterdir():
            if p.suffix.lower() == ".csv" and p.stem.lower() == nome.lower():
                return _parse_semicolon_file(p)
    # fallback — cria arquivo template em empresas/
    target = folder / f"{nome}.txt" if folder.exists() else RAW_DIR / "empresas" / f"{nome}.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_text(
            "EMPRESA;ID_EMPRESA;OBRA;ID_OBRA;RECEITA;CUSTOS_DESPESAS;MC;MARGEM\n"
            f"# Dados da empresa '{nome}' — preencha os valores abaixo\n",
            encoding="utf-8",
        )
    return []


def read_mc_por_obra() -> list[dict[str, Any]]:
    """
    Agrega os CSVs de todas as empresas em data/sienge_raw/empresas/*.csv.
    Retorna lista com campos: ID_OBRA, OBRA, EMPRESA, ID_EMPRESA, RECEITA, CUSTOS_DESPESAS, MC, MARGEM.
    """
    folder = RAW_DIR / "empresas"
    if not folder.exists():
        return []
    rows: list[dict[str, Any]] = []
    for p in sorted(folder.iterdir()):
        if p.suffix.lower() != ".csv":
            continue
        for row in _parse_semicolon_file(p):
            # ignora linhas de template/comentário
            obra = str(row.get("OBRA") or row.get("obra") or "").strip()
            if not obra or obra.startswith("#"):
                continue
            rows.append(row)
    return rows


# ─── PDFs ──────────────────────────────────────────────────────────────────────

def list_pdfs() -> list[str]:
    """Lista nomes dos PDFs disponíveis em data/sienge_raw/pdf/."""
    folder = RAW_DIR / "pdf"
    if not folder.exists():
        return []
    return sorted(p.name for p in folder.iterdir() if p.suffix.lower() == ".pdf")


def get_pdf_path(nome: str) -> Path | None:
    """Retorna o Path de um PDF se existir."""
    clean = Path(nome).name  # evita path traversal
    path = RAW_DIR / "pdf" / clean
    return path if path.exists() and path.suffix.lower() == ".pdf" else None


# ─── genérico / fallback ──────────────────────────────────────────────────────

def read_categoria_generica(categoria: str) -> dict[str, Any]:
    """
    Tenta ler qualquer arquivo em data/sienge_raw/{categoria}*.
    Se não existir, cria a pasta e um TXT de template e retorna aviso.
    """
    # Normaliza nome
    slug = re.sub(r"[^a-z0-9_\-]", "_", categoria.lower().strip())

    # Tenta encontrar arquivo na raiz do RAW_DIR
    candidatos = [
        RAW_DIR / f"{slug}.csv",
        RAW_DIR / f"{slug}.txt",
        RAW_DIR / slug / f"{slug}.csv",
        RAW_DIR / slug / f"{slug}.txt",
    ]
    for path in candidatos:
        if path.exists():
            rows = _parse_semicolon_file(path)
            return {"source": str(path.relative_to(BASE_DIR)), "rows": rows, "total": len(rows), "created": False}

    # Não encontrou → cria fallback
    created_path = _ensure_fallback(slug)
    return {
        "source": str(created_path.relative_to(BASE_DIR)),
        "rows": [],
        "total": 0,
        "created": True,
        "message": f"Arquivo de template criado em '{created_path.relative_to(BASE_DIR)}'. Preencha os dados.",
    }


# ─── contas a pagar / receber / títulos — leitura por pasta de empresa ────────

def _load_empresa_name_map() -> dict[str, int]:
    """Retorna {NOME_UPPER: id} a partir de data/empresas.json."""
    emp_file = Path(BASE_DIR) / "data" / "empresas.json"
    if not emp_file.exists():
        return {}
    try:
        data = json.loads(emp_file.read_text(encoding="utf-8"))
        return {
            str(e.get("name", "")).strip().upper(): int(e["id"])
            for e in data
            if "id" in e
        }
    except Exception:
        return {}


def _parse_money_ptbr(v: Any) -> float:
    raw = re.sub(r"[R$\s]", "", str(v or "")).strip()
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _parse_date_ptbr(v: Any) -> str:
    raw = str(v or "").strip()
    if not raw:
        return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(raw[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:10]


def read_contas_pagar_empresas() -> list[dict[str, Any]]:
    """Lê todos os CSVs de data/sienge_raw/empresas/contas a pagar/."""
    folder = RAW_DIR / "empresas" / "contas a pagar"
    if not folder.exists():
        return []
    name_map = _load_empresa_name_map()
    rows: list[dict[str, Any]] = []
    for path in sorted(folder.glob("*.csv")):
        company_name = path.stem
        company_id = name_map.get(company_name.strip().upper())
        for row in _parse_semicolon_file(path):
            total = _parse_money_ptbr(row.get("Total") or row.get("Valor no vencto") or "")
            if total == 0:
                continue
            rows.append({
                "companyId": company_id,
                "companyName": company_name,
                "creditor": row.get("Credor") or "",
                "document": row.get("Documento") or "",
                "dueDate": _parse_date_ptbr(row.get("Lançamento") or row.get("Lancamento") or ""),
                "total": total,
            })
    return rows


def read_contas_receber_empresas() -> list[dict[str, Any]]:
    """Lê todos os CSVs de data/sienge_raw/empresas/contas a receber/."""
    folder = RAW_DIR / "empresas" / "contas a receber"
    if not folder.exists():
        return []
    name_map = _load_empresa_name_map()
    rows: list[dict[str, Any]] = []
    for path in sorted(folder.glob("*.csv")):
        company_name = path.stem
        company_id = name_map.get(company_name.strip().upper())
        for row in _parse_semicolon_file(path):
            total = _parse_money_ptbr(row.get("Total") or row.get("Valor no vencto") or "")
            if total == 0:
                continue
            rows.append({
                "companyId": company_id,
                "companyName": company_name,
                "creditor": row.get("Credor") or "",
                "document": row.get("Documento") or "",
                "dueDate": _parse_date_ptbr(row.get("Lançamento") or row.get("Lancamento") or ""),
                "total": total,
            })
    return rows


def read_titulos_receber_empresas() -> list[dict[str, Any]]:
    """Lê todos os CSVs de data/sienge_raw/empresas/titulos_a_receber/."""
    folder = RAW_DIR / "empresas" / "titulos_a_receber"
    if not folder.exists():
        return []
    name_map = _load_empresa_name_map()
    rows: list[dict[str, Any]] = []
    for path in sorted(folder.glob("*.csv")):
        company_name = path.stem
        company_id_from_file = name_map.get(company_name.strip().upper())
        for row in _parse_semicolon_file(path):
            # titulos_a_receber tem campo "Cód. empresa" próprio
            raw_cid = row.get("Cód. empresa") or row.get("Cod. empresa") or ""
            try:
                company_id: int | None = int(raw_cid) if raw_cid else company_id_from_file
            except ValueError:
                company_id = company_id_from_file
            total = _parse_money_ptbr(
                row.get("Valor total") or row.get("Valor líquido") or row.get("Valor a receber") or ""
            )
            rows.append({
                "companyId": company_id,
                "companyName": row.get("Empresa") or company_name,
                "title": row.get("Título") or row.get("Titulo") or "",
                "client": row.get("Cliente") or "",
                "document": (row.get("Documento") or "") + " " + (row.get("Nº documento") or row.get("No documento") or ""),
                "dueDate": _parse_date_ptbr(row.get("Data emissão") or row.get("Data emissao") or ""),
                "total": total,
            })
    return rows

