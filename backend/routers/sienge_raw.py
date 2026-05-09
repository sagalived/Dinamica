"""
Router /api/sienge-raw — serve dados brutos exportados do SIENGE
lidos diretamente de data/sienge_raw/ (CSV, TXT e PDF).

Quando um dado não é encontrado, cria automaticamente uma pasta
com um arquivo TXT de template para preenchimento posterior.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from backend.services.sienge_raw_reader import (
    get_pdf_path,
    list_empresas,
    list_pdfs,
    read_categoria_generica,
    read_contas_abertas,
    read_contas_baixadas,
    read_contas_pagar_empresas,
    read_contas_receber_empresas,
    read_divergencias,
    read_empresa,
    read_faturamento,
    read_margem,
    read_mc_geral,
    read_mc_por_obra,
    read_obras_ativas,
    read_receita_total,
    read_recebidas_por_obra,
    read_titulos_receber_empresas,
)

router = APIRouter(prefix="/api/sienge-raw", tags=["sienge-raw"])


# ─── obras ────────────────────────────────────────────────────────────────────

@router.get("/obras-ativas")
def obras_ativas() -> dict[str, Any]:
    rows = read_obras_ativas()
    return {"total": len(rows), "rows": rows}


# ─── contas ───────────────────────────────────────────────────────────────────

@router.get("/contas-abertas")
def contas_abertas(tipo: str | None = None) -> dict[str, Any]:
    """
    Retorna contas abertas (a pagar e a receber).
    Filtro opcional: ?tipo=PAGAR ou ?tipo=RECEBER
    """
    rows = read_contas_abertas()
    if tipo:
        rows = [r for r in rows if r.get("TIPO", "").upper() == tipo.upper()]
    return {"total": len(rows), "rows": rows}


@router.get("/contas-baixadas")
def contas_baixadas(tipo: str | None = None) -> dict[str, Any]:
    rows = read_contas_baixadas()
    if tipo:
        rows = [r for r in rows if r.get("TIPO", "").upper() == tipo.upper()]
    return {"total": len(rows), "rows": rows}


@router.get("/divergencias")
def divergencias() -> dict[str, Any]:
    rows = read_divergencias()
    return {"total": len(rows), "rows": rows}


@router.get("/faturamento")
def faturamento(empresa: str | None = None) -> dict[str, Any]:
    """
    Faturamento por empresa lido dos PDFs.
    Filtro opcional: ?empresa=DINAMICA
    """
    rows = read_faturamento()
    if empresa:
        rows = [r for r in rows if empresa.upper() in r.get("empresa", "").upper()]
    return {"total": len(rows), "rows": rows}


# ─── financeiro ───────────────────────────────────────────────────────────────

@router.get("/margem")
def margem() -> dict[str, Any]:
    rows = read_margem()
    return {"total": len(rows), "rows": rows}


@router.get("/mc-geral")
def mc_geral() -> dict[str, Any]:
    rows = read_mc_geral()
    return {"total": len(rows), "rows": rows}


@router.get("/receita-total")
def receita_total() -> dict[str, Any]:
    rows = read_receita_total()
    return {"total": len(rows), "rows": rows}


@router.get("/recebidas-por-obra")
def recebidas_por_obra() -> dict[str, Any]:
    rows = read_recebidas_por_obra()
    return {"total": len(rows), "rows": rows}


# ─── empresas ─────────────────────────────────────────────────────────────────

@router.get("/empresas")
def empresas() -> dict[str, Any]:
    """Lista todas as empresas disponíveis em data/sienge_raw/empresas/."""
    nomes = list_empresas()
    return {"total": len(nomes), "empresas": nomes}


@router.get("/mc-por-obra")
def mc_por_obra() -> dict[str, Any]:
    """MC, receita, custos e margem% por obra agregados de todas as empresas."""
    rows = read_mc_por_obra()
    return {"total": len(rows), "rows": rows}


@router.get("/contas-pagar-empresas")
def contas_pagar_empresas() -> dict[str, Any]:
    """Contas a pagar agregadas de data/sienge_raw/empresas/contas a pagar/."""
    rows = read_contas_pagar_empresas()
    return {"total": len(rows), "rows": rows}


@router.get("/contas-receber-empresas")
def contas_receber_empresas() -> dict[str, Any]:
    """Contas a receber agregadas de data/sienge_raw/empresas/contas a receber/."""
    rows = read_contas_receber_empresas()
    return {"total": len(rows), "rows": rows}


@router.get("/titulos-receber-empresas")
def titulos_receber_empresas() -> dict[str, Any]:
    """Títulos a receber agregados de data/sienge_raw/empresas/titulos_a_receber/."""
    rows = read_titulos_receber_empresas()
    return {"total": len(rows), "rows": rows}


@router.get("/empresas/{nome}")
def empresa_detalhe(nome: str) -> dict[str, Any]:
    """Retorna dados de margem/MC de uma empresa específica."""
    rows = read_empresa(nome)
    created = len(rows) == 0
    result: dict[str, Any] = {"empresa": nome, "total": len(rows), "rows": rows}
    if created:
        result["message"] = f"Arquivo de template criado para '{nome}'. Preencha os dados."
    return result


# ─── PDFs ──────────────────────────────────────────────────────────────────────

@router.get("/pdfs")
def pdfs() -> dict[str, Any]:
    """Lista PDFs disponíveis em data/sienge_raw/pdf/."""
    nomes = list_pdfs()
    return {"total": len(nomes), "pdfs": nomes}


@router.get("/pdfs/{nome}")
def pdf_download(nome: str) -> FileResponse:
    """Baixa/abre um PDF pelo nome do arquivo."""
    path = get_pdf_path(nome)
    if path is None:
        raise HTTPException(status_code=404, detail=f"PDF '{nome}' não encontrado.")
    return FileResponse(str(path), media_type="application/pdf", filename=nome)


# ─── genérico / fallback ──────────────────────────────────────────────────────

@router.get("/{categoria}")
def categoria_generica(categoria: str) -> dict[str, Any]:
    """
    Endpoint genérico: tenta encontrar dados para a categoria informada.
    Se não existir, cria data/sienge_raw/{categoria}/{categoria}.txt com template.

    Exemplos:
      GET /api/sienge-raw/insumos   → cria data/sienge_raw/insumos/insumos.txt
      GET /api/sienge-raw/contratos → cria data/sienge_raw/contratos/contratos.txt
    """
    # Garante que não conflite com rotas já definidas
    rotas_fixas = {
        "obras-ativas", "contas-abertas", "contas-baixadas", "divergencias",
        "margem", "mc-geral", "receita-total", "recebidas-por-obra",
        "empresas", "pdfs", "faturamento",
    }
    if categoria in rotas_fixas:
        raise HTTPException(status_code=400, detail="Use o endpoint específico para esta categoria.")
    return read_categoria_generica(categoria)
