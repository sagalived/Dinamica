"""
Converte os PDFs do SIENGE (data/sienge_raw/pdf/) em arquivos Excel
em data/sienge_raw/excel/, usando extração de texto linha a linha
(muito mais rápido que extração de tabelas para PDFs com centenas de páginas).

Execute uma vez (ou sempre que os PDFs forem atualizados):
    .venv\\Scripts\\python.exe scripts/pdf_to_excel.py

Arquivos gerados:
    contas_a_pagar.xlsx
    contas_a_receber.xlsx
    contas_pagas.xlsx
    contas_recebidas.xlsx
    faturamento.xlsx
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
PDF_DIR = ROOT / "data" / "sienge_raw" / "pdf"
OUT_DIR = ROOT / "data" / "sienge_raw" / "excel"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MONEY = r"[\d]+(?:\.\d{3})*,\d{2}"  # ex: 1.234,56


def _empresa_da_linha(line: str) -> str:
    """Extrai nome da empresa de 'Empresa1 - NOME LTDA' ou 'EmpresaNOME LTDA'."""
    m = re.search(r"Empresa\d*\s*[-–]?\s*(.+?)(?:\s+Per[ií]odo|\s+Data\s|\s*$)", line)
    return m.group(1).strip() if m else ""


def _progress(current: int, total: int, label: str) -> None:
    pct = int(current / total * 100)
    bar = "#" * (pct // 5) + "-" * (20 - pct // 5)
    print(f"\r  [{bar}] {pct:3d}%  pagina {current}/{total}  {label:<30}", end="", flush=True)


# ─── 1. Contas a Pagar ────────────────────────────────────────────────────────

def _parse_contas_pagar(path: Path) -> pd.DataFrame:
    """
    Layout por pagina (texto):
      Empresa1 - NOME LTDA
      Data de vencimentoDD/MM/AAAA
      CREDOR  PPC.XXX  XXXX/1  QT  IND  DIAS  VALOR  ACRES  DESC  TOTAL
      Total do dia ...
    """
    rows = []
    empresa = ""
    vencimento = ""

    with pdfplumber.open(path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, 1):
            _progress(i, total, path.name[:25])
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue

                e = _empresa_da_linha(line)
                if e:
                    empresa = e
                    continue

                m = re.match(r"Data de vencimento\s*(\d{2}/\d{2}/\d{4})", line)
                if m:
                    vencimento = m.group(1)
                    continue

                if line.startswith(("Total do", "Obs:", "Credor", "Após", "Período", "Juros")):
                    continue

                # Linha de dado: termina com 4 valores monetários
                m = re.search(
                    rf"({MONEY})\s+({MONEY})\s+({MONEY})\s+({MONEY})\s*$", line
                )
                if not m:
                    continue

                total_val, desc, acr, valor = m.group(4), m.group(3), m.group(2), m.group(1)
                prefix = line[: m.start()].strip()

                # Extrai documento (PPC.XXXX ou BOL.XXX ou similar) e lançamento
                doc_m = re.search(r"([A-Z]+\.\s*\d+)\s+([\d/]+)\s+", prefix)
                if doc_m:
                    documento = doc_m.group(1).replace(" ", "")
                    lancamento = doc_m.group(2)
                    credor = prefix[: doc_m.start()].strip()
                else:
                    documento = ""
                    lancamento = ""
                    credor = prefix

                rows.append({
                    "empresa": empresa,
                    "vencimento": vencimento,
                    "credor": credor,
                    "documento": documento,
                    "lancamento": lancamento,
                    "valor_vencto": valor,
                    "acrescimo": acr,
                    "desconto": desc,
                    "total": total_val,
                })

    print()
    return pd.DataFrame(rows)


# ─── 2. Contas a Receber ──────────────────────────────────────────────────────

def _parse_contas_receber(path: Path) -> pd.DataFrame:
    """
    Layout texto:
      Empresa1 - NOME LTDA
      [header com 'Data  Valor original  Saldo atual ...']
      Setembro/2020  9.193,62  9.193,62  0,00  1.227,35  0,00  0,00  7.966,27
    """
    rows = []
    empresa = ""

    with pdfplumber.open(path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, 1):
            _progress(i, total, path.name[:25])
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                e = _empresa_da_linha(line)
                if e:
                    empresa = e
                    continue
                if line.startswith(("Data", "Total", "Após", "Período", "Desconto")):
                    continue

                # Data no formato 'Mes/AAAA' ou 'DD/MM/AAAA'
                m_data = re.match(r"(\w+/\d{4}|\d{2}/\d{2}/\d{4})", line)
                if not m_data:
                    continue

                valores = re.findall(MONEY, line)
                if len(valores) < 2:
                    continue

                rows.append({
                    "empresa": empresa,
                    "data": m_data.group(1),
                    "valor_original": valores[0] if len(valores) > 0 else "",
                    "saldo_atual": valores[1] if len(valores) > 1 else "",
                    "acrescimo": valores[2] if len(valores) > 2 else "",
                    "desconto": valores[3] if len(valores) > 3 else "",
                    "seguro": valores[4] if len(valores) > 4 else "",
                    "taxa_adm": valores[5] if len(valores) > 5 else "",
                    "total": valores[6] if len(valores) > 6 else "",
                })

    print()
    return pd.DataFrame(rows)


# ─── 3. Contas Pagas ──────────────────────────────────────────────────────────

def _parse_contas_pagas(path: Path) -> pd.DataFrame:
    """
    Layout:
      Empresa2 - JFK CONSTRUÇÕES LTDA
      Data da baixaDD/MM/AAAA
      CREDOR  CD  BOL.XXX  XXXX  QT  DD/MM/AAAA  SEQ  88,78T  0,00  0,00  88,78
    """
    rows = []
    empresa = ""
    data_baixa = ""
    seen: set[str] = set()

    with pdfplumber.open(path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, 1):
            _progress(i, total, path.name[:25])
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                e = _empresa_da_linha(line)
                if e:
                    empresa = e
                    continue

                m = re.match(r"Data da baixa\s*(\d{2}/\d{2}/\d{4})", line)
                if m:
                    data_baixa = m.group(1)
                    continue

                if line.startswith(("Total do", "Credor", "Após", "Período")):
                    continue

                # Linha de dado: termina com 3 valores monetários (acr desc liq)
                # e tem um valor com 'T' opcional antes
                m = re.search(
                    rf"(\d{{1,3}}(?:\.\d{{3}})*,\d{{2}}T?)\s+({MONEY})\s+({MONEY})\s+({MONEY})\s*$",
                    line,
                )
                if not m:
                    continue

                valor_baixa = m.group(1).replace("T", "").strip()
                acr = m.group(2)
                desc = m.group(3)
                liquido = m.group(4)
                prefix = line[: m.start()].strip()

                # Data vencimento no prefix
                dt_vencto_m = re.search(r"(\d{2}/\d{2}/\d{4})", prefix)
                dt_vencto = dt_vencto_m.group(1) if dt_vencto_m else ""

                # Documento
                doc_m = re.search(r"([A-Z]+\.\s*\d+\w*)", prefix)
                documento = doc_m.group(1).replace(" ", "") if doc_m else ""

                # Credor = tudo antes do doc/data
                credor = prefix[:doc_m.start()].strip() if doc_m else prefix

                key = f"{empresa}|{data_baixa}|{credor}|{valor_baixa}"
                if key in seen:
                    continue
                seen.add(key)

                rows.append({
                    "empresa": empresa,
                    "data_baixa": data_baixa,
                    "credor": credor,
                    "documento": documento,
                    "dt_vencto": dt_vencto,
                    "valor_baixa": valor_baixa,
                    "acrescimo": acr,
                    "desconto": desc,
                    "liquido": liquido,
                })

    print()
    return pd.DataFrame(rows)


# ─── 4. Contas Recebidas ──────────────────────────────────────────────────────

def _parse_contas_recebidas(path: Path) -> pd.DataFrame:
    """
    Cada linha de dado começa com DD/MM/AAAA (data baixa).
    """
    rows = []
    empresa = ""

    with pdfplumber.open(path) as pdf:
        total = len(pdf.pages)
        for i, page in enumerate(pdf.pages, 1):
            _progress(i, total, path.name[:25])
            txt = page.extract_text() or ""
            for line in txt.splitlines():
                line = line.strip()
                if not line:
                    continue
                e = _empresa_da_linha(line)
                if e:
                    empresa = e
                    continue
                if line.startswith(("Total do", "Dt.", "Coluna", "Observa", "Período")):
                    continue

                # Data de baixa no início
                m_dt = re.match(r"(\d{2}/\d{2}/\d{4})\s+(.+)", line)
                if not m_dt:
                    continue

                dt_baixa = m_dt.group(1)
                resto = m_dt.group(2)

                valores = re.findall(MONEY, resto)
                if len(valores) < 2:
                    continue

                # NFSE / CT / REC número
                doc_m = re.search(r"(NFSE\.\d+|CT\.\d+\s*\w*|REC\.\d+|BOL\.\S+)", resto)
                documento = doc_m.group(1) if doc_m else ""

                # Data vencimento (segunda data no linha)
                datas = re.findall(r"\d{2}/\d{2}/\d{4}", resto)
                dt_vencto = datas[0] if datas else ""

                # Cliente = tudo antes do documento
                cliente = resto[:doc_m.start()].strip() if doc_m else ""
                # Remove datas do cliente
                cliente = re.sub(r"\d{2}/\d{2}/\d{4}", "", cliente).strip()

                rows.append({
                    "empresa": empresa,
                    "dt_baixa": dt_baixa,
                    "cliente": cliente,
                    "documento": documento,
                    "dt_vencto": dt_vencto,
                    "valor_baixa": valores[0],
                    "acrescimo": valores[1] if len(valores) > 1 else "",
                    "seguro": valores[2] if len(valores) > 2 else "",
                    "taxa_adm": valores[3] if len(valores) > 3 else "",
                    "desconto": valores[4] if len(valores) > 4 else "",
                    "liquido": valores[5] if len(valores) > 5 else "",
                })

    print()
    return pd.DataFrame(rows)


# ─── 5. Faturamento ───────────────────────────────────────────────────────────

def _parse_faturamento(paths: list[Path]) -> pd.DataFrame:
    """
    Linha de dado: tem data de emissão DD/MM/AAAA e pelo menos 2 valores monetários.
    """
    rows = []

    for path in paths:
        empresa = path.stem
        with pdfplumber.open(path) as pdf:
            total = len(pdf.pages)
            for i, page in enumerate(pdf.pages, 1):
                _progress(i, total, path.name[:25])
                txt = page.extract_text() or ""
                for line in txt.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    m_e = re.search(r"Empresa\s*(.+?)(?:\s+Per[ií]odo|\s*$)", line)
                    if m_e:
                        empresa = m_e.group(1).strip()
                        continue
                    if line.startswith(("Total", "Centro", "07/05", "SIENGE")):
                        continue

                    # Linha de dado: contém data de emissão
                    m_data = re.search(r"(\d{2}/\d{2}/\d{4})", line)
                    if not m_data:
                        continue

                    data_emissao = m_data.group(1)
                    valores = re.findall(MONEY, line)
                    if not valores:
                        continue

                    # Documento (NFSE.XXX ou CT.XXX ou REC.XXX)
                    doc_m = re.search(r"(NFSE\.\d+|CT\.\d+\s*\w*|REC\.\d+|BOL\.\S+)", line)
                    documento = doc_m.group(1) if doc_m else ""

                    # Tudo antes do documento = centro_custo + cliente
                    prefix = line[:doc_m.start()].strip() if doc_m else ""

                    rows.append({
                        "arquivo": path.name,
                        "empresa": empresa,
                        "centro_custo_cliente": prefix,
                        "documento": documento,
                        "data_emissao": data_emissao,
                        "valor_bruto": valores[0],
                        "total_liquido": valores[-1] if len(valores) > 1 else valores[0],
                    })

        print()

    return pd.DataFrame(rows)


# ─── orquestrador ─────────────────────────────────────────────────────────────

def main() -> None:
    conversoes: list[tuple[str, pd.DataFrame | None]] = []

    # 1. Contas a Pagar
    p = PDF_DIR / "Contas a Pagar (por Data de Vencimento).pdf"
    if p.exists():
        print(f"\nConvertendo: {p.name}")
        df = _parse_contas_pagar(p)
        out = OUT_DIR / "contas_a_pagar.xlsx"
        df.to_excel(out, index=False)
        print(f"  -> {out.name}  ({len(df)} linhas)")
        conversoes.append(("contas_a_pagar", df))
    else:
        print(f"  [ausente] {p.name}")

    # 2. Contas a Receber
    p = PDF_DIR / "Contas a Receber (por Data de Vencimento).pdf"
    if p.exists():
        print(f"\nConvertendo: {p.name}")
        df = _parse_contas_receber(p)
        out = OUT_DIR / "contas_a_receber.xlsx"
        df.to_excel(out, index=False)
        print(f"  -> {out.name}  ({len(df)} linhas)")
        conversoes.append(("contas_a_receber", df))

    # 3. Contas Pagas (usa apenas o PDF sem duplicar; prefere o de maior periodo)
    for nome in ["Contas Pagas (por Data).pdf", "contaspagas(por data).pdf"]:
        p = PDF_DIR / nome
        if p.exists():
            print(f"\nConvertendo: {p.name}")
            df = _parse_contas_pagas(p)
            out = OUT_DIR / "contas_pagas.xlsx"
            df.to_excel(out, index=False)
            print(f"  -> {out.name}  ({len(df)} linhas)")
            conversoes.append(("contas_pagas", df))
            break  # usa só o primeiro encontrado

    # 4. Contas Recebidas
    p = PDF_DIR / "Contas Recebidas (por Data de Recebimento).pdf"
    if p.exists():
        print(f"\nConvertendo: {p.name}")
        df = _parse_contas_recebidas(p)
        out = OUT_DIR / "contas_recebidas.xlsx"
        df.to_excel(out, index=False)
        print(f"  -> {out.name}  ({len(df)} linhas)")
        conversoes.append(("contas_recebidas", df))

    # 5. Faturamento (todos os PDFs de relatório)
    fat_paths = sorted(
        p for p in PDF_DIR.glob("*.pdf")
        if any(k in p.name.lower() for k in ("faturamento", "relatorio"))
    )
    if fat_paths:
        print(f"\nConvertendo faturamento ({len(fat_paths)} PDFs)...")
        df = _parse_faturamento(fat_paths)
        out = OUT_DIR / "faturamento.xlsx"
        df.to_excel(out, index=False)
        print(f"  -> {out.name}  ({len(df)} linhas)")
        conversoes.append(("faturamento", df))

    print("\n" + "=" * 50)
    print("Conversao concluida!")
    print(f"Arquivos Excel em: {OUT_DIR}")
    for nome, df in conversoes:
        if df is not None:
            print(f"  {nome}.xlsx  ->  {len(df)} registros, {len(df.columns)} colunas")
    print("=" * 50)


if __name__ == "__main__":
    main()
