"""Gera data/sienge_raw/Margem.xlt com Receita - Custos por mês desde 2019."""
import csv
import io
from pathlib import Path
import re

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "sienge_raw"


def to_num(v: str) -> float:
    s = re.sub(r"R\$", "", str(v or ""), flags=re.IGNORECASE).strip()
    s = s.replace("%", "").replace(" ", "")
    # Formato BR: 1.234,56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def read_csv(path: Path) -> list[dict]:
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    lines = [l for l in text.splitlines() if l.strip() and not l.startswith("Fonte:")]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=";")
    return [{k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()} for row in reader]


def fmt_brl(n: float) -> str:
    return f"R$ {n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# Lê receita_total.csv
r_rows: dict[str, float] = {}
for row in read_csv(RAW_DIR / "receita_total.csv"):
    m = row.get("MES", "").strip()
    if m and re.match(r"^\d{4}-\d{2}$", m):
        r_rows[m] = to_num(row.get("RECEITA_OPERACIONAL", "0"))

# Lê MC_geral.csv
mc_rows: dict[str, dict] = {}
for row in read_csv(RAW_DIR / "MC_geral.csv"):
    m = row.get("MES", "").strip()
    if m and re.match(r"^\d{4}-\d{2}$", m):
        mc_rows[m] = {
            "receita": to_num(row.get("RECEITA_BRUTA", "0")),
            "custos": to_num(row.get("TOTAL_VARIAVEIS", "0")),
            "mc_orig": to_num(row.get("MC", "0")),
        }

months = sorted(set(list(r_rows.keys()) + list(mc_rows.keys())))
print(f"Meses: {len(months)}  ({months[0]} → {months[-1]})")

out_lines = ["MES;RECEITA_OPERACIONAL;CUSTOS_VARIAVEIS;MC;MARGEM"]
tot_r = 0.0
tot_mc = 0.0

for m in months:
    rec = r_rows.get(m) or mc_rows.get(m, {}).get("receita", 0.0)
    cus = mc_rows.get(m, {}).get("custos", 0.0)
    mc_val = rec - cus
    pct = (mc_val / rec * 100) if rec else 0.0
    tot_r += rec
    tot_mc += mc_val
    out_lines.append(f"{m};{fmt_brl(rec)};{fmt_brl(cus)};{fmt_brl(mc_val)};{pct:.2f}%")

tot_pct = (tot_mc / tot_r * 100) if tot_r else 0.0
out_lines.append(f"TOTAL;{fmt_brl(tot_r)};-;{fmt_brl(tot_mc)};{tot_pct:.2f}%")

out_path = RAW_DIR / "Margem.xlt"
out_path.write_text("\n".join(out_lines), encoding="utf-8-sig")
print(f"Criado: {out_path}")
print(f"  Receita total : {fmt_brl(tot_r)}")
print(f"  MC total      : {fmt_brl(tot_mc)}")
print(f"  Margem geral  : {tot_pct:.2f}%")
