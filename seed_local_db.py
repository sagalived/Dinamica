"""
Popula o banco SQLite local com dados reais do Sienge.
Fontes:
  - data/*.json            -> obras, empresas, credores, usuarios
  - basecalculo/.../*.json -> pedidos, financeiro, receber (2025 + 2026)
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent

# Adiciona o root ao path para importar o backend
sys.path.insert(0, str(ROOT))

from backend.database import Base, SessionLocal, engine
from backend.models import SiengeSnapshot

Base.metadata.create_all(bind=engine)


def load_json(path: Path, encoding: str = "utf-8") -> any:
    return json.loads(path.read_bytes().decode(encoding))


def write_snapshot(db, key: str, payload) -> None:
    from sqlalchemy import select
    serialized = json.dumps(payload, ensure_ascii=False)
    from backend.models import SiengeSnapshot
    row = db.scalar(select(SiengeSnapshot).where(SiengeSnapshot.key == key))
    if row is None:
        db.add(SiengeSnapshot(key=key, payload=serialized))
    else:
        row.payload = serialized
    db.commit()
    print(f"  ✓ {key} ({len(payload) if isinstance(payload, (list, dict)) else 'object'})")


def _row_signature(item: dict) -> str:
    """Assinatura estável para deduplicar sem perder anos diferentes com o mesmo id."""
    sig = {
        "id": item.get("id"),
        "companyId": item.get("companyId"),
        "buildingId": item.get("buildingId"),
        "idObra": item.get("idObra"),
        "date": item.get("dataVencimento") or item.get("dueDate") or item.get("date") or item.get("operationDate") or item.get("paymentDate"),
        "amount": item.get("valor") if item.get("valor") is not None else item.get("rawValue") if item.get("rawValue") is not None else item.get("amount") if item.get("amount") is not None else item.get("value"),
        "type": item.get("type"),
        "statementType": item.get("statementType"),
        "statementOrigin": item.get("statementOrigin"),
        "documentNumber": item.get("documentNumber"),
        "billId": item.get("billId"),
    }
    return json.dumps(sig, sort_keys=True, ensure_ascii=False)


def merge_lists(a: list, b: list) -> list:
    """Mescla listas removendo apenas duplicatas exatas de negócio (não só por id)."""
    seen = {}
    for item in a + b:
        if not isinstance(item, dict):
            continue
        key = _row_signature(item)
        if key not in seen:
            seen[key] = item
    return list(seen.values())


def main():
    DATA_DIR = ROOT / "data"
    BASE_DIR = ROOT / "basecalculo" / "sienge-dashboard"

    print("Abrindo banco de dados SQLite...")
    db = SessionLocal()

    try:
        # ── Dados estáticos ──────────────────────────────────────────────
        print("\n[1/4] Importando dados estáticos (obras, empresas, credores, usuarios)...")

        obras = load_json(DATA_DIR / "obras.json")
        write_snapshot(db, "obras.json", obras)

        empresas = load_json(DATA_DIR / "empresas.json")
        write_snapshot(db, "empresas.json", empresas)

        credores = load_json(DATA_DIR / "credores.json")
        write_snapshot(db, "credores.json", credores)

        usuarios = load_json(DATA_DIR / "usuarios.json")
        write_snapshot(db, "usuarios.json", usuarios)

        # ── Dados transacionais (2025 + 2026) ───────────────────────────
        print("\n[2/4] Carregando dados transacionais de 2025...")
        d2025 = load_json(BASE_DIR / "tmp_filtered_2025.json", encoding="utf-16")

        print("[3/4] Carregando dados transacionais de 2026...")
        d2026 = load_json(BASE_DIR / "tmp_filtered.json", encoding="utf-16")

        print("\n[4/4] Mesclando e salvando no banco...")

        pedidos = merge_lists(d2025.get("pedidos", []), d2026.get("pedidos", []))
        write_snapshot(db, "pedidos.json", pedidos)

        financeiro = merge_lists(d2025.get("financeiro", []), d2026.get("financeiro", []))
        write_snapshot(db, "financeiro.json", financeiro)

        receber = merge_lists(d2025.get("receber", []), d2026.get("receber", []))
        write_snapshot(db, "receber.json", receber)

        # ── Metadados de sync ────────────────────────────────────────────
        latest_sync = d2026.get("latestSync", {})
        latest_sync["message"] = "Dados importados do cache local (Sienge indisponível)"
        latest_sync["source"] = "local_cache"
        write_snapshot(db, "sienge_sync_meta", latest_sync)

        # ── Resumo ───────────────────────────────────────────────────────
        print("\n✅ Seed concluído!")
        print(f"   obras:      {len(obras)}")
        print(f"   empresas:   {len(empresas)}")
        print(f"   credores:   {len(credores)}")
        print(f"   usuarios:   {len(usuarios)}")
        print(f"   pedidos:    {len(pedidos)} (2025+2026)")
        print(f"   financeiro: {len(financeiro)} (2025+2026)")
        print(f"   receber:    {len(receber)} (2025+2026)")

    finally:
        db.close()


if __name__ == "__main__":
    main()
