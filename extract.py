import os
import json
import app

output_dir = r"C:\Users\dinam\OneDrive\Desktop\pendrive\API"
os.makedirs(output_dir, exist_ok=True)

def write_txt(filename, data):
    path = os.path.join(output_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, list):
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        else:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"Salvo: {path}")

print("Obtendo usuários...")
usuarios_resp = app.fetch_all("/public/api/v1/users")
usuarios = usuarios_resp.get("data", {}).get("results", [])

print("Obtendo empresas...")
empresas_resp = app.fetch_all("/public/api/v1/companies")
empresas = empresas_resp.get("data", {}).get("results", [])

# Solicitantes (usuários ou cache local?)
# Pelo nome, vamos verificar se tem algum campo de role ou se podemos extrair do cache
solicitantes = []
administradores = []

for u in usuarios:
    role = str(u.get("role", "")).lower()
    profile = str(u.get("profile", "")).lower()
    name = str(u.get("name", "")).lower()
    
    is_admin = ("admin" in role or "admin" in profile)
    is_solic = ("solicitante" in role or "solicitante" in profile or "comprador" in role)
    
    if is_admin:
        administradores.append(u)
    if is_solic:
        solicitantes.append(u)

# Se não achou solicitantes explicitamente, mas temos o cache de pedidos:
if not solicitantes:
    with open("data/solicitantes-cache.json", "r", encoding="utf-8") as f:
        cache = json.load(f)
        for k, v in cache.items():
            solicitantes.append({"id": k, "name": v, "type": "Extraido de pedidos"})

# Escrevendo arquivos conforme solicitado
write_txt("usuarios.txt", usuarios)
write_txt("empresas.txt", empresas)
write_txt("todas_as_empresas.txt", empresas)
write_txt("todas_as_empresas_cadastradas.txt", empresas)
write_txt("solicitantes.txt", solicitantes)
write_txt("todos_os_solicitantes.txt", solicitantes)
write_txt("administradores.txt", administradores)
write_txt("usuarios_e_empresas.txt", {"usuarios": usuarios, "empresas": empresas})

print("Concluído!")
