#!/usr/bin/env python3
"""
Varredura em sienge_raw para remover arquivos duplicados
Identifica e remove arquivos duplicados mantendo o mais recente
"""

import os
import hashlib
from pathlib import Path
from collections import defaultdict
from datetime import datetime

def get_file_hash(file_path):
    """Calcula hash MD5 do arquivo para detectar duplicatas"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except:
        return None

def get_file_info(file_path):
    """Retorna informações do arquivo"""
    try:
        stat = os.stat(file_path)
        return {
            'size': stat.st_size,
            'mtime': stat.st_mtime,
            'ctime': stat.st_ctime,
        }
    except:
        return None

def remove_duplicates(sienge_raw_dir, dry_run=True):
    """
    Varredura e remoção de duplicatas
    dry_run=True: apenas lista o que seria removido
    dry_run=False: realmente remove os arquivos
    """
    
    if not os.path.exists(sienge_raw_dir):
        print(f"[ERRO] Diretório não encontrado: {sienge_raw_dir}")
        return
    
    print(f"[SCAN] Varrendo diretório: {sienge_raw_dir}")
    print(f"{'[DRY RUN]' if dry_run else '[EXECUTE]'} Procurando por duplicatas...\n")
    
    # Mapear arquivos por hash e por nome
    hash_map = defaultdict(list)  # hash -> [file_paths]
    name_map = defaultdict(list)  # basename -> [file_paths]
    
    total_files = 0
    total_size = 0
    
    # Varrer todos os arquivos recursivamente
    for root, dirs, files in os.walk(sienge_raw_dir):
        for filename in files:
            if filename.endswith(('.txt', '.csv')):
                file_path = os.path.join(root, filename)
                total_files += 1
                
                file_info = get_file_info(file_path)
                if file_info:
                    total_size += file_info['size']
                    
                    # Agrupar por hash (conteúdo idêntico)
                    file_hash = get_file_hash(file_path)
                    if file_hash:
                        hash_map[file_hash].append({
                            'path': file_path,
                            'info': file_info
                        })
                    
                    # Agrupar por nome (mesmo arquivo, múltiplas versões)
                    basename = os.path.basename(file_path)
                    name_map[basename].append({
                        'path': file_path,
                        'info': file_info
                    })
    
    print(f"[OK] Total de arquivos encontrados: {total_files}")
    print(f"[OK] Tamanho total: {total_size / (1024*1024):.2f} MB\n")
    
    # Analisar duplicatas por hash (conteúdo idêntico)
    print("=" * 70)
    print("DUPLICATAS POR HASH (Conteúdo idêntico)")
    print("=" * 70)
    
    removed_by_content = []
    space_saved = 0
    
    for file_hash, files in hash_map.items():
        if len(files) > 1:
            print(f"\n[DUP] Hash: {file_hash[:8]}...")
            print(f"   Encontradas {len(files)} cópias do mesmo arquivo:\n")
            
            # Ordenar por data de modificação (mais recente primeiro)
            files_sorted = sorted(files, key=lambda x: x['info']['mtime'], reverse=True)
            
            # Manter o mais recente
            for idx, file_info in enumerate(files_sorted):
                file_path = file_info['path']
                mtime = datetime.fromtimestamp(file_info['info']['mtime'])
                size_kb = file_info['info']['size'] / 1024
                
                if idx == 0:
                    print(f"   [KEEP] MANTER: {file_path}")
                    print(f"      Modificado: {mtime.strftime('%Y-%m-%d %H:%M:%S')} | {size_kb:.1f} KB\n")
                else:
                    print(f"   [DEL] REMOVER: {file_path}")
                    print(f"      Modificado: {mtime.strftime('%Y-%m-%d %H:%M:%S')} | {size_kb:.1f} KB")
                    space_saved += file_info['info']['size']
                    removed_by_content.append(file_path)
    
    # Analisar duplicatas por nome (múltiplas versões do mesmo arquivo)
    print("\n" + "=" * 70)
    print("DUPLICATAS POR NOME (Múltiplas versões)")
    print("=" * 70)
    
    removed_by_version = []
    
    for basename, files in name_map.items():
        if len(files) > 1 and '_' in basename and (basename.endswith('.txt') or basename.endswith('.csv')):
            # Padrão: arquivo_20260101_120000.txt (timestamp no nome)
            print(f"\n[DUP] Arquivo: {basename}")
            print(f"   Encontradas {len(files)} versões:\n")
            
            # Ordenar por timestamp no nome ou data de modificação
            files_sorted = sorted(files, key=lambda x: x['info']['mtime'], reverse=True)
            
            for idx, file_info in enumerate(files_sorted):
                file_path = file_info['path']
                mtime = datetime.fromtimestamp(file_info['info']['mtime'])
                size_kb = file_info['info']['size'] / 1024
                
                if idx == 0:
                    print(f"   [KEEP] MANTER: {os.path.basename(file_path)}")
                    print(f"      Modificado: {mtime.strftime('%Y-%m-%d %H:%M:%S')} | {size_kb:.1f} KB\n")
                else:
                    print(f"   [DEL] REMOVER: {os.path.basename(file_path)}")
                    print(f"      Modificado: {mtime.strftime('%Y-%m-%d %H:%M:%S')} | {size_kb:.1f} KB")
                    if file_path not in removed_by_content:  # Não contar duas vezes
                        space_saved += file_info['info']['size']
                        removed_by_version.append(file_path)
    
    # Resumo e execução
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print(f"\n[STAT] Arquivos a remover por conteúdo idêntico: {len(removed_by_content)}")
    print(f"[STAT] Arquivos a remover por versão duplicada: {len(removed_by_version)}")
    print(f"[STAT] Espaço que será liberado: {space_saved / (1024*1024):.2f} MB")
    
    all_files_to_remove = list(set(removed_by_content + removed_by_version))
    
    if all_files_to_remove:
        if dry_run:
            print(f"\n[WARN] [DRY RUN] {len(all_files_to_remove)} arquivos seriam removidos")
            print("\nPara REALMENTE remover os duplicatas, execute:")
            print(f"  python scripts/cleanup_sienge_duplicates.py --execute")
        else:
            print(f"\n[DELETING] Removendo {len(all_files_to_remove)} arquivos duplicados...\n")
            removed_count = 0
            for file_path in all_files_to_remove:
                try:
                    os.remove(file_path)
                    print(f"   [OK] Removido: {file_path}")
                    removed_count += 1
                except Exception as e:
                    print(f"   [ERROR] Erro ao remover {file_path}: {e}")
            
            print(f"\n[SUCCESS] Limpeza concluida!")
            print(f"   - {removed_count} arquivos removidos")
            print(f"   - {space_saved / (1024*1024):.2f} MB liberados")
    else:
        print(f"\n[SUCCESS] Nenhum duplicado encontrado!")

if __name__ == '__main__':
    import sys
    
    sienge_raw_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'sienge_raw')
    
    # Verificar se --execute foi passado
    execute = '--execute' in sys.argv
    
    remove_duplicates(sienge_raw_dir, dry_run=not execute)
