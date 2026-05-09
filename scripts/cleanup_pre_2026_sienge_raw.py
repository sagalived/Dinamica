#!/usr/bin/env python3
"""
Script para limpar dados pré-2026 do sienge_raw
Remove arquivos CSV/TXT antigos e deixa apenas dados de 2026+
"""

import os
import csv
from pathlib import Path
from datetime import datetime

DATA_RAW_PATH = Path(__file__).parent.parent / "data" / "sienge_raw"
YEAR_2026 = 2026

def is_pre_2026_file(filepath: str) -> bool:
    """Verifica se um arquivo contém dados de antes de 2026"""
    if not os.path.exists(filepath):
        return False
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f, delimiter=';')
            if reader is None:
                return False
            
            for row in reader:
                # Procura por campos de data
                date_fields = [
                    'DATA', 'DATA_CRIACAO', 'DATA_ATUALIZACAO', 
                    'VENCIMENTO', 'VENCTO', 'BAIXA', 'DATE', 
                    'CREATED_AT', 'UPDATED_AT', 'DUE_DATE'
                ]
                
                for field in date_fields:
                    if field in row and row[field]:
                        try:
                            # Tenta parsear a data
                            date_str = str(row[field]).strip()
                            if '/' in date_str:
                                day, month, year = date_str.split('/')
                                year = int(year)
                            elif '-' in date_str:
                                year = int(date_str.split('-')[0])
                            else:
                                continue
                            
                            if year < YEAR_2026:
                                return True
                        except (ValueError, IndexError):
                            continue
    except Exception as e:
        print(f"Erro ao processar {filepath}: {e}")
    
    return False

def clean_sienge_raw():
    """Limpa dados pré-2026 do sienge_raw"""
    if not DATA_RAW_PATH.exists():
        print(f"Caminho {DATA_RAW_PATH} não encontrado")
        return
    
    cleaned_count = 0
    total_count = 0
    
    # Processa recursivamente todos os arquivos
    for root, dirs, files in os.walk(DATA_RAW_PATH):
        for file in files:
            if file.endswith(('.csv', '.txt')):
                filepath = os.path.join(root, file)
                total_count += 1
                
                print(f"Verificando: {filepath}")
                
                if is_pre_2026_file(filepath):
                    try:
                        os.remove(filepath)
                        cleaned_count += 1
                        print(f"  ✓ Removido (dados pré-2026)")
                    except Exception as e:
                        print(f"  ✗ Erro ao remover: {e}")
                else:
                    print(f"  ✓ Mantido (2026+)")
    
    print(f"\n{'='*50}")
    print(f"Limpeza concluída!")
    print(f"Total de arquivos processados: {total_count}")
    print(f"Arquivos removidos: {cleaned_count}")

if __name__ == '__main__':
    print("Limpando dados pré-2026 do sienge_raw...")
    clean_sienge_raw()
