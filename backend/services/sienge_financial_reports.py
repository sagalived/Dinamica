"""
Serviço para sincronizar dados do Relatório Financeiro de Obras do Sienge
URL: https://dinamicaempreendimentos.sienge.com.br/sienge/8/index.html#/common/page/2534
"""

import json
from datetime import datetime
from sqlalchemy.orm import Session
from backend.models import SiengeFinancialReport, Building
from backend.services.sienge_client import sienge_client


async def sync_financial_reports(db: Session) -> dict:
    """
    Sincroniza o Relatório Financeiro de Obras do Sienge.
    
    O Sienge armazena esses dados em um endpoint específico que retorna
    um JSON com o relatório financeiro de cada obra/empreendimento.
    
    Returns:
        dict com estatísticas de sincronização
    """
    
    stats = {
        'total': 0,
        'synced': 0,
        'errors': 0,
        'timestamp': datetime.utcnow().isoformat(),
    }
    
    try:
        # Buscar todas as obras (buildings) do banco local
        buildings = db.query(Building).filter(Building.active == True).all()
        
        for building in buildings:
            try:
                building_id = building.id
                building_code = building.code if hasattr(building, 'code') else None
                company_id = building.company_id
                company_name = building.company_name
                
                # Aqui você faria a chamada à API do Sienge para buscar dados
                # da obra específica. Por enquanto, estruturamos para receber
                # dados que viriam de um endpoint tipo:
                # GET /api/obras/{building_id}/relatorio-financeiro
                
                # Exemplo de estrutura esperada:
                financial_data = await fetch_building_financial_report(building_id)
                
                if financial_data:
                    # Atualizar ou criar registro
                    report = db.query(SiengeFinancialReport).filter(
                        SiengeFinancialReport.building_id == str(building_id)
                    ).first()
                    
                    if not report:
                        report = SiengeFinancialReport()
                    
                    # Atualizar campos
                    report.building_id = str(building_id)
                    report.building_code = building_code or building.code if hasattr(building, 'code') else None
                    report.building_name = building.name
                    report.company_id = str(company_id) if company_id else None
                    report.company_name = company_name
                    report.custo_direto = financial_data.get('custoDireto', 0.0)
                    report.custo_indireto = financial_data.get('custoIndireto', 0.0)
                    report.custo_total = financial_data.get('custoTotal', 0.0)
                    report.receita = financial_data.get('receita', 0.0)
                    report.margem = financial_data.get('margem', 0.0)
                    report.margem_percentual = financial_data.get('margemPercentual', 0.0)
                    report.data_atualizacao = datetime.utcnow().strftime('%Y-%m-%d')
                    report.payload = json.dumps(financial_data)
                    
                    db.add(report)
                    stats['synced'] += 1
                
                stats['total'] += 1
                
            except Exception as e:
                print(f"Erro ao sincronizar obra {building_id}: {str(e)}")
                stats['errors'] += 1
                stats['total'] += 1
        
        db.commit()
        
    except Exception as e:
        print(f"Erro geral em sync_financial_reports: {str(e)}")
        stats['errors'] += 1
        db.rollback()
    
    return stats


async def fetch_building_financial_report(building_id: int) -> dict | None:
    """
    Busca o relatório financeiro de uma obra específica do Sienge.
    
    Nota: Esta função precisa ser implementada com base na API real do Sienge.
    Por enquanto, retorna um template de estrutura esperada.
    
    Args:
        building_id: ID da obra no Sienge
    
    Returns:
        dict com dados financeiros da obra ou None se não encontrado
    """
    
    # Exemplo de chamada que seria feita:
    # response = await sienge_client.get(f'/obras/{building_id}/relatorio-financeiro')
    # return response.json() if response.status_code == 200 else None
    
    # Por enquanto, retornamos None para indicar que não há dados
    return None


def get_financial_reports(db: Session, company_id: str | None = None, building_id: str | None = None) -> list[dict]:
    """
    Retorna relatórios financeiros sincronizados, com filtros opcionais.
    
    Args:
        db: Sessão do banco de dados
        company_id: ID da empresa (opcional)
        building_id: ID da obra (opcional)
    
    Returns:
        Lista de dicts com dados financeiros
    """
    
    query = db.query(SiengeFinancialReport)
    
    if company_id:
        query = query.filter(SiengeFinancialReport.company_id == company_id)
    
    if building_id:
        query = query.filter(SiengeFinancialReport.building_id == building_id)
    
    # Ordena por nome da obra
    query = query.order_by(SiengeFinancialReport.building_name)
    
    results = query.all()
    
    return [
        {
            'id': str(r.id),
            'codigoObra': r.building_code or r.building_id,
            'nomeObra': r.building_name,
            'empresa': r.company_name,
            'custoDireto': r.custo_direto,
            'custoIndireto': r.custo_indireto,
            'custoTotal': r.custo_total,
            'receita': r.receita,
            'margem': r.margem,
            'margemPercentual': r.margem_percentual,
            'dataAtualizacao': r.data_atualizacao,
        }
        for r in results
    ]


def cleanup_old_financial_reports(db: Session, keep_from_year: int = 2026) -> dict:
    """
    Remove registros de relatórios financeiros anteriores a um determinado ano.
    
    Args:
        db: Sessão do banco de dados
        keep_from_year: Ano a partir do qual manter dados (padrão: 2026)
    
    Returns:
        dict com estatísticas da limpeza
    """
    
    try:
        # Calcular data limite (01/01 do ano especificado)
        limit_date = f"{keep_from_year-1}-12-31"
        
        # Deletar registros anteriores
        deleted = db.query(SiengeFinancialReport).filter(
            SiengeFinancialReport.data_atualizacao <= limit_date
        ).delete()
        
        db.commit()
        
        return {
            'deleted': deleted,
            'timestamp': datetime.utcnow().isoformat(),
            'message': f'Deletados {deleted} registros anteriores a {keep_from_year}-01-01'
        }
        
    except Exception as e:
        db.rollback()
        return {
            'deleted': 0,
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }
