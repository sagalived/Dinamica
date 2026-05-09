/**
 * CENTRO DE CUSTO — Relatório Financeiro de Obras
 * - Exibe relatório financeiro de obras do Sienge
 * - Sincroniza dados automaticamente
 * - Filtra por período e obras
 */

import React, { useEffect, useState, useMemo } from 'react';
import { motion } from 'motion/react';
import { RefreshCw, TrendingUp, TrendingDown, DollarSign, Calendar as CalendarIcon, X } from 'lucide-react';
import { format, parse, startOfDay, endOfDay } from 'date-fns';
import { cn } from '../../lib/utils';
import { useSienge } from '../../contexts/SiengeContext';
import { useTheme } from '../../contexts/ThemeContext';
import { api } from '../../lib/api';
import { BuildingIdFilter } from '../../components/BuildingIdFilter';
import { CompanyIdFilter } from '../../components/CompanyIdFilter';
import { FilterBar, FilterState } from '../../components/FilterBar';

interface CentroCustoRow {
  id: string;
  codigoObra: string;
  nomeObra: string;
  empresa: string;
  custoDireto: number;
  custoIndireto: number;
  custoTotal: number;
  receita: number;
  margem: number;
  margemPercentual: number;
  dataAtualizacao: string;
}

const fmt = (v: number) =>
  new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(v);

export function CentroDeCustoTab() {
  const { isDark } = useTheme();
  const { 
    buildings,
    companies,
    dataRevision,
    loading: syncing,
    refresh: syncSienge,
    selectedCompany, setSelectedCompany,
    fcSelectedBuilding: selectedBuilding, setFcSelectedBuilding: setSelectedBuilding,
  } = useSienge();

  const [centroCustoData, setCentroCustoData] = useState<CentroCustoRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [startDate, setStartDate] = useState<Date | undefined>(startOfDay(new Date(new Date().getFullYear(), 0, 1)));
  const [endDate, setEndDate] = useState<Date | undefined>(endOfDay(new Date()));

  // Fetch Centro de Custo data
  useEffect(() => {
    let cancelled = false;

    const fetchCentroCusto = async () => {
      setLoading(true);
      setError('');
      try {
        const response = await api.get('/sienge/centro-custo');
        if (cancelled) return;
        setCentroCustoData(Array.isArray(response?.data) ? response.data : []);
      } catch (err: any) {
        if (cancelled) return;
        setError(err.message || 'Erro ao carregar dados de centro de custo');
        setCentroCustoData([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchCentroCusto();
    return () => {
      cancelled = true;
    };
  }, [dataRevision]);

  // Filter data
  const filteredData = useMemo(() => {
    return centroCustoData.filter((row) => {
      // Company filter
      if (selectedCompany !== 'all') {
        const buildingMatch = buildings.find((b: any) => String(b.id) === selectedBuilding || String(b.code) === selectedBuilding);
        if (buildingMatch && String(buildingMatch.companyId ?? buildingMatch.company_id) !== selectedCompany) {
          return false;
        }
      }

      // Building filter
      if (selectedBuilding !== 'all') {
        const buildingMatch = buildings.find((b: any) => 
          String(b.id) === selectedBuilding || 
          String(b.code) === selectedBuilding ||
          String(b.code) === row.codigoObra
        );
        if (!buildingMatch) return false;
      }

      return true;
    });
  }, [centroCustoData, buildings, selectedCompany, selectedBuilding]);

  // Summary calculations
  const summary = useMemo(() => {
    return {
      totalCustoDireto: filteredData.reduce((sum, r) => sum + r.custoDireto, 0),
      totalCustoIndireto: filteredData.reduce((sum, r) => sum + r.custoIndireto, 0),
      totalCusto: filteredData.reduce((sum, r) => sum + r.custoTotal, 0),
      totalReceita: filteredData.reduce((sum, r) => sum + r.receita, 0),
      totalMargem: filteredData.reduce((sum, r) => sum + r.margem, 0),
    };
  }, [filteredData]);

  const margemPercentualGeral = summary.totalReceita > 0 
    ? ((summary.totalMargem / summary.totalReceita) * 100) 
    : 0;

  return (
    <motion.div key="centro-custo-tab" initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -20 }} className="space-y-8">
      {/* Header */}
      <div className="space-y-6">
        <div className="flex items-start justify-between gap-4 flex-wrap">
          <div className="space-y-2">
            <h1 className="text-3xl font-black uppercase tracking-tight text-white">
              Centro de Custo
            </h1>
            <p className="text-gray-400 text-sm">
              Relatório Financeiro de Obras — Sienge
            </p>
          </div>
          <button
            onClick={syncSienge}
            disabled={syncing}
            className="h-9 px-4 bg-[#1B3C58] hover:bg-[#234b6e] text-white font-bold rounded-xl flex items-center gap-2 text-xs transition-all disabled:opacity-50"
          >
            <RefreshCw size={14} className={cn(syncing && 'animate-spin')} />
            {syncing ? 'Sincronizando...' : 'Sincronizar Sienge'}
          </button>
        </div>
      </div>

      {/* Filters */}
      <FilterBar
        onFilter={(filters: FilterState) => {
          setSelectedCompany(filters.companyId || 'all');
          setSelectedBuilding(filters.buildingId || 'all');
          if (filters.startDate) setStartDate(parse(filters.startDate, 'yyyy-MM-dd', new Date()));
          if (filters.endDate) setEndDate(endOfDay(parse(filters.endDate, 'yyyy-MM-dd', new Date())));
        }}
        onClear={() => {
          setSelectedCompany('all');
          setSelectedBuilding('all');
          setStartDate(undefined);
          setEndDate(undefined);
        }}
      />

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl">
          <p className="text-[10px] font-black uppercase tracking-widest text-gray-400 mb-2">Custo Direto</p>
          <p className="text-2xl font-black text-white">{fmt(summary.totalCustoDireto)}</p>
        </div>
        <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl">
          <p className="text-[10px] font-black uppercase tracking-widest text-gray-400 mb-2">Custo Indireto</p>
          <p className="text-2xl font-black text-white">{fmt(summary.totalCustoIndireto)}</p>
        </div>
        <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl">
          <p className="text-[10px] font-black uppercase tracking-widest text-gray-400 mb-2">Custo Total</p>
          <p className="text-2xl font-black text-white">{fmt(summary.totalCusto)}</p>
        </div>
        <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl">
          <p className="text-[10px] font-black uppercase tracking-widest text-gray-400 mb-2">Receita</p>
          <p className="text-2xl font-black text-green-400">{fmt(summary.totalReceita)}</p>
        </div>
        <div className="bg-[#161618] border border-white/5 p-4 rounded-2xl shadow-2xl">
          <p className="text-[10px] font-black uppercase tracking-widest text-gray-400 mb-2">Margem</p>
          <p className="text-2xl font-black text-orange-400">{margemPercentualGeral.toFixed(1)}%</p>
        </div>
      </div>

      {/* Table */}
      <div className="bg-[#161618] border border-white/5 rounded-2xl shadow-2xl overflow-hidden">
        {loading ? (
          <div className="p-8 text-center text-gray-400">
            <p>Carregando dados...</p>
          </div>
        ) : error ? (
          <div className="p-8 text-center text-red-400">
            <p>{error}</p>
          </div>
        ) : filteredData.length === 0 ? (
          <div className="p-8 text-center text-gray-400">
            <p>Nenhum dado encontrado</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="border-b border-white/5 bg-black/40">
                <tr>
                  <th className="px-4 py-3 text-left text-[11px] font-black uppercase tracking-widest text-orange-500">Código</th>
                  <th className="px-4 py-3 text-left text-[11px] font-black uppercase tracking-widest text-orange-500">Obra</th>
                  <th className="px-4 py-3 text-right text-[11px] font-black uppercase tracking-widest text-orange-500">Custo Direto</th>
                  <th className="px-4 py-3 text-right text-[11px] font-black uppercase tracking-widest text-orange-500">Custo Indireto</th>
                  <th className="px-4 py-3 text-right text-[11px] font-black uppercase tracking-widest text-orange-500">Custo Total</th>
                  <th className="px-4 py-3 text-right text-[11px] font-black uppercase tracking-widest text-orange-500">Receita</th>
                  <th className="px-4 py-3 text-right text-[11px] font-black uppercase tracking-widest text-orange-500">Margem %</th>
                </tr>
              </thead>
              <tbody>
                {filteredData.map((row, idx) => (
                  <tr key={row.id} className={cn('border-b border-white/5 hover:bg-white/5 transition', idx % 2 === 0 ? 'bg-black/20' : '')}>
                    <td className="px-4 py-3 text-sm text-gray-300">{row.codigoObra}</td>
                    <td className="px-4 py-3 text-sm text-white font-bold">{row.nomeObra}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-300">{fmt(row.custoDireto)}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-300">{fmt(row.custoIndireto)}</td>
                    <td className="px-4 py-3 text-sm text-right text-gray-300">{fmt(row.custoTotal)}</td>
                    <td className="px-4 py-3 text-sm text-right text-green-400 font-bold">{fmt(row.receita)}</td>
                    <td className="px-4 py-3 text-sm text-right text-orange-400 font-bold">{row.margemPercentual.toFixed(1)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </motion.div>
  );
}
