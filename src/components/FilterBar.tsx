import React, { useState, useMemo, useEffect } from 'react';
import { X, Calendar } from 'lucide-react';
import { useSienge } from '../contexts/SiengeContext';
import { subMonths, format } from 'date-fns';

interface FilterBarProps {
  onFilter: (filters: FilterState) => void;
  onClear: () => void;
}

export interface FilterState {
  period: 'last3m' | 'allTime';
  year?: number;
  startDate: string;
  endDate: string;
  buildingId: string;
  buildingName: string;
  companyId: string;
  companyName: string;
}

export function FilterBar({ onFilter, onClear }: FilterBarProps) {
  // Visual desativado: manter somente o filtro legado escuro do LayoutPrincipal.
  return null;

  const { companies, buildings } = useSienge();
  const today = new Date();
  
  // Estado
  const [period, setPeriod] = useState<'last3m' | 'allTime'>('last3m');
  const [year, setYear] = useState<number>(2026);
  const [buildingId, setBuildingId] = useState<string>('');
  const [buildingName, setBuildingName] = useState<string>('');
  const [companyId, setCompanyId] = useState<string>('');
  const [companyName, setCompanyName] = useState<string>('');

  // Calcular datas automaticamente baseado no período
  const startDate = useMemo(() => {
    if (period === 'last3m') {
      return format(subMonths(today, 3), 'dd/MM/yyyy');
    } else {
      return '01/01/2026';
    }
  }, [period]);

  const endDate = useMemo(() => {
    return format(today, 'dd/MM/yyyy');
  }, []);

  // Auto-preencher empresa e obra quando ID da obra mudar
  useEffect(() => {
    if (buildingId) {
      const building = buildings.find(b => String(b.id) === buildingId);
      if (building) {
        setBuildingName(building.name || building.description || '');
        if (building.companyId || building.company_id) {
          const company = companies.find(c => String(c.id) === String(building.companyId || building.company_id));
          if (company) {
            setCompanyId(String(company.id));
            setCompanyName(company.name || '');
          }
        }
      }
    }
  }, [buildingId, buildings, companies]);

  const handleFilter = () => {
    const startStr = period === 'last3m' 
      ? format(subMonths(today, 3), 'yyyy-MM-dd')
      : '2026-01-01';
    
    const filters: FilterState = {
      period,
      year: period === 'allTime' ? year : undefined,
      startDate: startStr,
      endDate: format(today, 'yyyy-MM-dd'),
      buildingId,
      buildingName,
      companyId,
      companyName,
    };
    onFilter(filters);
  };

  const handleClear = () => {
    setPeriod('last3m');
    setYear(2026);
    setBuildingId('');
    setBuildingName('');
    setCompanyId('');
    setCompanyName('');
    onClear();
  };

  // Gerar lista de anos (2026 até ano atual + 1)
  const years = useMemo(() => {
    const currentYear = new Date().getFullYear();
    const list = [];
    for (let y = 2026; y <= currentYear + 1; y++) {
      list.push(y);
    }
    return list;
  }, []);

  return (
    <div className="w-full bg-gradient-to-r from-green-600 to-green-700 px-4 py-3 rounded-lg flex items-center gap-3 flex-wrap mb-6">
      
      {/* PERÍODO - Botão toggle */}
      <div className="flex items-center gap-2">
        <span className="text-white font-bold text-xs uppercase whitespace-nowrap">PERÍODO</span>
        <button
          onClick={() => setPeriod(period === 'last3m' ? 'allTime' : 'last3m')}
          className={`px-3 py-1 rounded font-bold text-xs transition ${
            period === 'last3m'
              ? 'bg-green-700 text-white border-2 border-white'
              : 'bg-white text-green-700 hover:bg-gray-100'
          }`}
        >
          {period === 'last3m' ? 'ÚLTIMOS 3 MESES' : 'PERÍODO TOTAL'}
        </button>
      </div>

      {/* DATA INICIAL */}
      <div className="flex items-center gap-1">
        <span className="text-white font-bold text-xs uppercase whitespace-nowrap">DATA INICIAL</span>
        <div className="flex items-center bg-white rounded px-2 py-1 gap-1">
          <Calendar size={14} className="text-green-600" />
          <input
            type="text"
            disabled
            value={startDate}
            className="border-0 outline-none bg-white text-xs font-semibold text-gray-800 w-20 cursor-default"
          />
        </div>
      </div>

      {/* DATA FINAL */}
      <div className="flex items-center gap-1">
        <span className="text-white font-bold text-xs uppercase whitespace-nowrap">DATA FINAL</span>
        <div className="flex items-center bg-white rounded px-2 py-1 gap-1">
          <Calendar size={14} className="text-green-600" />
          <input
            type="text"
            disabled
            value={endDate}
            className="border-0 outline-none bg-white text-xs font-semibold text-gray-800 w-20 cursor-default"
          />
        </div>
      </div>

      {/* ID OBRA */}
      <div className="flex items-center gap-1">
        <span className="text-white font-bold text-xs uppercase">ID</span>
        <input
          type="text"
          placeholder="obra"
          value={buildingId}
          onChange={(e) => setBuildingId(e.target.value)}
          className="border-2 border-gray-400 bg-white rounded px-2 py-1 text-xs font-semibold text-gray-800 w-16 placeholder-gray-500"
        />
      </div>

      {/* EMPRESA */}
      <div className="flex items-center gap-1">
        <span className="text-white font-bold text-xs uppercase">EMPRESA</span>
        <select
          value={companyId}
          onChange={(e) => {
            setCompanyId(e.target.value);
            const company = companies.find(c => String(c.id) === e.target.value);
            if (company) {
              setCompanyName(company.name || '');
            } else {
              setCompanyName('');
            }
          }}
          className="border-2 border-gray-400 bg-white rounded px-2 py-1 text-xs font-semibold text-gray-800 w-32"
        >
          <option value="">Todas</option>
          {companies.map(c => (
            <option key={c.id} value={String(c.id)}>
              {c.name}
            </option>
          ))}
        </select>
      </div>

      {/* OBRAS / CENTRO DE CUSTO */}
      <div className="flex items-center gap-1">
        <span className="text-white font-bold text-xs uppercase">OBRAS</span>
        <select
          value={buildingId}
          onChange={(e) => {
            setBuildingId(e.target.value);
          }}
          className="border-2 border-gray-400 bg-white rounded px-2 py-1 text-xs font-semibold text-gray-800 w-44"
        >
          <option value="">Todas as Obras</option>
          {buildings.map(b => (
            <option key={b.id} value={String(b.id)}>
              {b.id} - {b.name || b.description}
            </option>
          ))}
        </select>
      </div>

      {/* ANO (aparecer quando PERÍODO TOTAL) */}
      {period === 'allTime' && (
        <div className="flex items-center gap-1">
          <span className="text-white font-bold text-xs uppercase">ANO</span>
          <select
            value={year}
            onChange={(e) => setYear(parseInt(e.target.value))}
            className="border-2 border-gray-400 bg-white rounded px-2 py-1 text-xs font-semibold text-gray-800 w-16"
          >
            {years.map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
      )}

      {/* BOTÕES */}
      <div className="flex items-center gap-2 ml-auto">
        <button
          onClick={handleClear}
          className="bg-white text-gray-700 px-3 py-1 rounded font-bold text-xs hover:bg-gray-100 transition flex items-center gap-1 whitespace-nowrap"
        >
          <X size={14} />
          Limpar
        </button>
        <button
          onClick={handleFilter}
          className="bg-white text-green-700 px-4 py-1 rounded font-bold text-xs hover:bg-gray-100 transition whitespace-nowrap"
        >
          Filtrar
        </button>
      </div>
    </div>
  );
}
