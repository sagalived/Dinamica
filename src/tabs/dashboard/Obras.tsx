import React, { useState } from 'react';
import { motion } from 'motion/react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import {
  ResponsiveContainer, CartesianGrid, XAxis, YAxis, Tooltip, Legend,
  PieChart, Pie, Cell, BarChart, Bar
} from 'recharts';
import { useSienge } from '../../contexts/SiengeContext';
import { FilterBar, FilterState } from '../../components/FilterBar';
import { formatarMoedaBR } from '../utilitarios/formatacaoptbr';

import type { DashboardObrasProps } from './types';

export function DashboardObras({ buildingCostData }: DashboardObrasProps) {
  const [currentFilters, setCurrentFilters] = useState<FilterState | null>(null);

  const handleFilterApply = (filters: FilterState) => {
    setCurrentFilters(filters);
  };

  const handleFilterClear = () => {
    setCurrentFilters(null);
  };

  return (
    <motion.div key="db-obras" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.95 }} className="space-y-8">
      {/* Filtros */}
      <FilterBar onFilter={handleFilterApply} onClear={handleFilterClear} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Custo por Obra (Top 7)</CardTitle></CardHeader>
          <CardContent className="h-[300px] sm:h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={buildingCostData} layout="vertical" margin={{ left: 50 }}>
                <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#ffffff05" />
                <XAxis type="number" hide />
                <YAxis dataKey="name" type="category" axisLine={false} tickLine={false} tick={{fill: '#fff', fontSize: 10}} width={120} />
                <Tooltip cursor={{fill: '#ffffff05'}} formatter={(value: number) => formatarMoedaBR(Number(value))} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                <Bar dataKey="gasto" name="Gasto Total" fill="#f97316" radius={[0, 4, 4, 0]} barSize={20} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Concentração de Gasto</CardTitle></CardHeader>
          <CardContent className="h-[300px] sm:h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={buildingCostData} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={2} dataKey="gasto">
                   {buildingCostData.map((e, index) => <Cell key={index} fill={['#f97316', '#3b82f6', '#10b981', '#ef4444', '#8b5cf6', '#6366f1', '#14b8a6'][index % 7]} />)}
                </Pie>
                <Tooltip formatter={(value: number) => formatarMoedaBR(Number(value))} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>
    </motion.div>
  );
}
