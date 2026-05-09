import React, { useState } from 'react';
import { motion } from 'motion/react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import {
  ResponsiveContainer, CartesianGrid, XAxis, YAxis, Tooltip, Legend,
  PieChart, Pie, Cell, BarChart, Bar
} from 'recharts';
import { useSienge } from '../../contexts/SiengeContext';
import { FilterBar, FilterState } from '../../components/FilterBar';

import type { DashboardLogisticaProps } from './types';

export function DashboardLogistica({ supplierData, paymentMethodData }: DashboardLogisticaProps) {
  const [currentFilters, setCurrentFilters] = useState<FilterState | null>(null);

  const handleFilterApply = (filters: FilterState) => {
    setCurrentFilters(filters);
  };

  const handleFilterClear = () => {
    setCurrentFilters(null);
  };

  return (
    <motion.div key="db-log" initial={{ opacity: 0, scale: 0.95 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.95 }} className="space-y-8">
      {/* Filtros */}
      <FilterBar onFilter={handleFilterApply} onClear={handleFilterClear} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-8">
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Top Fornecedores (Valores)</CardTitle></CardHeader>
          <CardContent className="h-[300px] sm:h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={supplierData} margin={{ bottom: 20 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ffffff05" />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 10}} angle={-45} textAnchor="end" />
                <YAxis axisLine={false} tickLine={false} tick={{fill: '#666', fontSize: 12}} tickFormatter={(v) => `R$${v/1000}k`} />
                <Tooltip cursor={{fill: '#ffffff05'}} formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                <Bar dataKey="value" name="Volume Comprado" fill="#3b82f6" radius={[4, 4, 0, 0]} maxBarSize={60} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
        <Card className="bg-[#161618] border-white/5 shadow-2xl">
          <CardHeader><CardTitle className="text-lg font-black uppercase tracking-tight text-white">Formas de Pagamento Aprovadas</CardTitle></CardHeader>
          <CardContent className="h-[300px] sm:h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={paymentMethodData} cx="50%" cy="50%" innerRadius={0} outerRadius={110} dataKey="value">
                   {paymentMethodData.map((e, index) => <Cell key={index} fill={['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ef4444'][index % 5]} />)}
                </Pie>
                <Tooltip formatter={(value: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)} contentStyle={{ backgroundColor: '#161618', border: 'none' }} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>
    </motion.div>
  );
}
