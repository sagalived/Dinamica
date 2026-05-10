import React, { useMemo, useState } from 'react';
import { motion } from 'framer-motion';
import { Label } from '../../components/ui/label';
import { Button } from '../../components/ui/button';
import { CalendarIcon, Filter, FileText, CheckCircle2, RefreshCw } from 'lucide-react';
import { Popover, PopoverTrigger, PopoverContent } from '../../components/ui/popover';
import { Calendar } from '../../components/ui/calendar';
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from '../../components/ui/select';
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from '../../components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { cn } from '../../lib/utils';
import { format, subMonths } from 'date-fns';
import { ptBR } from 'date-fns/locale';
import { useSienge } from '../../contexts/SiengeContext';
import { useTheme } from '../../contexts/ThemeContext';
import { calcularFluxoCaixa } from './leandroLogic';
import { safeFormat } from '../dashboard/logic';
import { formatarNumeroBR, formatarQuantidadeBR } from '../utilitarios/formatacaoptbr';
import { calcularResumoFluxoCaixa } from './fluxo-caixa/fluxocaixaresumo';

export function FinanceiroFluxoTab() {
  const {
    fcPeriodMode, setFcPeriodMode,
    fcStartDate, setFcStartDate,
    fcEndDate, setFcEndDate,
    fcSelectedCompany, setFcSelectedCompany,
    fcSelectedBuilding, setFcSelectedBuilding,
    fcHideInternal, setFcHideInternal,
    loading, companies, buildings,
    allFinancialTitles, allReceivableTitles,
    nfeDocuments,
    setDataRevision
  } = useSienge();
  
  const { isDark } = useTheme();



  const fcSelectedCompanyName = companies.find((c: any) => String(c.id) === fcSelectedCompany)?.name || '';
  const buildingMap = useMemo(() => {
    const map: Record<string, string> = {};
    buildings.forEach((b: any) => map[b.id] = b.name);
    return map;
  }, [buildings]);

  const fcBuildingOptions = useMemo(() => {
    if (fcSelectedCompany === 'all') return buildings;
    return buildings.filter((b: any) => String(b.companyId) === fcSelectedCompany);
  }, [buildings, fcSelectedCompany]);

  const defaultWindow = useMemo(() => {
    const end = new Date();
    const start = subMonths(end, 6);
    return { start, end };
  }, []);

  const { rows: fluxoDeCaixaData, saldoAnterior: fluxoDeCaixaSaldoAnterior } = useMemo(() => {
    const fcHasManualDate = Boolean(fcStartDate || fcEndDate);
    const fcEffectiveStart = fcHasManualDate
      ? (fcStartDate || null)
      : (fcPeriodMode === 'last6m' ? defaultWindow.start : null);
    const fcEffectiveEnd = fcHasManualDate
      ? (fcEndDate || fcStartDate || null)
      : (fcPeriodMode === 'last6m' ? defaultWindow.end : null);
    return calcularFluxoCaixa({
      allReceivableTitles,
      allFinancialTitles,
      buildings,
      fcSelectedCompany,
      fcSelectedBuilding,
      fcHideInternal,
      startNumeric: fcEffectiveStart ? parseInt(format(fcEffectiveStart, 'yyyyMMdd')) : null,
      endNumeric: fcEffectiveEnd ? parseInt(format(fcEffectiveEnd, 'yyyyMMdd')) : null,
    });
  }, [allFinancialTitles, allReceivableTitles, defaultWindow.end, defaultWindow.start, fcEndDate, fcHideInternal, fcPeriodMode, fcSelectedBuilding, fcSelectedCompany, fcStartDate, buildings]);

  const nfeSummary = useMemo(() => {
    const docs = Array.isArray(nfeDocuments) ? nfeDocuments : [];
    const total = docs.reduce((acc: number, doc: any) => {
      const value = Number(
        doc?.totalAmount ??
        doc?.totalInvoiceAmount ??
        doc?.amount ??
        doc?.value ??
        doc?.valor ??
        doc?.valorTotal ??
        0
      );
      return acc + (Number.isFinite(value) ? value : 0);
    }, 0);
    return { count: docs.length, total };
  }, [nfeDocuments]);

  const resumoFluxo = useMemo(() => {
    return calcularResumoFluxoCaixa({
      rows: fluxoDeCaixaData,
      saldoAnterior: fluxoDeCaixaSaldoAnterior,
    });
  }, [fluxoDeCaixaData, fluxoDeCaixaSaldoAnterior]);

  return (
    <motion.div
      key="financeiro-fluxo"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -20 }}
      className="space-y-6"
    >
      {/* Tabela do Fluxo de Caixa */}
      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[600px]">
        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
          <div className="flex flex-col">
            <CardTitle className="text-xl font-black uppercase text-white flex items-center gap-2">
              <FileText className="text-orange-500" size={20} /> Fluxo de Caixa (Extrato/Razão)
            </CardTitle>
            <CardDescription className="text-gray-400 text-xs mt-1">
              Cruzamento das Contas a Pagar e Receber projetando o saldo cumulativo
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
          <Table>
            <TableHeader className="bg-black/60 sticky top-0 z-10 backdrop-blur-md">
              <TableRow className="border-white/5 hover:bg-transparent">
                <TableHead className="text-[10px] font-black uppercase text-gray-500 w-24">Data</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 w-28">Tit/Parc</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 w-12">Orig.</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 w-32">Conta</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 w-28">Tp. Lanç.</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500">Cliente/Fornecedor</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-32">Entradas (R$)</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-32">Saídas (R$)</TableHead>
                <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right w-36">Saldo (R$)</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {fluxoDeCaixaData.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-10 text-gray-500 font-bold">
                    Nenhuma movimentação encontrada para o período e empresa selecionados. Use "Limpar Filtros" para exibir todo o extrato.
                  </TableCell>
                </TableRow>
              ) : (
                fluxoDeCaixaData.map((row: any, idx: number) => (
                  <TableRow
                    key={`fc-${idx}-${row.id}`}
                    className={cn(
                      "border-white/5 hover:bg-white/5 transition-colors",
                      row.entrada > 0 ? "border-l-2 border-l-emerald-600/30" : "border-l-2 border-l-red-600/20"
                    )}
                  >
                    <TableCell className="text-xs text-gray-400 whitespace-nowrap">
                      {safeFormat(row.data, 'dd/MM/yyyy')}
                    </TableCell>
                    <TableCell className="text-xs font-mono text-blue-300 whitespace-nowrap" title={row.statementType}>
                      {row.titParc || row.documento}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className={cn(
                        "text-[9px] uppercase border-white/10 px-1 py-0",
                        row.origem === 'CX' ? "text-yellow-400 bg-yellow-500/10" :
                        row.origem === 'BC' ? "text-sky-400 bg-sky-500/10" :
                        row.origem === 'GE' ? "text-purple-400 bg-purple-500/10" :
                        row.origem === 'CP' ? "text-orange-400 bg-orange-500/10" :
                        row.origem === 'AC' ? "text-pink-400 bg-pink-500/10" :
                        "text-gray-400 bg-white/5"
                      )}>
                        {row.origem || '-'}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-xs text-gray-300 whitespace-nowrap truncate max-w-[120px]" title={row.bankAccount}>
                      {row.bankAccount || '-'}
                    </TableCell>
                    <TableCell className="text-[10px] text-gray-400 whitespace-nowrap truncate max-w-[120px]" title={row.statementType}>
                      {row.statementType}
                    </TableCell>
                    <TableCell className="text-xs font-bold text-gray-200 truncate max-w-[220px]" title={row.pessoa}>
                      {row.pessoa}
                    </TableCell>
                    <TableCell className={cn(
                      'text-right font-mono whitespace-nowrap',
                      row.entrada > 0 ? 'text-emerald-400' :
                      row.entrada < 0 ? 'text-orange-400' : 'text-gray-600'
                    )}>
                      {row.entrada !== 0
                        ? (row.entrada < 0 ? '-' : '') + formatarNumeroBR(Math.abs(row.entrada))
                        : <span className="text-gray-600">-</span>}
                    </TableCell>
                    <TableCell className="text-right font-mono text-red-400 whitespace-nowrap">
                      {row.saida > 0
                        ? formatarNumeroBR(row.saida)
                        : <span className="text-gray-600">-</span>}
                    </TableCell>
                    <TableCell className={cn(
                      "text-right font-black font-mono whitespace-nowrap",
                      row.saldo >= 0 ? "text-emerald-400" : "text-red-400"
                    )}>
                      {row.saldo < 0 ? '-' : ''}{formatarNumeroBR(Math.abs(row.saldo))}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
        {/* Resumo Rodapé */}
        <div className="bg-black/40 border-t border-white/5 p-4 flex justify-between items-center text-sm">
           <div className="flex gap-6">
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Saldo Anterior</span>
                <span className={cn('font-mono font-black', fluxoDeCaixaSaldoAnterior >= 0 ? 'text-sky-400' : 'text-orange-400')}>
                  {fluxoDeCaixaSaldoAnterior < 0 ? '-' : ''}R$ {formatarNumeroBR(Math.abs(fluxoDeCaixaSaldoAnterior))}
                </span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Total Entradas</span>
                <span className="font-mono text-emerald-400 font-black">R$ {formatarNumeroBR(resumoFluxo.totalEntradas)}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Total Saídas</span>
                <span className="font-mono text-red-400 font-black">R$ {formatarNumeroBR(resumoFluxo.totalSaidas)}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Registros</span>
                <span className="font-mono text-gray-300 font-black">{formatarQuantidadeBR(resumoFluxo.totalRegistros)}</span>
              </div>
              <div className="flex flex-col">
                <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">NF Emitidas</span>
                <span className="font-mono text-sky-300 font-black">
                  {formatarQuantidadeBR(nfeSummary.count)} / R$ {formatarNumeroBR(nfeSummary.total)}
                </span>
              </div>
           </div>
           <div className="flex flex-col text-right">
              <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest">Saldo Acumulado do Período</span>
              <span className={cn('text-xl font-mono font-black', resumoFluxo.saldoFinal >= 0 ? 'text-emerald-500' : 'text-red-500')}>
                {resumoFluxo.saldoFinal < 0 ? '-' : ''}R$ {formatarNumeroBR(Math.abs(resumoFluxo.saldoFinal))}
              </span>
           </div>
        </div>
      </Card>
    </motion.div>
  );
}
