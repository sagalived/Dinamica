import React from 'react';
import { motion } from 'motion/react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../components/ui/card';
import { TrendingUp, ArrowDownToLine, ArrowUpFromLine, CheckCircle2, Clock, Landmark, DollarSign, ListOrdered, FileText, AlertTriangle, X } from 'lucide-react';
import { cn } from '../../lib/utils';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { useSienge } from '../../contexts/SiengeContext';
import { safeFormat } from '../dashboard/logic';
import { isSettledFinancialStatus, translateStatusLabel, toMoney } from './logic';
import { useMemo, useState } from 'react';

export function FinanceiroValores() {
  const { financialTitles, receivableTitles, bankBalance: saldoBancario, orders } = useSienge();
  
  const [financeLimit, setFinanceLimit] = useState<number>(50);
  const [reportType, setReportType] = useState<string>('compras');
  const [selectedOverdueTitle, setSelectedOverdueTitle] = useState<any>(null);

  const todayStart = useMemo(() => {
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  }, []);

  const isOverdue = (title: any) => {
    const due = Number(title?.dueDateNumeric || 0);
    return due > 0 && due < todayStart && !isSettledFinancialStatus(title?.status);
  };

  const formatMoney = (value: any) => (
    Number(value || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  );

  const getPersonName = (title: any, type: 'payable' | 'receivable') => {
    if (type === 'payable') {
      return title?.creditorName || title?.nomeCredor || title?.supplierName || 'Credor sem nome';
    }
    return title?.clientName || title?.customerName || title?.nomeCliente || title?.creditorName || 'Cliente sem nome';
  };

  const stats = useMemo(() => {
    const ordersArray = Array.isArray(orders) ? orders : [];
    const total = ordersArray.reduce((acc, curr) => acc + toMoney(curr.totalAmount), 0);
    const avg = ordersArray.length > 0 ? total / ordersArray.length : 0;
    
    const fTotal = financialTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    const rTotal = receivableTitles.reduce((acc, curr) => acc + toMoney(curr.amount), 0);
    const balance = rTotal - fTotal;

    return { total, avg, fTotal, rTotal, balance };
  }, [orders, financialTitles, receivableTitles]);

  const dreStats = useMemo(() => {
    const rol = stats.rTotal;
    const receitaBruta = rol / 0.8836;
    const deducoes = receitaBruta - rol;
    const despesasTotais = stats.fTotal;
    
    const maoDeObra = despesasTotais * 0.215;
    const materiais = despesasTotais * 0.557;
    const servicos = despesasTotais * 0.105;
    const cspTotal = maoDeObra + materiais + servicos;
    
    const despGerais = despesasTotais * 0.051;
    const despTributarias = despesasTotais * 0.003;
    const preLabore = despesasTotais * 0.046;
    const despOperacionaisTotal = despGerais + despTributarias + preLabore;
    
    const despFinanceiras = despesasTotais * 0.022;
    const irCsll = despesasTotais * 0.001;

    const resultadoBruto = rol - cspTotal;
    const resultadoOperacional = resultadoBruto - despOperacionaisTotal - despFinanceiras;
    const resultadoLiquido = resultadoOperacional - irCsll;

    return {
      receitaBruta, deducoes, rol,
      custos: { maoDeObra, materiais, servicos, total: cspTotal },
      resultadoBruto,
      despesas: { gerais: despGerais, tributarias: despTributarias, preLabore, total: despOperacionaisTotal },
      despFinanceiras, irCsll, resultadoLiquido
    };
  }, [stats]);


  return (
    <>
            <motion.div
              key="finance"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="space-y-8"
            >
              {(() => {
                const openPayables = financialTitles.filter(t => !isSettledFinancialStatus(t.status)).sort((a,b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));
                const openReceivables = receivableTitles.filter(t => !isSettledFinancialStatus(t.status)).sort((a,b) => (a.dueDateNumeric || 0) - (b.dueDateNumeric || 0));
                const paidPayables = financialTitles.filter(t => isSettledFinancialStatus(t.status)).sort((a,b) => (b.paymentDateNumeric || b.dueDateNumeric || 0) - (a.paymentDateNumeric || a.dueDateNumeric || 0));
                const overdueReceivables = openReceivables.filter(isOverdue);
                const overduePayables = openPayables.filter(isOverdue);

                const totalPayable = openPayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                const totalReceivable = openReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                const overdueReceivableTotal = overdueReceivables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                const overduePayableTotal = overduePayables.reduce((acc, curr) => acc + (curr.amount || 0), 0);
                return (
                  <>
                    {/* Demonstração de Resultados (DRE) Projetada */}
                    <Card className="bg-[#161618] border-white/5 shadow-2xl mb-6">
                      <CardHeader className="border-b border-white/5 bg-black/20 pb-4">
                        <div className="flex items-center justify-between">
                          <CardTitle className="text-xl font-black uppercase tracking-tight text-white flex items-center gap-2">
                            <TrendingUp className="text-orange-500" size={24} />
                            Demonstrativo de Resultado (DRE Projetado Sienge)
                          </CardTitle>
                          <span className="text-xs font-bold bg-orange-600/20 text-orange-500 px-3 py-1 rounded-full border border-orange-500/20">
                            Dinâmico
                          </span>
                        </div>
                        <CardDescription className="text-gray-400 mt-2">
                          Cálculo analítico baseado nos títulos financeiros recebidos e pagos, utilizando matriz de proporção do Sienge.
                        </CardDescription>
                      </CardHeader>
                      <CardContent className="pt-6">
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                          {/* Receitas */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-green-500 uppercase tracking-wider mb-4 border-b border-green-500/20 pb-2">1. Receitas</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-300">Receita Operacional Bruta</span>
                              <span className="font-mono text-white">R$ {dreStats.receitaBruta.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Deduções e Impostos</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.deducoes.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center font-bold text-green-400 bg-green-500/10 p-2 rounded">
                              <span>(=) Receita Líquida (ROL)</span>
                              <span className="font-mono">R$ {dreStats.rol.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>

                          {/* Custos */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-red-500 uppercase tracking-wider mb-4 border-b border-red-500/20 pb-2">2. Custos (CSP)</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Mão-de-Obra</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.maoDeObra.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Materiais e Insumos</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.materiais.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Serviços</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.custos.servicos.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center font-bold text-red-400 bg-red-500/10 p-2 rounded">
                              <span>Total CSP</span>
                              <span className="font-mono">-R$ {dreStats.custos.total.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>

                          {/* Despesas e Resultado */}
                          <div className="space-y-4">
                            <h4 className="text-sm font-black text-orange-500 uppercase tracking-wider mb-4 border-b border-orange-500/20 pb-2">3. Despesas e Resultado</h4>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Despesas Gerais/Adm</span>
                              <span className="font-mono text-red-400">-R$ {dreStats.despesas.gerais.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Pré-Labore & Trib.</span>
                              <span className="font-mono text-red-400">-R$ {(dreStats.despesas.preLabore + dreStats.despesas.tributarias).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                              <span className="text-gray-400">(-) Desp. Financeiras + IR/CSLL</span>
                              <span className="font-mono text-red-400">-R$ {(dreStats.despFinanceiras + dreStats.irCsll).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                            <div className={cn("flex justify-between items-center font-black p-3 rounded text-lg", dreStats.resultadoLiquido >= 0 ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400")}>
                              <span>(=) RESULTADO LÍQUIDO</span>
                              <span className="font-mono">R$ {dreStats.resultadoLiquido.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                          </div>
                        </div>
                      </CardContent>
                    </Card>

                    <div className="flex items-center justify-end gap-4 bg-[#161618] border border-white/5 p-4 rounded-xl shadow-2xl mb-2">
                       <div className="ml-auto flex items-end gap-6">
                           <div className="flex flex-col text-right">
                              <span className="text-[9px] text-gray-400 font-bold uppercase tracking-widest">Saldo Bancário (Sienge API)</span>
                              {saldoBancario !== null ? (
                                <span className={cn("text-xl font-black", saldoBancario >= 0 ? "text-green-500" : "text-red-500")}>
                                  {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(saldoBancario)}
                                </span>
                              ) : (
                                <span className="text-xl font-black text-emerald-500 opacity-60">Em Sincronização...</span>
                              )}
                           </div>
                       </div>
                    </div>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 sm:gap-6">
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Total a Pagar</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            R$ {totalPayable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                          <button onClick={() => setReportType('pagar')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Total a Receber</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            R$ {totalReceivable.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                          <button onClick={() => setReportType('receber')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl">
                        <CardHeader>
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Saldo Previsto</CardDescription>
                          <CardTitle className={cn("text-2xl font-black", (totalReceivable - totalPayable) >= 0 ? "text-green-500" : "text-red-500")}>
                            R$ {(totalReceivable - totalPayable).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                          </CardTitle>
                        </CardHeader>
                      </Card>
                      <Card className="bg-[#161618] border-white/5 shadow-2xl relative group">
                        <CardHeader className="pt-4 pr-16">
                          <CardDescription className="text-[10px] font-black uppercase text-orange-500">Títulos em Aberto</CardDescription>
                          <CardTitle className="text-2xl font-black text-white">
                            {openPayables.length + openReceivables.length}
                          </CardTitle>
                          <button onClick={() => setReportType('abertos')} className="absolute top-4 right-4 bg-white/10 hover:bg-white/20 text-white rounded px-2 py-1 flex items-center gap-1 text-[9px] font-bold uppercase transition-colors"><FileText size={10}/> Relatório</button>
                        </CardHeader>
                      </Card>
                    </div>

                    {(overdueReceivables.length > 0 || overduePayables.length > 0) && (
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        {overdueReceivables.length > 0 && (
                          <Card className="bg-red-950/20 border-red-500/20 shadow-none">
                            <CardHeader className="p-4 pb-2">
                              <CardTitle className="text-sm font-black uppercase tracking-tight text-red-300 flex items-center gap-2">
                                <AlertTriangle size={16} /> Receber em atraso
                              </CardTitle>
                              <CardDescription className="text-[11px] text-red-200/70">
                                {overdueReceivables.length} título(s) • R$ {formatMoney(overdueReceivableTotal)}
                              </CardDescription>
                            </CardHeader>
                            <CardContent className="px-4 pb-4 pt-1 grid grid-cols-1 sm:grid-cols-2 gap-2">
                              {overdueReceivables.slice(0, 4).map((title: any, idx: number) => (
                                <button
                                  key={`overdue-rec-${title.id || idx}`}
                                  type="button"
                                  onClick={() => setSelectedOverdueTitle({ ...title, alertType: 'receivable' })}
                                  className="text-left rounded-lg border border-red-500/20 bg-black/20 p-3 hover:bg-red-500/10 transition-colors"
                                >
                                  <p className="text-[10px] font-black uppercase text-red-300 truncate">{getPersonName(title, 'receivable')}</p>
                                  <div className="mt-1 flex items-center justify-between gap-2">
                                    <span className="text-xs text-gray-400">{safeFormat(title.dueDate, 'dd/MM/yy')}</span>
                                    <span className="text-xs font-black text-white whitespace-nowrap">R$ {formatMoney(title.amount)}</span>
                                  </div>
                                </button>
                              ))}
                            </CardContent>
                          </Card>
                        )}

                        {overduePayables.length > 0 && (
                          <Card className="bg-orange-950/20 border-orange-500/20 shadow-none">
                            <CardHeader className="p-4 pb-2">
                              <CardTitle className="text-sm font-black uppercase tracking-tight text-orange-300 flex items-center gap-2">
                                <AlertTriangle size={16} /> Pagar em atraso
                              </CardTitle>
                              <CardDescription className="text-[11px] text-orange-200/70">
                                {overduePayables.length} título(s) • R$ {formatMoney(overduePayableTotal)}
                              </CardDescription>
                            </CardHeader>
                            <CardContent className="px-4 pb-4 pt-1 grid grid-cols-1 sm:grid-cols-2 gap-2">
                              {overduePayables.slice(0, 4).map((title: any, idx: number) => (
                                <button
                                  key={`overdue-pay-${title.id || idx}`}
                                  type="button"
                                  onClick={() => setSelectedOverdueTitle({ ...title, alertType: 'payable' })}
                                  className="text-left rounded-lg border border-orange-500/20 bg-black/20 p-3 hover:bg-orange-500/10 transition-colors"
                                >
                                  <p className="text-[10px] font-black uppercase text-orange-300 truncate">{getPersonName(title, 'payable')}</p>
                                  <div className="mt-1 flex items-center justify-between gap-2">
                                    <span className="text-xs text-gray-400">{safeFormat(title.dueDate, 'dd/MM/yy')}</span>
                                    <span className="text-xs font-black text-white whitespace-nowrap">R$ {formatMoney(title.amount)}</span>
                                  </div>
                                </button>
                              ))}
                            </CardContent>
                          </Card>
                        )}
                      </div>
                    )}

                    <div className="flex flex-col gap-6 sm:gap-8">
                      {/* CONTAS A RECEBER */}
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
                          <CardTitle className="text-lg font-black uppercase text-emerald-500">1. Contas a Receber</CardTitle>
                          <button onClick={() => setReportType('receber')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Cliente e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Previsto</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {openReceivables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={4} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhum título a receber pendente.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                openReceivables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `rec-${idx}`} className={cn("border-white/5 hover:bg-white/5 border-l-2", isOverdue(title) ? "border-l-red-500/70 bg-red-500/5" : "border-l-emerald-500/50")}>
                                    <TableCell className="text-xs font-mono text-gray-500">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-emerald-400 truncate max-w-[200px]" title={title.creditorName || title.customerName}>
                                        {getPersonName(title, 'receivable')}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-400">
                                      {safeFormat(title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-white whitespace-nowrap">
                                      R$ {formatMoney(title.amount)}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {openReceivables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-white hover:bg-white/10">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* CONTAS A PAGAR */}
                      <Card className="bg-[#161618] border-white/5 shadow-2xl flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-white/5">
                          <CardTitle className="text-lg font-black uppercase text-orange-500">2. Contas a Pagar</CardTitle>
                          <button onClick={() => setReportType('pagar')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Credor e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Vencimento</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {openPayables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={4} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhum título a pagar pendente neste período.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                openPayables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `pay-${idx}`} className={cn("border-white/5 hover:bg-white/5 border-l-2", isOverdue(title) ? "border-l-red-500/70 bg-red-500/5" : "border-l-orange-500/50")}>
                                    <TableCell className="text-xs font-mono text-gray-500">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-gray-300 truncate max-w-[200px]" title={title.creditorName}>
                                        {title.creditorName}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-400">
                                      {safeFormat(title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-orange-500 whitespace-nowrap">
                                      R$ {formatMoney(title.amount)}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {openPayables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-white hover:bg-white/10">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* CONTAS PAGAS / BAIXADAS */}
                      <Card className="bg-[#111] border-white/5 shadow-inner flex flex-col h-[400px]">
                        <CardHeader className="pb-4 flex flex-row items-center justify-between border-b border-emerald-900/50">
                          <CardTitle className="text-lg font-black uppercase text-gray-400">3. Contas Pagas (Baixadas)</CardTitle>
                          <button onClick={() => setReportType('pagar')} className="bg-white/5 hover:bg-white/10 text-white rounded-md px-3 py-1.5 flex items-center gap-2 text-xs font-bold uppercase transition-colors"><FileText size={14}/> Gerar Relatório</button>
                        </CardHeader>
                        <CardContent className="p-0 flex-1 overflow-auto custom-scrollbar">
                          <Table>
                            <TableHeader className="bg-black/40 sticky top-0 z-10 backdrop-blur-md">
                              <TableRow className="border-white/5 hover:bg-transparent">
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-16">ID</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Credor e Título</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500">Banco Pago</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 w-24">Pago Em</TableHead>
                                <TableHead className="text-[9px] font-black uppercase text-gray-500 text-right">Valor Pago</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {paidPayables.length === 0 ? (
                                <TableRow>
                                  <TableCell colSpan={5} className="text-center py-10 text-gray-500 font-bold">
                                    Nenhuma conta paga localizada.
                                  </TableCell>
                                </TableRow>
                              ) : (
                                paidPayables.slice(0, financeLimit).map((title, idx) => (
                                  <TableRow key={title.id || `paid-${idx}`} className="border-white/5 hover:bg-white/5 border-l-2 border-l-gray-600/50 opacity-70 hover:opacity-100">
                                    <TableCell className="text-xs font-mono text-gray-500 line-through">{title.id}</TableCell>
                                    <TableCell>
                                      <p className="font-bold text-gray-400 truncate max-w-[200px]" title={title.creditorName}>
                                        {title.creditorName}
                                      </p>
                                      <p className="text-[9px] text-gray-500 truncate max-w-[200px]">{title.description}</p>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-500">
                                       <span className="bg-white/5 px-2 py-0.5 rounded uppercase text-[9px] font-bold">Sistema / Caixa</span>
                                    </TableCell>
                                    <TableCell className="text-xs text-gray-500">
                                      {safeFormat(title.paymentDate || title.dueDate, 'dd/MM/yy')}
                                    </TableCell>
                                    <TableCell className="text-right font-black text-gray-300 whitespace-nowrap">
                                      R$ {(title.amount || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                                    </TableCell>
                                  </TableRow>
                                ))
                              )}
                            </TableBody>
                          </Table>
                          {paidPayables.length > financeLimit && (
                            <div className="p-4 flex justify-center">
                              <Button variant="outline" onClick={() => setFinanceLimit(prev => prev + 100)} className="text-xs bg-white/5 border-white/10 text-gray-300 hover:bg-white/10 hover:text-white">Carregar mais</Button>
                            </div>
                          )}
                        </CardContent>
                      </Card>
                    </div>

                  </>
                );
              })()}
            </motion.div>

            {selectedOverdueTitle && (
              <div
                className="fixed inset-0 z-[999] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4"
                onClick={() => setSelectedOverdueTitle(null)}
              >
                <div
                  className="bg-[#111] border border-white/10 rounded-2xl shadow-2xl w-full max-w-2xl overflow-hidden"
                  onClick={(event) => event.stopPropagation()}
                >
                  <div className="p-5 border-b border-white/10 flex items-start justify-between gap-4">
                    <div>
                      <p className={cn(
                        "text-[10px] font-black uppercase tracking-widest mb-1",
                        selectedOverdueTitle.alertType === 'payable' ? 'text-orange-400' : 'text-red-300'
                      )}>
                        {selectedOverdueTitle.alertType === 'payable' ? 'Conta a pagar em atraso' : 'Conta a receber em atraso'}
                      </p>
                      <h2 className="text-xl font-black text-white uppercase leading-tight">
                        {getPersonName(selectedOverdueTitle, selectedOverdueTitle.alertType === 'payable' ? 'payable' : 'receivable')}
                      </h2>
                    </div>
                    <button
                      type="button"
                      onClick={() => setSelectedOverdueTitle(null)}
                      className="shrink-0 rounded-full p-2 bg-white/5 text-gray-400 hover:text-white hover:bg-white/10 transition-colors"
                    >
                      <X size={18} />
                    </button>
                  </div>

                  <div className="p-5 grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
                    <div className="rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Valor</span>
                      <strong className="text-2xl font-black text-white">R$ {formatMoney(selectedOverdueTitle.amount)}</strong>
                    </div>
                    <div className="rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Vencimento</span>
                      <strong className="text-lg font-black text-red-300">{safeFormat(selectedOverdueTitle.dueDate, 'dd/MM/yyyy')}</strong>
                    </div>
                    <div className="rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Título</span>
                      <strong className="text-sm font-mono text-gray-200 break-all">{selectedOverdueTitle.id || selectedOverdueTitle.documentNumber || 'Sem número'}</strong>
                    </div>
                    <div className="rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Status</span>
                      <Badge variant="outline" className="border-red-500/30 bg-red-500/10 text-red-300">
                        {translateStatusLabel(selectedOverdueTitle.status)}
                      </Badge>
                    </div>
                    <div className="sm:col-span-2 rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Descrição</span>
                      <p className="text-gray-200 leading-relaxed">{selectedOverdueTitle.description || selectedOverdueTitle.documentNumber || 'Sem descrição disponível.'}</p>
                    </div>
                    <div className="sm:col-span-2 rounded-xl bg-black/30 border border-white/5 p-4">
                      <span className="block text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1">Obra</span>
                      <p className="text-gray-200">{selectedOverdueTitle.buildingName || selectedOverdueTitle.buildingId || selectedOverdueTitle.buildingCode || 'Sem obra vinculada'}</p>
                    </div>
                  </div>
                </div>
              </div>
            )}

    </>
  );
}
