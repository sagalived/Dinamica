import React from 'react';
import { motion } from 'motion/react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Bell, Printer, X } from 'lucide-react';
import { cn } from '../../lib/utils';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { Badge } from '../../components/ui/badge';
import { useSienge } from '../../contexts/SiengeContext';
import { safeFormat } from '../dashboard/logic';
import { translateStatusLabel, toMoney } from '../financeiro/logic';
import { Button } from '../../components/ui/button';
import { ResponsiveContainer, LineChart, Line } from 'recharts';
import { useState } from 'react';
import { format } from 'date-fns';
import { FilterBar, FilterState } from '../../components/FilterBar';
import { formatarMoedaBR } from '../utilitarios/formatacaoptbr';

export function ObrasAlerta() {
  const {
    orders,
    priceAlerts,
    allFinancialTitles: financialTitles,
    allReceivableTitles: receivableTitles,
    itemsDetailsMap,
    latestPricesMap,
    baselinePricesMap,
    alertSortConfig,
    setAlertSortConfig,
    globalItemHistory,
    users,
    creditorMap,
    buildings,
  } = useSienge();

  const [modalItemHistory, setModalItemHistory] = useState<any>(null);
  const [isPrinting, setIsPrinting] = useState(false);
  const [searchItem, setSearchItem] = useState('');
  const [expandedDetail, setExpandedDetail] = useState<any>(null);
  const [currentFilters, setCurrentFilters] = useState<FilterState | null>(null);

  const handleFilterApply = (filters: FilterState) => {
    setCurrentFilters(filters);
  };

  const handleFilterClear = () => {
    setCurrentFilters(null);
  };

  const resolveBuildingName = (id: any) => buildings.find((b: any) => String(b.id) === String(id))?.name || id;
  const resolveBuildingLabel = (title: any) => {
    const explicit = title?.buildingName || title?.enterpriseName;
    if (explicit) return explicit;
    const id = title?.buildingId ?? title?.buildingCode ?? title?.idObra ?? title?.codigoObra;
    return resolveBuildingName(id);
  };

  const pagamentosHoje = React.useMemo(() => {
    const today = parseInt(format(new Date(), 'yyyyMMdd'));
    return financialTitles.filter((t: any) => t.dueDateNumeric === today && t.status !== 'BAIXADO' && t.status !== 'PAGO' && t.status !== 'LIQUIDADO');
  }, [financialTitles]);

  const handlePrint = () => {
    setIsPrinting(true);
    setTimeout(() => {
      window.print();
      setIsPrinting(false);
    }, 500);
  };

  const toggleSort = (key: string) => {
    let direction = 'asc';
    if (alertSortConfig?.key === key && alertSortConfig.direction === 'asc') direction = 'desc';
    setAlertSortConfig({ key, direction });
  };

  const renderSortIcon = (key: string) => {
    if (alertSortConfig?.key !== key) return null;
    return alertSortConfig.direction === 'asc' ? ' ↑' : ' ↓';
  };

  return (
    <>
      <motion.div
        key="obras-alerta"
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        className="w-full space-y-6"
      >
        {/* Filtros */}
        <FilterBar onFilter={handleFilterApply} onClear={handleFilterClear} />
        <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
          <h3 className="text-xl sm:text-2xl font-black text-white flex items-center gap-3">
            <Bell className="text-green-500" size={24} />
            Variações de Preço
          </h3>
          <div className="flex items-center gap-3">
            <Badge className="bg-green-600 text-white font-black px-3 py-1 print:hidden text-xs">
              {priceAlerts.length} {priceAlerts.length === 1 ? 'ALERTA' : 'ALERTAS'}
            </Badge>
            {pagamentosHoje.length > 0 && (
              <Badge className="bg-emerald-600 animate-pulse font-black px-3 py-1 text-xs text-white">
                {pagamentosHoje.length} PAGAMENTO(S) HOJE
              </Badge>
            )}
            <Button
              onClick={handlePrint}
              className="bg-white text-black hover:bg-gray-200 font-black tracking-tight rounded-xl print:hidden text-sm h-9"
            >
              <Printer size={14} className="mr-2" />
              PDF
            </Button>
          </div>
        </div>

        {pagamentosHoje.length > 0 && (
          <div className="mb-8">
            <h4 className="text-emerald-500 font-bold uppercase tracking-widest text-sm mb-3">Pagamentos Efetuados Hoje</h4>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {pagamentosHoje.map((p: any) => (
                <Card key={p.id} className="bg-emerald-950/20 border-emerald-500/20 shadow-none">
                  <CardContent className="p-4">
                    <p className="text-emerald-400 text-xs font-bold uppercase mb-1">{p.creditorName}</p>
                    <h3 className="text-white font-black text-lg">{formatarMoedaBR(Number(p.amount || 0))}</h3>
                    <p className="text-gray-400 text-[10px] mt-1">{p.description} (Obra: {resolveBuildingLabel(p)})</p>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3 sm:gap-4">
          {priceAlerts.slice(0, 8).map((alert: any, idx: number) => (
            <Card
              key={idx}
              onClick={() => setModalItemHistory({ name: alert.item, history: alert.history?.filter((h: any) => h.date.substring(0, 10) === alert.newDate.substring(0, 10)) || [] })}
              className="cursor-pointer hover:border-green-500/50 hover:scale-[1.02] bg-gradient-to-br from-green-600/20 to-transparent border-green-500/20 shadow-none overflow-hidden transition-all"
            >
              <CardContent className="p-4 sm:p-5 pb-0 relative">
                <div className="flex flex-col mb-4">
                  <h4
                    className="text-white font-black uppercase text-xs sm:text-[13px] leading-tight w-full mb-3 pb-2 border-b border-white/5"
                    title={alert.item}
                  >
                    {alert.item}
                  </h4>

                  <div className="flex items-start gap-2 w-full justify-between">
                    <div className="flex flex-1 items-center gap-3 sm:gap-6">
                      <div className="flex flex-col flex-1">
                        <p className="text-gray-500 text-[9px] font-bold tracking-widest uppercase mb-1">Anterior</p>
                        <h3 className="text-xs sm:text-sm font-bold text-gray-400 decoration-red-500/30">
                          {formatarMoedaBR(Number(alert.oldPrice || 0))}
                        </h3>
                        <p className="text-[#666] text-[8px] font-bold mt-0.5">{safeFormat(alert.oldDate)}</p>
                      </div>

                      <div className="w-px h-8 bg-white/5 mx-1" />

                      <div className="flex flex-col flex-1">
                        <p className="text-green-500 text-[9px] font-bold tracking-widest uppercase mb-1">Atual</p>
                        <h3 className="text-sm sm:text-base font-black text-white">{formatarMoedaBR(Number(alert.newPrice || 0))}</h3>
                        <p className="text-green-600/50 text-[8px] font-bold mt-0.5">{safeFormat(alert.newDate)}</p>
                      </div>
                    </div>

                    <div className="shrink-0 flex items-center justify-center bg-green-500/10 border border-green-500/20 rounded-lg px-2 sm:px-3 py-1 sm:py-1.5 h-fit mt-3">
                      <span className="text-green-500 font-black text-[10px] sm:text-xs tracking-tighter">
                        +{alert.diff > 1000 ? '>1000' : alert.diff}%
                      </span>
                    </div>
                  </div>
                </div>

                {alert.history && alert.history.length > 0 && (
                  <div className="mt-4 h-[50px] sm:h-16 w-full opacity-60 hover:opacity-100 transition-opacity pointer-events-none text-green-500">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={alert.history}>
                        <Line
                          type="monotone"
                          dataKey="price"
                          stroke="currentColor"
                          strokeWidth={2}
                          dot={{ fill: 'currentColor', strokeWidth: 1, r: 2 }}
                          activeDot={{ r: 4 }}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </CardContent>
            </Card>
          ))}
        </div>

        {/* MODAL HISTÓRICO DE PREÇOS */}
        {modalItemHistory && (
          <div
            className="fixed inset-0 z-[999] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4"
            onClick={() => {
              setModalItemHistory(null);
              setExpandedDetail(null);
            }}
          >
            <div
              className="bg-[#111] border border-green-500/30 p-6 rounded-3xl max-w-5xl w-full flex flex-col shadow-2xl max-h-[90vh] overflow-y-auto"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex justify-between items-start mb-2">
                <div>
                  <h2 className="text-white text-xl md:text-2xl font-black uppercase tracking-widest leading-tight">
                    {modalItemHistory.name}
                  </h2>
                  <p className="text-green-400/80 text-xs font-bold uppercase tracking-widest mt-1">
                    Histórico de Preços • {modalItemHistory.history.length}{' '}
                    {modalItemHistory.history.length === 1 ? 'registro' : 'registros'} encontrado
                    {modalItemHistory.history.length !== 1 ? 's' : ''}
                  </p>
                </div>
                <button
                  onClick={() => {
                    setModalItemHistory(null);
                    setExpandedDetail(null);
                  }}
                  className="text-gray-500 hover:text-white bg-white/5 hover:bg-white/10 rounded-full p-2 ml-4 flex-shrink-0"
                >
                  <X size={20} />
                </button>
              </div>

              {modalItemHistory.history.length === 1 && (
                <div className="mb-4 px-4 py-3 bg-blue-900/20 border border-blue-500/30 rounded-xl flex items-start gap-3">
                  <span className="text-blue-400 text-lg flex-shrink-0">ℹ️</span>
                  <p className="text-blue-300 text-xs font-bold leading-relaxed">
                    Este produto foi comprado apenas <strong>1 vez</strong> no histórico disponível. A cotação no Sienge pode
                    ter incluído múltiplos fornecedores, mas os preços comparativos dos concorrentes não estão disponíveis via
                    API. O sistema exibe o histórico de compras realizadas.
                  </p>
                </div>
              )}

              <div
                className={`grid grid-cols-1 gap-4 ${
                  modalItemHistory.history.length >= 3
                    ? 'md:grid-cols-3'
                    : modalItemHistory.history.length === 2
                      ? 'md:grid-cols-2'
                      : 'md:grid-cols-1 max-w-sm mx-auto w-full'
                }`}
              >
                {modalItemHistory.history.length === 0 ? (
                  <div className="col-span-3 text-center text-gray-500 py-10 font-bold tracking-widest uppercase">
                    Nenhum histórico disponível para exibir
                  </div>
                ) : (
                  (() => {
                    const sorted = [...modalItemHistory.history].sort(
                      (a, b) => new Date(b.date).getTime() - new Date(a.date).getTime(),
                    );
                    const championIdx = sorted.length > 1 ? 0 : -1;
                    const championPrice = sorted[0]?.price;
                    const prevPrice = sorted[1]?.price;
                    const championIsExpensive = championPrice !== undefined && prevPrice !== undefined && championPrice > prevPrice;
                    return sorted.slice(0, 3).map((hist: any, i: number) => {
                      const isChampion = i === championIdx;
                      const priceColor = isChampion ? (championIsExpensive ? 'text-red-400' : 'text-green-400') : 'text-white';
                      const champBg = isChampion
                        ? championIsExpensive
                          ? 'bg-red-600/10 border-red-500/50 shadow-[0_0_30px_rgba(239,68,68,0.15)] scale-[1.02]'
                          : 'bg-emerald-600/10 border-emerald-500/50 shadow-[0_0_30px_rgba(16,185,129,0.15)] scale-[1.02]'
                        : 'bg-black/40 border-white/10';
                      const champTagBg = isChampion
                        ? championIsExpensive
                          ? 'bg-gradient-to-bl from-red-600 to-rose-500'
                          : 'bg-gradient-to-bl from-emerald-500 to-teal-500'
                        : '';
                      const buyerName = users.find((u: any) => String(u.id) === hist.buyerId)?.name || hist.buyerId || '—';
                      const supplierName = creditorMap[hist.creditorId || ''] || hist.creditorId || '—';
                      const labelText = sorted.length === 1 ? 'Última Compra' : `Compra ${i + 1} de ${sorted.length > 3 ? '3+' : sorted.length}`;
                      return (
                        <div key={i} className={cn('border rounded-2xl p-6 flex flex-col relative overflow-hidden transition-all', champBg)}>
                          {isChampion && (
                            <div
                              className={cn(
                                'absolute top-0 right-0 text-white font-black text-[10px] tracking-widest uppercase px-4 py-1.5 rounded-bl-xl shadow-lg',
                                champTagBg,
                              )}
                            >
                              {championIsExpensive ? '⚠ Alta' : '✓ Mais Recente'}
                            </div>
                          )}
                          <h3 className="text-gray-400 font-bold uppercase tracking-widest text-xs mb-4 text-center border-b border-white/10 pb-4">
                            {labelText}
                          </h3>
                          <div className="flex flex-col items-center justify-center flex-1 py-4">
                            <h4 className={cn('text-3xl font-black mb-1', priceColor)}>
                              {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(hist.price)}
                            </h4>
                            <p className="text-gray-400 text-sm font-bold mt-1">{supplierName}</p>
                            <p className="text-gray-500 text-xs font-bold uppercase tracking-widest mt-1">{safeFormat(hist.date)}</p>
                          </div>
                          <div className="mt-4 pt-4 border-t border-white/5 flex flex-col text-center gap-2">
                            <button
                              onClick={() => setExpandedDetail(expandedDetail === i ? null : i)}
                              className="text-[10px] text-green-400 hover:text-green-300 uppercase font-black tracking-widest py-1.5 rounded-md border border-green-500/20 bg-green-500/5 hover:bg-green-500/10 transition-all"
                            >
                              {expandedDetail === i ? '▲ Fechar Detalhes' : `▼ Ver Detalhes  #${hist.orderId || 'S/N'}`}
                            </button>
                            {expandedDetail === i && (
                              <div className="text-left space-y-2 bg-black/30 rounded-xl p-3 border border-white/5 mt-1">
                                <div className="flex flex-col">
                                  <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Comprador</span>
                                  <span className="text-xs text-white font-bold">{buyerName}</span>
                                </div>
                                <div className="flex flex-col">
                                  <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Fornecedor</span>
                                  <span className="text-xs text-white font-bold">{supplierName}</span>
                                </div>
                                <div className="flex flex-col">
                                  <span className="text-[9px] text-gray-500 uppercase font-black tracking-widest">Data / Hora</span>
                                  <span className="text-xs text-white font-bold">{safeFormat(hist.date, 'dd/MM/yyyy HH:mm')}</span>
                                </div>
                              </div>
                            )}
                          </div>
                        </div>
                      );
                    });
                  })()
                )}
              </div>
            </div>
          </div>
        )}

        <Card className="bg-[#161618] border-white/5 shadow-2xl mt-10">
          <CardHeader className="print:hidden">
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white">Relatório / Alertas de Itens</CardTitle>
          </CardHeader>
          <CardContent className="p-0 overflow-x-auto overflow-y-auto max-h-[500px] sm:max-h-[600px] custom-scrollbar print:overflow-visible print:max-h-none">
            <Table className="print:text-black relative">
              <TableHeader className="bg-black/80 sticky top-0 z-10 backdrop-blur-md print:bg-gray-100 print:relative border-b border-white/10">
                <TableRow className="border-none print:border-gray-200">
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Item e Código</TableHead>
                  <TableHead
                    className="text-[10px] font-black uppercase text-gray-500 print:text-black cursor-pointer group hover:text-white transition-colors"
                    onClick={() => toggleSort('date')}
                  >
                    <div className="flex items-center">Data {renderSortIcon('date')}</div>
                  </TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Status</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Solicitante</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Comprador</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 print:text-black">Prazos</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 text-center print:text-black">Qtd</TableHead>
                  <TableHead
                    className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors"
                    onClick={() => toggleSort('vlrUnit')}
                  >
                    <div className="flex items-center justify-end">Vlr Unit {renderSortIcon('vlrUnit')}</div>
                  </TableHead>
                  <TableHead
                    className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors"
                    onClick={() => toggleSort('vlrAtual')}
                  >
                    <div className="flex items-center justify-end">Vlr Atual {renderSortIcon('vlrAtual')}</div>
                  </TableHead>
                  <TableHead
                    className="text-[10px] font-black uppercase text-gray-500 text-right print:text-black cursor-pointer group hover:text-white transition-colors"
                    onClick={() => toggleSort('valorTotal')}
                  >
                    <div className="flex items-center justify-end">Valor Total {renderSortIcon('valorTotal')}</div>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {orders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={9} className="text-center py-10 text-gray-500 font-bold">
                      Nenhum pedido ou alerta registrado para o período e filtros selecionados.
                    </TableCell>
                  </TableRow>
                ) : (
                  (() => {
                    let flatItems: any[] = [];
                    orders.forEach((o: any) => {
                      const itemsList = itemsDetailsMap[o.id];
                      const comprador = o.buyerId || 'N/A';
                      const solicitante =
                        o.requesterId && o.requesterId !== '0'
                          ? String(o.requesterId).replace(/^Comprador\s+/i, '').trim()
                          : String(o.createdBy || 'N/A').replace(/^Comprador\s+/i, '').trim();

                      if (!itemsList || itemsList.length === 0) {
                        flatItems.push({
                          o,
                          idx: 0,
                          isFallback: true,
                          desc: `Cod. ${o.id} (Carregando...)`,
                          comprador,
                          solicitante,
                          qty: 0,
                          vlrBase: 0,
                          vlrAtual: 0,
                          totalAmount: o.totalAmount,
                          dateNumeric: o.dateNumeric || 0,
                        });
                        return;
                      }

                      itemsList.forEach((item: any, idx: number) => {
                        if (!item) return;
                        const qty = Number(item.quantity || item.quantidade || 1);
                        const realUnitValue = Number(item.netPrice || item.unitPrice || item.valorUnitario || 0);
                        const totalAmount = qty * realUnitValue;
                        const desc = item.resourceDescription || item.descricao || `Item ${idx + 1}`;
                        const vlrAtual = Number(latestPricesMap[desc]) || 0;
                        const vlrBase = Number(baselinePricesMap[desc]) || realUnitValue;

                        flatItems.push({
                          o,
                          idx,
                          isFallback: false,
                          desc,
                          comprador,
                          solicitante,
                          qty,
                          vlrBase,
                          vlrAtual,
                          totalAmount,
                          dateNumeric: o.dateNumeric || 0,
                        });
                      });
                    });

                    if (alertSortConfig) {
                      flatItems.sort((a, b) => {
                        let valA = 0,
                          valB = 0;
                        if (alertSortConfig.key === 'date') {
                          valA = a.dateNumeric;
                          valB = b.dateNumeric;
                        }
                        if (alertSortConfig.key === 'vlrUnit') {
                          valA = a.vlrBase;
                          valB = b.vlrBase;
                        }
                        if (alertSortConfig.key === 'vlrAtual') {
                          valA = a.vlrAtual;
                          valB = b.vlrAtual;
                        }
                        if (alertSortConfig.key === 'valorTotal') {
                          valA = a.totalAmount;
                          valB = b.totalAmount;
                        }

                        return alertSortConfig.direction === 'asc' ? valA - valB : valB - valA;
                      });
                    } else {
                      flatItems.sort((a, b) => b.dateNumeric - a.dateNumeric);
                    }

                    return flatItems.slice(0, isPrinting ? 999999 : 100).map((flat, i) => {
                      const { o, isFallback, desc, comprador, solicitante, qty, vlrBase, vlrAtual, totalAmount } = flat;
                      if (isFallback) {
                        return (
                          <TableRow key={`alert-${o.id}-fallback-${i}`} className="border-white/5 hover:bg-white/5 transition-colors">
                            <TableCell className="font-bold text-green-500 whitespace-nowrap">{desc}</TableCell>
                            <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                            <TableCell>
                              <Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">
                                {translateStatusLabel(o.status)}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{solicitante}</TableCell>
                            <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{comprador}</TableCell>
                            <TableCell className="text-xs text-gray-400">{o.paymentCondition || 'N/A'}</TableCell>
                            <TableCell className="text-xs font-mono text-gray-500 text-center">-</TableCell>
                            <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                            <TableCell className="text-xs text-gray-400 font-mono text-right">-</TableCell>
                            <TableCell className="text-right font-black text-white whitespace-nowrap">
                              R$ {totalAmount.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                            </TableCell>
                          </TableRow>
                        );
                      }

                      const isCheaper = vlrAtual > 0 && vlrAtual < vlrBase;
                      const isExpensive = vlrAtual > vlrBase;
                      const colorClass = isCheaper
                        ? 'text-green-500 font-black drop-shadow-[0_0_6px_rgba(34,197,94,0.75)]'
                        : isExpensive
                          ? 'text-red-500 font-black drop-shadow-[0_0_6px_rgba(239,68,68,0.75)]'
                          : 'text-gray-400';

                      return (
                        <TableRow
                          key={`alert-${o.id}-${flat.idx}-${i}`}
                          onClick={() => {
                            const history = globalItemHistory[desc]
                              ? globalItemHistory[desc].sort((a: any, b: any) => new Date(b.date).getTime() - new Date(a.date).getTime())
                              : [];
                            setModalItemHistory({ name: desc, history });
                          }}
                          className={cn(
                            'border-white/5 hover:bg-white/10 transition-colors border-l-2 cursor-pointer',
                            isCheaper &&
                              'border-l-green-500 shadow-[inset_4px_0_0_rgba(34,197,94,0.95)] bg-[linear-gradient(90deg,rgba(34,197,94,0.12),transparent_18%)]',
                            isExpensive &&
                              'border-l-red-500 shadow-[inset_4px_0_0_rgba(239,68,68,0.95)] bg-[linear-gradient(90deg,rgba(239,68,68,0.12),transparent_18%)]',
                          )}
                        >
                          <TableCell className="font-bold text-green-500" title={desc}>
                            <div className="max-w-[200px] truncate">{desc}</div>
                          </TableCell>
                          <TableCell className="text-xs text-gray-500">{safeFormat(o.date)}</TableCell>
                          <TableCell>
                            <Badge variant="outline" className="bg-white/5 text-gray-400 border-white/10 uppercase text-[9px]">
                              {translateStatusLabel(o.status)}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{solicitante}</TableCell>
                          <TableCell className="text-xs text-gray-400 max-w-[120px] truncate">{comprador}</TableCell>
                          <TableCell className="text-xs text-gray-400">{o.paymentCondition || 'N/A'}</TableCell>
                          <TableCell className="text-xs font-mono text-gray-500 text-center">{qty}</TableCell>
                          <TableCell className="text-xs text-gray-400 font-mono text-right" title="Valor Anterior da Data Inicial">
                            R$ {vlrBase.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </TableCell>
                          <TableCell className={`text-xs font-mono text-right ${colorClass}`}>
                            {vlrAtual > 0
                              ? `R$ ${vlrAtual.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                              : '-'}
                          </TableCell>
                          <TableCell className="text-right font-black text-white whitespace-nowrap">
                            R$ {totalAmount.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                          </TableCell>
                        </TableRow>
                      );
                    });
                  })()
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </motion.div>
    </>
  );
}
