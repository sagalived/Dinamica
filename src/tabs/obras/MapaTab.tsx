import React, { useState } from 'react';
import { useSienge } from '../../contexts/SiengeContext';
import { useAuth } from '../../contexts/AuthContext';
import { motion } from 'framer-motion';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../../components/ui/card';
import { Map as MapIcon, Search, User as UserIcon } from 'lucide-react';
import { cn } from '../../lib/utils';
import { sienge as api } from '../../lib/api';
import { FilterBar, FilterState } from '../../components/FilterBar';

export function MapaTab() {
  const { 
    buildings, orders, financialTitles,
    selectedMapBuilding, setSelectedMapBuilding, 
    buildingSearch, setBuildingSearch,
    editingEngineer, setEditingEngineer,
    engineerDraft, setEngineerDraft,
    savingEngineer, setSavingEngineer,
    setBuildings
  } = useSienge();

  // Estado dos filtros
  const [currentFilters, setCurrentFilters] = useState<FilterState | null>(null);

  const handleFilterApply = (filters: FilterState) => {
    setCurrentFilters(filters);
  };

  const handleFilterClear = () => {
    setCurrentFilters(null);
  };

  const { isAdmin } = useAuth();

  const buildingOptions = [...buildings].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <motion.div
      key="map"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="space-y-6"
    >
      {/* Filtros */}
      <FilterBar onFilter={handleFilterApply} onClear={handleFilterClear} />

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 sm:gap-6 min-h-[400px] sm:h-[600px]">
        {/* 1. Lista de Obras */}
        <Card className="lg:col-span-1 bg-[#161618] border-white/5 shadow-2xl flex flex-col h-full">
          <CardHeader className="pb-4">
            <CardTitle className="text-white font-black uppercase text-sm tracking-tight">Obras Ativas</CardTitle>
            <CardDescription className="text-xs">
              {(buildingOptions.filter(b => b.name.toLowerCase().includes(buildingSearch.toLowerCase()) || String(b.id).includes(buildingSearch)) || []).length} encontradas
            </CardDescription>
            <div className="mt-3 relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500" size={14} />
              <input 
              type="text" 
              placeholder="Pesquisar obra..." 
              className="w-full bg-black/40 border border-white/10 rounded-lg py-2 pl-9 pr-3 text-xs text-white placeholder:text-gray-600 focus:outline-none focus:border-orange-500/50"
              value={buildingSearch}
              onChange={(e) => setBuildingSearch(e.target.value)}
            />
          </div>
        </CardHeader>
        <CardContent className="flex-1 overflow-y-auto px-2 pb-4 space-y-1 custom-scrollbar">
          {(() => {
            const filtered = buildingOptions.filter(b =>
              b.name.toLowerCase().includes(buildingSearch.toLowerCase()) ||
              String(b.id).includes(buildingSearch)
            );
            // Compute volume per building from filtered orders for sorting
            const buildingVolume: Record<string, number> = {};
            orders.forEach((o: any) => {
              const key = String(o.buildingId);
              buildingVolume[key] = (buildingVolume[key] || 0) + (o.totalAmount || 0);
            });
            return filtered
              .sort((a, b) => (buildingVolume[String(b.id)] || 0) - (buildingVolume[String(a.id)] || 0))
              .map(b => {
                const vol = buildingVolume[String(b.id)] || 0;
                return (
                  <button
                    key={b.id}
                    onClick={() => { setSelectedMapBuilding(b.id); setEditingEngineer(false); }}
                    className={cn(
                      "w-full text-left p-3 rounded-xl transition-all border text-xs font-bold",
                      selectedMapBuilding === b.id
                        ? "bg-orange-600/20 border-orange-500/50 text-orange-500"
                        : "bg-black/20 border-white/5 text-gray-400 hover:bg-white/5 hover:text-white"
                    )}
                  >
                    <div className="truncate mb-1 text-sm">{b.name}</div>
                    <div className="flex items-center justify-between">
                      <div className="text-[9px] text-gray-500 uppercase flex items-center gap-1">
                        <MapIcon size={10} /> ID: {b.id}
                      </div>
                      {vol > 0 && (
                        <div className="text-[9px] font-black text-orange-500/70">
                          R$&nbsp;{vol.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}
                        </div>
                      )}
                    </div>
                  </button>
                );
            });
          })()}
        </CardContent>
      </Card>

      {/* 2. Mapa Google */}
      <Card className="lg:col-span-2 bg-[#161618] border-white/5 shadow-2xl relative overflow-hidden p-0 h-full">
        {(() => {
          const currentBuilding = buildings.find((b: any) => b.id === selectedMapBuilding);
          if (!currentBuilding) {
            return (
              <div className="flex flex-col items-center justify-center h-full text-gray-500 bg-[#0a0a0b]">
                <MapIcon size={48} className="mb-4 opacity-20" />
                <p className="font-bold text-sm">Selecione uma obra na lista para visualizar o mapa</p>
              </div>
            );
          }

          const query = currentBuilding.address || currentBuilding.name;

          return (
            <iframe 
              width="100%" 
              height="100%" 
              frameBorder="0" 
              scrolling="no" 
              marginHeight={0} 
              marginWidth={0} 
              src={`https://maps.google.com/maps?q=${encodeURIComponent(query)}&t=m&z=14&output=embed`}
              style={{ filter: "invert(90%) hue-rotate(180deg) brightness(80%) contrast(120%)" }}
              title="Google Maps"
            ></iframe>
          );
        })()}
      </Card>

      {/* 3. Resumo Financeiro */}
      <Card className="lg:col-span-1 bg-[#161618] border-white/5 shadow-2xl h-full overflow-y-auto">
        <CardHeader className="pb-4">
          <CardTitle className="text-white font-black uppercase text-sm tracking-tight leading-tight">
            {selectedMapBuilding
              ? buildings.find((b: any) => b.id === selectedMapBuilding)?.name
              : "Resumo da Obra"}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {selectedMapBuilding ? (
            <div className="space-y-5">
              {(() => {
                const currentBuilding = buildings.find((b: any) => b.id === selectedMapBuilding);
                // Use ALL orders (no date filter) for accurate building totals
                const buildingOrders = orders.filter((o: any) => String(o.buildingId) === String(selectedMapBuilding));
                // Financial titles sem vínculo de obra podem vir zerados; aqui somamos apenas os vinculados.
                const buildingPayable = financialTitles.filter((f: any) => {
                  if (String(f.buildingId) === String(selectedMapBuilding)) return true;
                  return false;
                });
                const openPayableBuilding = buildingPayable.filter(
                  (f: any) => f.status !== 'BAIXADO' && f.status !== 'PAGO' && f.status !== 'LIQUIDADO'
                );

                const totalOrders = buildingOrders.reduce((acc: number, curr: any) => acc + (curr.totalAmount || 0), 0);
                const totalPayable = openPayableBuilding.reduce((acc: number, curr: any) => acc + (curr.amount || 0), 0);

                const saveEngineer = async () => {
                  if (!currentBuilding) return;
                  setSavingEngineer(true);
                  try {
                    await api.post('/obras/meta', { id: currentBuilding.id, engineer: engineerDraft });
                    // Update local state
                    setBuildings((prev: any[]) => prev.map(b =>
                      b.id === currentBuilding.id ? { ...b, engineer: engineerDraft } : b
                    ));
                    setEditingEngineer(false);
                  } catch (e) {
                    console.error('Erro ao salvar engenheiro', e);
                  } finally {
                    setSavingEngineer(false);
                  }
                };

                return (
                  <>
                    {/* Responsável Técnico */}
                    <div className="bg-black/20 p-4 rounded-xl border border-white/5">
                      <div className="flex items-start gap-3">
                        <div className="w-9 h-9 rounded-full bg-orange-500/20 flex items-center justify-center shrink-0 mt-0.5">
                          <UserIcon size={16} className="text-orange-500" />
                        </div>
                        <div className="min-w-0 flex-1">
                          <p className="text-[10px] font-black uppercase text-gray-500 mb-1">Responsável Técnico</p>
                          {editingEngineer ? (
                            <div className="flex flex-col gap-2">
                              <input
                                autoFocus
                                value={engineerDraft}
                                onChange={e => setEngineerDraft(e.target.value)}
                                onKeyDown={e => { if(e.key === 'Enter') saveEngineer(); if(e.key === 'Escape') setEditingEngineer(false); }}
                                className="bg-black/60 border border-orange-500/40 rounded-lg px-3 py-1.5 text-sm text-white w-full focus:outline-none focus:border-orange-500"
                                placeholder="Nome do responsável"
                              />
                              <div className="flex gap-2">
                                <button
                                  onClick={saveEngineer}
                                  disabled={savingEngineer}
                                  className="flex-1 bg-orange-600 hover:bg-orange-700 text-white text-xs font-black py-1.5 rounded-lg transition-colors disabled:opacity-50"
                                >
                                  {savingEngineer ? 'Salvando...' : 'Salvar'}
                                </button>
                                <button
                                  onClick={() => setEditingEngineer(false)}
                                  className="flex-1 bg-white/5 hover:bg-white/10 text-gray-400 text-xs font-bold py-1.5 rounded-lg transition-colors"
                                >
                                  Cancelar
                                </button>
                              </div>
                            </div>
                          ) : (
                            <div className="flex items-center justify-between gap-2">
                              <p className="text-sm font-bold text-white leading-tight truncate">
                                {currentBuilding?.engineer || 'Não definido'}
                              </p>
                              {isAdmin && (
                                <button
                                  onClick={() => { setEngineerDraft(currentBuilding?.engineer || ''); setEditingEngineer(true); }}
                                  className="shrink-0 text-[9px] font-black uppercase text-orange-500/70 hover:text-orange-500 border border-orange-500/20 hover:border-orange-500/50 px-2 py-1 rounded-md transition-colors"
                                >
                                  Editar
                                </button>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="space-y-3">
                      <div className="flex items-center justify-between p-3 rounded-xl bg-orange-600/10 border border-orange-500/20">
                        <span className="text-xs font-bold text-orange-500">Volume de Compras</span>
                        <span className="text-sm font-black text-orange-400">
                          R$ {totalOrders.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                        </span>
                      </div>
                      
                      <div className="flex items-center justify-between p-3 rounded-xl bg-red-600/10 border border-red-500/20">
                        <span className="text-xs font-bold text-red-500">Títulos a Pagar (Aberto)</span>
                        <span className="text-sm font-black text-red-400">
                          R$ {totalPayable.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                        </span>
                      </div>
                    </div>
                  </>
                );
              })()}
            </div>
          ) : (
            <div className="text-center text-gray-500 py-10">
              <p className="text-xs">Selecione uma obra</p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
    </motion.div>
  );
}
