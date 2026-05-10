import React, { useMemo, useState, useEffect } from 'react';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useSienge } from '../contexts/SiengeContext';
import { useAuth } from '../contexts/AuthContext';
import { useTheme } from '../contexts/ThemeContext';
import { NavigationMenu } from './NavigationMenu';
import { cn } from '../lib/utils';
import { SlidersHorizontal, ChevronDown, RefreshCw, Sun, Moon, LogOut, User as UserIcon, Download, Calendar as CalendarIcon, Truck, LayoutDashboard, DollarSign, Bell, Map as MapIcon, X, Search } from 'lucide-react';
import { Label } from '@/components/ui/label';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { format, subMonths } from 'date-fns';
import { Button } from './ui/button';
import logoWordmark from '../assets/dinamica-wordmark.svg';
import logoWordmarkDark from '../assets/dinamica-wordmark-dark.svg';
import ModalComponent from '@/components/Modal';
import {
  filtrarListaPorTexto,
  MAPA_ROTAS_POR_ABA,
  obterAbaAtivaPorRota,
  obterOpcoesObraPorEmpresa,
} from './layout-principal/layoutprincipalhelpers';

export function LayoutPrincipal() {
  const { isDark, toggleThemeMode, themeMode } = useTheme();
  const { sessionUser, logout, isRestrictedUser } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const path = location.pathname;
  const isDashboardGeral = path === '/';
  const [syncDetailsOpen, setSyncDetailsOpen] = useState(false);

  const {
    syncSienge, syncing, syncProgress, apiStatus,
    syncInfo,
    applyFilters,
    globalPeriodMode, setGlobalPeriodMode,
    startDate, setStartDate, endDate, setEndDate,
    selectedCompany, setSelectedCompany,
    selectedUser, setSelectedUser,
    selectedRequester, setSelectedRequester,
    fcSelectedBuilding, setFcSelectedBuilding,
    companies, users, requesters, buildings
  } = useSienge();

  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [buildingIdInput, setBuildingIdInput] = useState('');
  const [allTimeYear, setAllTimeYear] = useState(new Date().getFullYear());
  const [isCompanyModalOpen, setIsCompanyModalOpen] = useState(false);
  const [isBuildingModalOpen, setIsBuildingModalOpen] = useState(false);
  const [isBuyerModalOpen, setIsBuyerModalOpen] = useState(false);
  const [isRequesterModalOpen, setIsRequesterModalOpen] = useState(false);
  const [companySearch, setCompanySearch] = useState('');
  const [buildingSearch, setBuildingSearch] = useState('');
  const [buyerSearch, setBuyerSearch] = useState('');
  const [requesterSearch, setRequesterSearch] = useState('');
  const showFilters = path === '/' || path.startsWith('/financeiro') || path.startsWith('/dashboard/financeiro');
  const showBuyerRequesterFilters = path === '/financeiro/alerta';
  const latestSyncLabel = syncInfo?.finished_at || syncInfo?.started_at
    ? new Date(syncInfo.finished_at || syncInfo.started_at).toLocaleString('pt-BR')
    : 'Sem sincronização registrada';
  const updatedFiles = (syncInfo as any)?.updated_files || [];

  useEffect(() => {
    const now = new Date();
    setGlobalPeriodMode('last6m');
    setAllTimeYear(now.getFullYear());
    setStartDate(subMonths(now, 3));
    setEndDate(now);
  }, [setEndDate, setGlobalPeriodMode, setStartDate]);

  useEffect(() => {
    // Na aba Dashboard/Geral esses filtros não são usados.
    if (!isDashboardGeral) return;
    if (selectedUser !== 'all') setSelectedUser('all');
    if (selectedRequester !== 'all') setSelectedRequester('all');
  }, [isDashboardGeral, selectedRequester, selectedUser, setSelectedRequester, setSelectedUser]);

  useEffect(() => {
    if (showBuyerRequesterFilters) return;
    if (selectedUser !== 'all') setSelectedUser('all');
    if (selectedRequester !== 'all') setSelectedRequester('all');
  }, [selectedRequester, selectedUser, setSelectedRequester, setSelectedUser, showBuyerRequesterFilters]);

  const activeTab = useMemo(() => obterAbaAtivaPorRota(path), [path]);

  const setActiveTab = (id: string) => {
    const nextPath = MAPA_ROTAS_POR_ABA[id];
    if (nextPath) navigate(nextPath);
  };

  const buildingFilterOptions = useMemo(() => {
    return obterOpcoesObraPorEmpresa(buildings, selectedCompany);
  }, [buildings, selectedCompany]);

  const yearOptions = useMemo(() => {
    const currentYear = new Date().getFullYear();
    const years: number[] = [];
    for (let year = 2026; year <= currentYear + 1; year += 1) {
      years.push(year);
    }
    return years;
  }, []);

  const filteredCompanies = useMemo(() => {
    return filtrarListaPorTexto(companies || [], companySearch, ['name', 'id']);
  }, [companies, companySearch]);

  const filteredBuildings = useMemo(() => {
    return filtrarListaPorTexto(buildingFilterOptions || [], buildingSearch, ['name', 'id', 'code']);
  }, [buildingFilterOptions, buildingSearch]);

  const filteredUsers = useMemo(() => {
    return filtrarListaPorTexto(users || [], buyerSearch, ['name', 'id']);
  }, [buyerSearch, users]);

  const filteredRequesters = useMemo(() => {
    return filtrarListaPorTexto(requesters || [], requesterSearch, ['name', 'id']);
  }, [requesterSearch, requesters]);

  useEffect(() => {
    if (!showFilters) return;

    const now = new Date();
    if (globalPeriodMode === 'last6m') {
      setStartDate(subMonths(now, 3));
      setEndDate(now);
      return;
    }

    const periodStart = new Date(allTimeYear, 0, 1);
    const periodEnd = new Date(allTimeYear, 11, 31);
    setStartDate(periodStart);
    setEndDate(periodEnd > now ? now : periodEnd);
  }, [allTimeYear, globalPeriodMode, setEndDate, setStartDate, showFilters]);

  useEffect(() => {
    const value = buildingIdInput.trim();
    if (!value) return;

    const matchedBuilding = (buildings || []).find(
      (b: any) => String(b?.id) === value || String(b?.code || '') === value,
    );

    if (!matchedBuilding) return;

    const nextBuildingId = String(matchedBuilding?.id);
    const nextCompanyId = String(matchedBuilding?.companyId || matchedBuilding?.company_id || '');

    if (fcSelectedBuilding !== nextBuildingId) {
      setFcSelectedBuilding(nextBuildingId);
    }

    if (nextCompanyId && selectedCompany !== nextCompanyId) {
      setSelectedCompany(nextCompanyId);
    }
  }, [buildingIdInput, buildings, fcSelectedBuilding, selectedCompany, setFcSelectedBuilding, setSelectedCompany]);

  useEffect(() => {
    if (fcSelectedBuilding === 'all') {
      setBuildingIdInput('');
      return;
    }
    setBuildingIdInput(String(fcSelectedBuilding));
  }, [fcSelectedBuilding]);

  useEffect(() => {
    if (!showFilters) return;
    if (selectedCompany === 'all') return;
    if (fcSelectedBuilding === 'all') return;
    const selected = (buildings || []).find((b: any) => String(b?.id) === String(fcSelectedBuilding));
    if (selected && String(selected?.companyId) !== String(selectedCompany)) {
      setFcSelectedBuilding('all');
    }
  }, [buildings, fcSelectedBuilding, selectedCompany, setFcSelectedBuilding, showFilters]);

  const downloadData = () => {
    // Moved to Context or we can re-implement here later
    alert("Função de download movida temporariamente");
  };

  const clearFilters = () => {
    setGlobalPeriodMode('last6m');
    setAllTimeYear(new Date().getFullYear());
    setBuildingIdInput('');
    setSelectedCompany('all');
    setFcSelectedBuilding('all');
    setSelectedUser('all');
    setSelectedRequester('all');
  };

  if (!sessionUser) return null;

  return (
    <div className={cn(
      "min-h-screen overflow-x-hidden font-sans transition-colors duration-300", 
      isDark ? "bg-[#0F1115] text-slate-100" : "bg-[#F3F5F7] text-[#102A40]"
    )}>
      
      {/* Header */}
      <header className={cn(
        "border-b sticky top-0 z-50 print:hidden",
        isDark ? "border-slate-800 bg-[#11141A]" : "border-slate-200 bg-white"
      )}>
        <div className="w-full max-w-[98%] 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 lg:px-8 py-3 sm:py-4 flex flex-col gap-2">
          {/* Main Header Row */}
          <div className="w-full flex items-center gap-3 sm:gap-4 lg:gap-5">
            {/* Logo */}
            <div className="flex items-center gap-2 sm:gap-3 shrink min-w-0">
              <img
                src={isDark ? logoWordmarkDark : logoWordmark}
                alt="Dinâmica Empreendimentos"
                className={cn(
                  "w-auto",
                  isDark ? "h-9 sm:h-10" : "h-8 sm:h-10"
                )}
              />
              <h1 className="hidden">Dinâmica</h1>
            </div>

            {/* Desktop Actions */}
            <div className="hidden lg:flex items-center gap-3 shrink-0 ml-auto">
            <div className={cn(
              "flex flex-col rounded-lg border px-2 py-1.5 text-xs font-bold",
              isDark ? "border-slate-700 bg-slate-900 text-slate-200" : "border-slate-200 bg-slate-50 text-slate-700"
            )}>
              <div className="flex items-center gap-1.5">
                <UserIcon size={12} className="text-[#4CB232]" />
                <span className="truncate max-w-[90px]">{sessionUser.name}</span>
              </div>
              <Button
                onClick={logout}
                variant="outline"
                className={cn(
                  "mt-1 h-7 text-xs font-bold rounded-md px-2 gap-1",
                  isDark ? "border-slate-700 bg-slate-800 text-slate-100 hover:bg-slate-700" : "border-slate-200 bg-white text-slate-700 hover:bg-slate-100"
                )}
              >
                <LogOut size={11} />
                <span>Sair</span>
              </Button>
            </div>
            
            <Button
              onClick={toggleThemeMode}
              variant="outline"
              className={cn(
                "rounded-lg h-9 px-2 gap-1.5 font-bold text-sm",
                isDark ? "border-slate-700 bg-slate-900 text-slate-100 hover:bg-slate-800" : "border-slate-200 bg-white text-slate-700 hover:bg-slate-100"
              )}
            >
              {isDark ? <Sun size={14} /> : <Moon size={14} />}
              <span className="hidden xl:inline">{isDark ? 'Dia' : 'Noite'}</span>
            </Button>
            
            <Button
              onClick={downloadData}
              variant="outline"
              className="bg-[#4CB232]/10 text-[#3A9928] border-[#4CB232]/30 hover:bg-[#4CB232] hover:text-white font-bold rounded-lg h-9 px-2 gap-1.5 text-sm"
            >
              <Download size={14} />
              <span className="hidden xl:inline">Baixar</span>
            </Button>
          </div>

            {/* Mobile Actions */}
            <div className="flex lg:hidden items-center gap-1">
            <button
              onClick={syncSienge}
              disabled={syncing}
              className={cn("w-8 h-8 flex items-center justify-center rounded-lg text-white", isDark ? "bg-[#1B3C58]" : "bg-[#102A40]")}
            >
              <RefreshCw size={14} className={cn(syncing && "animate-spin")} />
            </button>
            <button
              onClick={toggleThemeMode}
              className={cn("w-8 h-8 flex items-center justify-center rounded-lg", isDark ? "bg-slate-900 text-slate-100" : "bg-white text-slate-700 border border-slate-200")}
            >
              {isDark ? <Sun size={14} /> : <Moon size={14} />}
            </button>
            <button
              onClick={logout}
              className={cn("w-8 h-8 flex items-center justify-center rounded-lg", isDark ? "bg-slate-800 text-slate-200" : "bg-slate-200 text-slate-700")}
            >
              <LogOut size={14} />
            </button>
            </div>
          </div>

          {/* Status Block Row - Centered like navigation tabs */}
          <div className="relative z-40 w-full flex justify-start lg:justify-center overflow-x-auto lg:overflow-visible custom-scrollbar">
            <div className="hidden lg:flex items-center gap-2.5 flex-nowrap">
              {apiStatus === 'online' && (
                <span className="inline-flex items-center gap-1.5 px-3.5 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/25 text-[11px] font-black text-emerald-500 uppercase tracking-wider whitespace-nowrap">
                  <span className="w-1 h-1 rounded-full bg-emerald-500 animate-pulse" style={{ marginBottom: '0px', marginTop: '0px' }} />
                  Live
                </span>
              )}
              <span className={cn(
                "inline-flex items-center rounded-lg border px-3.5 py-2 text-[11px] font-bold uppercase tracking-wider whitespace-nowrap",
                isDark ? "border-slate-700 bg-slate-900 text-slate-300" : "border-slate-200 bg-white text-slate-600"
              )} style={{ marginBottom: '0px', marginTop: '0px' }}>
                {latestSyncLabel}
              </span>
              <button
                type="button"
                onClick={syncSienge}
                disabled={syncing}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-lg border px-3.5 py-2 text-[11px] font-black uppercase tracking-wider transition whitespace-nowrap",
                  isDark
                    ? "border-[#4CB232]/35 bg-[#4CB232]/12 text-[#8BE06B] hover:bg-[#4CB232]/25"
                    : "border-[#4CB232]/35 bg-[#4CB232]/12 text-[#2D7A1D] hover:bg-[#4CB232]/22"
                )}
                style={{ marginBottom: '0px', marginTop: '0px' }}
              >
                <RefreshCw size={13} className={cn(syncing && 'animate-spin')} />
                <span className="hidden sm:inline">{syncing ? 'Sincronizando' : 'Sincronizar'}</span>
              </button>
              <button
                type="button"
                onClick={() => setSyncDetailsOpen(true)}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-lg border px-3.5 py-2 text-[11px] font-black uppercase tracking-wider transition whitespace-nowrap",
                  isDark
                    ? "border-slate-700 bg-slate-900 text-slate-200 hover:bg-slate-800"
                    : "border-slate-200 bg-white text-slate-600 hover:bg-slate-100"
                )}
                style={{ marginBottom: '0px', marginTop: '0px' }}
              >
                Detalhar Atualização
              </button>
            </div>
          </div>

          {/* Navigation Tabs */}
          <div className="relative z-40 w-full flex justify-start lg:justify-center overflow-x-auto lg:overflow-visible custom-scrollbar pb-1" style={{ marginBottom: '0px', marginTop: '0px' }}>
            <div className="w-fit">
              <NavigationMenu activeTab={activeTab} setActiveTab={setActiveTab as any} isRestrictedUser={isRestrictedUser} />
            </div>
          </div>
        </div>
      </header>

      {syncDetailsOpen && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/70 p-4 print:hidden" onClick={() => setSyncDetailsOpen(false)}>
          <div
            className={cn(
              "w-full max-w-3xl rounded-2xl border shadow-2xl overflow-hidden",
              isDark ? "border-slate-800 bg-[#11141A]" : "border-slate-200 bg-white"
            )}
            onClick={(event) => event.stopPropagation()}
          >
            <div className={cn("flex items-start justify-between gap-4 border-b px-5 py-4", isDark ? "border-slate-800" : "border-slate-200") }>
              <div>
                <div className="text-[11px] uppercase tracking-[0.24em] text-[#4CB232] font-black">Detalhamento da atualização</div>
                <div className={cn("mt-1 text-sm", isDark ? "text-slate-300" : "text-slate-600")}>{syncInfo?.message || 'Sincronização concluída'}</div>
              </div>
              <Button variant="ghost" onClick={() => setSyncDetailsOpen(false)}>Fechar</Button>
            </div>
            <div className="p-5 space-y-4 max-h-[70vh] overflow-auto custom-scrollbar">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <div className={cn("rounded-xl border p-3", isDark ? "border-slate-800 bg-white/[0.03]" : "border-slate-200 bg-slate-50")}>
                  <div className="text-[10px] uppercase tracking-wider text-slate-400">Início</div>
                  <div className={cn("mt-1 text-sm font-semibold", isDark ? "text-white" : "text-slate-900")}>{syncInfo?.started_at ? new Date(syncInfo.started_at).toLocaleString('pt-BR') : '-'}</div>
                </div>
                <div className={cn("rounded-xl border p-3", isDark ? "border-slate-800 bg-white/[0.03]" : "border-slate-200 bg-slate-50")}>
                  <div className="text-[10px] uppercase tracking-wider text-slate-400">Fim</div>
                  <div className={cn("mt-1 text-sm font-semibold", isDark ? "text-white" : "text-slate-900")}>{syncInfo?.finished_at ? new Date(syncInfo.finished_at).toLocaleString('pt-BR') : '-'}</div>
                </div>
                <div className={cn("rounded-xl border p-3", isDark ? "border-slate-800 bg-white/[0.03]" : "border-slate-200 bg-slate-50")}>
                  <div className="text-[10px] uppercase tracking-wider text-slate-400">Arquivos atualizados</div>
                  <div className={cn("mt-1 text-sm font-semibold", isDark ? "text-emerald-300" : "text-emerald-700")}>{updatedFiles.length}</div>
                </div>
              </div>

              <div className={cn("rounded-xl border p-3 text-sm", isDark ? "border-slate-800 bg-white/[0.02] text-slate-300" : "border-slate-200 bg-slate-50 text-slate-700")}>
                <div className="text-[10px] uppercase tracking-wider text-slate-400">Última atualização</div>
                <div className="mt-1 font-medium">{latestSyncLabel}</div>
              </div>

              {updatedFiles.length > 0 ? (
                <div className={cn("overflow-hidden rounded-2xl border", isDark ? "border-slate-800" : "border-slate-200")}>
                  <table className="w-full border-separate border-spacing-0 text-sm">
                    <thead className={cn(isDark ? "bg-slate-900 text-slate-300" : "bg-slate-50 text-slate-600")}>
                      <tr>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Arquivo</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Dataset</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Motivo</th>
                        <th className="px-3 py-2 text-left text-[11px] uppercase tracking-wider">Período</th>
                      </tr>
                    </thead>
                    <tbody>
                      {updatedFiles.map((file: any) => (
                        <tr key={file.file_name} className={cn("border-t", isDark ? "border-slate-800 odd:bg-white/[0.02]" : "border-slate-200 odd:bg-slate-50")}>
                          <td className={cn("px-3 py-2 font-mono text-xs break-all", isDark ? "text-slate-100" : "text-slate-800")}>{file.file_name}</td>
                          <td className={cn("px-3 py-2", isDark ? "text-slate-300" : "text-slate-700")}>{file.dataset}</td>
                          <td className={cn("px-3 py-2", isDark ? "text-slate-300" : "text-slate-700")}>{file.reason || '-'}</td>
                          <td className={cn("px-3 py-2 text-xs", isDark ? "text-slate-400" : "text-slate-500")}>{file.start_date || '-'} {file.end_date ? `até ${file.end_date}` : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className={cn("rounded-xl border p-4 text-sm", isDark ? "border-slate-800 bg-white/[0.02] text-slate-400" : "border-slate-200 bg-slate-50 text-slate-500")}>
                  Nenhum arquivo novo foi gerado nesta atualização.
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      <main className="w-full max-w-full 2xl:max-w-[1800px] mx-auto px-4 sm:px-6 py-6 sm:py-10 pb-24 xl:pb-10 pt-10 xl:pt-8">
        
        {/* Filters Topbar */}
        {showFilters && (
          <div className="mb-6 sm:mb-10 bg-[#161618] rounded-2xl border border-white/5 shadow-xl print:hidden overflow-hidden mt-8 xl:mt-0">
            <button onClick={() => setMobileFiltersOpen(!mobileFiltersOpen)} className="w-full flex items-center justify-between p-4 sm:p-6 md:cursor-default">
              <div className="flex items-center gap-3">
                <SlidersHorizontal size={16} className="text-orange-500" />
                <span className="text-sm font-black uppercase tracking-widest text-orange-500">Filtros</span>
              </div>
              <ChevronDown size={16} className={cn("text-gray-500 transition-transform md:hidden", mobileFiltersOpen && "rotate-180")} />
            </button>
            <div className={cn("md:block", mobileFiltersOpen ? "block" : "hidden")}>
              <div className="flex flex-col sm:flex-row flex-wrap items-stretch sm:items-end gap-4 px-4 sm:px-6 pb-4 sm:pb-6 pt-0">
                <div className="space-y-2 flex-1 sm:flex-none">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Período</Label>
                  <div className="flex items-center gap-2 bg-black/30 border border-white/10 rounded-xl p-1 h-11">
                    <button
                      onClick={() => {
                        setGlobalPeriodMode('last6m');
                      }}
                      className={cn(
                        "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                        globalPeriodMode === 'last6m'
                          ? "bg-orange-600 text-white"
                          : "text-gray-300 hover:text-white hover:bg-white/10"
                      )}
                    >
                      Últimos 3 meses
                    </button>
                    <button
                      onClick={() => {
                        setGlobalPeriodMode('all');
                      }}
                      className={cn(
                        "h-9 px-3 rounded-lg text-[11px] font-black uppercase tracking-wide transition-all",
                        globalPeriodMode === 'all'
                          ? "bg-sky-600 text-white"
                          : "text-gray-300 hover:text-white hover:bg-white/10"
                      )}
                    >
                      Período total
                    </button>
                  </div>
                </div>

                <div className="space-y-2 w-[100px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">ID</Label>
                  <input
                    value={buildingIdInput}
                    onChange={(e) => setBuildingIdInput(e.target.value)}
                    placeholder="obra"
                    className="w-full h-11 rounded-xl bg-black/40 border border-white/10 px-3 text-sm font-bold text-white placeholder:text-gray-500 outline-none focus:border-orange-500"
                  />
                </div>

                <div className="space-y-2 flex-1 sm:flex-none">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Inicial</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full sm:w-[160px] bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !startDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                        {startDate ? format(startDate, "dd/MM/yyyy") : <span>Início</span>}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10" align="start">
                      <Calendar
                        mode="single"
                        selected={startDate}
                        onSelect={(date: any) => {
                          if (!date) return;
                          setGlobalPeriodMode('last6m');
                          setStartDate(date);
                        }}
                        className="text-white"
                      />
                    </PopoverContent>
                  </Popover>
                </div>

                <div className="space-y-2 flex-1 sm:flex-none">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Data Final</Label>
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className={cn("w-full sm:w-[160px] bg-black/40 border-white/10 h-11 rounded-xl justify-start text-left font-bold text-white", !endDate && "text-gray-400")}>
                        <CalendarIcon className="mr-2 h-4 w-4 text-orange-500" />
                        {endDate ? format(endDate, "dd/MM/yyyy") : <span>Fim</span>}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-auto p-0 bg-[#161618] border-white/10" align="start">
                      <Calendar
                        mode="single"
                        selected={endDate}
                        onSelect={(date: any) => {
                          if (!date) return;
                          setGlobalPeriodMode('last6m');
                          setEndDate(date);
                        }}
                        className="text-white"
                      />
                    </PopoverContent>
                  </Popover>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Empresa (Sienge)</Label>
                  <div className="flex items-center gap-2">
                    <div className="flex-1">
                      <Select value={selectedCompany} onValueChange={setSelectedCompany}>
                        <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                          <span className="truncate">{selectedCompany === 'all' ? 'Todas as Empresas' : companies?.find((c: any) => String(c.id) === selectedCompany)?.name || 'Todas as Empresas'}</span>
                        </SelectTrigger>
                        <SelectContent className="bg-[#161618] border-white/10 text-white">
                          <SelectItem value="all">Todas as Empresas</SelectItem>
                          {companies?.map((c: any) => (
                            <SelectItem key={`empresa-${c.id}`} value={String(c.id)}>{c.name || `Empresa ${c.id}`}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => setIsCompanyModalOpen(true)}
                      className="h-11 w-11 p-0 border-white/10 bg-black/30 text-white hover:bg-white/10 rounded-xl"
                      aria-label="Buscar empresa"
                    >
                      <Search size={16} />
                    </Button>
                  </div>
                </div>

                <div className="space-y-2 flex-1 min-w-[200px]">
                  <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Obra</Label>
                  <div className="flex items-center gap-2">
                    <div className="flex-1">
                      <Select
                        value={fcSelectedBuilding}
                        onValueChange={(value) => {
                          setFcSelectedBuilding(value);
                          if (value === 'all') {
                            setBuildingIdInput('');
                            return;
                          }

                          setBuildingIdInput(value);
                          const selected = (buildings || []).find((b: any) => String(b?.id) === String(value));
                          if (selected?.companyId || selected?.company_id) {
                            setSelectedCompany(String(selected.companyId || selected.company_id));
                          }
                        }}
                      >
                        <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                          <span className="truncate">{fcSelectedBuilding === 'all' ? 'Todas as Obras' : buildings?.find((b: any) => String(b.id) === fcSelectedBuilding)?.name || `Obra ${fcSelectedBuilding}`}</span>
                        </SelectTrigger>
                        <SelectContent className="bg-[#161618] border-white/10 text-white">
                          <SelectItem value="all">Todas as Obras</SelectItem>
                          {buildingFilterOptions?.map((b: any) => (
                            <SelectItem key={`obra-${b.id}`} value={String(b.id)}>{b.name || `Obra ${b.id}`}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => setIsBuildingModalOpen(true)}
                      className="h-11 w-11 p-0 border-white/10 bg-black/30 text-white hover:bg-white/10 rounded-xl"
                      aria-label="Buscar obra"
                    >
                      <Search size={16} />
                    </Button>
                  </div>
                </div>

                {globalPeriodMode === 'all' && (
                  <div className="space-y-2 w-[120px]">
                    <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Ano</Label>
                    <Select value={String(allTimeYear)} onValueChange={(value) => setAllTimeYear(Number(value))}>
                      <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                        <span className="truncate">{String(allTimeYear)}</span>
                      </SelectTrigger>
                      <SelectContent className="bg-[#161618] border-white/10 text-white">
                        {yearOptions.map((year) => (
                          <SelectItem key={`year-${year}`} value={String(year)}>{String(year)}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                )}

                {showBuyerRequesterFilters && (
                  <>
                    <div className="space-y-2 flex-1 min-w-[180px]">
                      <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Comprador</Label>
                      <div className="flex items-center gap-2">
                        <div className="flex-1">
                          <Select value={selectedUser} onValueChange={setSelectedUser}>
                            <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                              <span className="truncate">{selectedUser === 'all' ? 'Todos' : users?.find((u: any) => String(u.id) === selectedUser)?.name || 'Todos'}</span>
                            </SelectTrigger>
                            <SelectContent className="bg-[#161618] border-white/10 text-white">
                              <SelectItem value="all">Todos os Compradores</SelectItem>
                              {users?.map((u: any) => (
                                <SelectItem key={`user-${u.id}`} value={String(u.id)}>{u.name}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <Button
                          type="button"
                          variant="outline"
                          onClick={() => setIsBuyerModalOpen(true)}
                          className="h-11 w-11 p-0 border-white/10 bg-black/30 text-white hover:bg-white/10 rounded-xl"
                          aria-label="Buscar comprador"
                        >
                          <Search size={16} />
                        </Button>
                      </div>
                    </div>

                    <div className="space-y-2 flex-1 min-w-[180px]">
                      <Label className="text-[10px] font-black uppercase tracking-widest text-orange-500">Solicitante</Label>
                      <div className="flex items-center gap-2">
                        <div className="flex-1">
                          <Select value={selectedRequester} onValueChange={setSelectedRequester}>
                            <SelectTrigger className="w-full bg-black/40 border-white/10 h-11 rounded-xl text-white font-bold">
                              <span className="truncate">{selectedRequester === 'all' ? 'Todos' : requesters?.find((r: any) => String(r.id) === selectedRequester)?.name || 'Todos'}</span>
                            </SelectTrigger>
                            <SelectContent className="bg-[#161618] border-white/10 text-white">
                              <SelectItem value="all">Todos os Solicitantes</SelectItem>
                              {requesters?.map((r: any) => (
                                <SelectItem key={`requester-${r.id}`} value={String(r.id)}>{r.name}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <Button
                          type="button"
                          variant="outline"
                          onClick={() => setIsRequesterModalOpen(true)}
                          className="h-11 w-11 p-0 border-white/10 bg-black/30 text-white hover:bg-white/10 rounded-xl"
                          aria-label="Buscar solicitante"
                        >
                          <Search size={16} />
                        </Button>
                      </div>
                    </div>
                  </>
                )}

                <div className="flex items-end gap-2 w-full sm:w-auto">
                  <Button
                    onClick={() => {
                      clearFilters();
                      applyFilters();
                      setMobileFiltersOpen(false);
                    }}
                    variant="outline"
                    className="h-11 px-4 border-white/10 bg-black/30 text-white hover:bg-white/10 font-black rounded-xl w-full sm:w-auto"
                  >
                    <X size={14} className="mr-2" />
                    Limpar
                  </Button>
                  <Button 
                    onClick={() => {
                      applyFilters();
                      setMobileFiltersOpen(false);
                    }} 
                    className="h-11 px-6 bg-orange-600 hover:bg-orange-700 text-white font-black rounded-xl shadow-lg shadow-orange-600/20 w-full sm:w-auto"
                  >
                    <SlidersHorizontal size={14} className="mr-2" />
                    Filtrar
                  </Button>
                </div>
              </div>
            </div>
          </div>
        )}

        <Outlet />

        {isCompanyModalOpen && (
          <ModalComponent title="Selecionar empresa" onClose={() => setIsCompanyModalOpen(false)}>
            <input
              value={companySearch}
              onChange={(e) => setCompanySearch(e.target.value)}
              placeholder="Buscar por nome ou ID"
              className={cn("w-full h-10 rounded-lg border px-3 text-sm", isDark ? "bg-black/30 border-white/10 text-white placeholder:text-slate-500" : "bg-white border-slate-300 text-slate-800")}
            />
            <div className="max-h-[340px] overflow-auto space-y-2">
              <label className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                <input
                  type="checkbox"
                  checked={selectedCompany === 'all'}
                  onChange={() => {
                    setSelectedCompany('all');
                    setIsCompanyModalOpen(false);
                  }}
                />
                Todas as empresas
              </label>
              {filteredCompanies.map((c: any) => {
                const id = String(c.id);
                return (
                  <label key={`company-modal-${id}`} className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                    <input
                      type="checkbox"
                      checked={selectedCompany === id}
                      onChange={() => {
                        setSelectedCompany(id);
                        setFcSelectedBuilding('all');
                        setBuildingIdInput('');
                        setIsCompanyModalOpen(false);
                      }}
                    />
                    <span className="truncate">{c.name || `Empresa ${id}`} (ID: {id})</span>
                  </label>
                );
              })}
            </div>
          </ModalComponent>
        )}

        {isBuildingModalOpen && (
          <ModalComponent title="Selecionar obra" onClose={() => setIsBuildingModalOpen(false)}>
            <input
              value={buildingSearch}
              onChange={(e) => setBuildingSearch(e.target.value)}
              placeholder="Buscar por nome, ID ou código"
              className={cn("w-full h-10 rounded-lg border px-3 text-sm", isDark ? "bg-black/30 border-white/10 text-white placeholder:text-slate-500" : "bg-white border-slate-300 text-slate-800")}
            />
            <div className="max-h-[340px] overflow-auto space-y-2">
              <label className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                <input
                  type="checkbox"
                  checked={fcSelectedBuilding === 'all'}
                  onChange={() => {
                    setFcSelectedBuilding('all');
                    setBuildingIdInput('');
                    setIsBuildingModalOpen(false);
                  }}
                />
                Todas as obras
              </label>
              {filteredBuildings.map((b: any) => {
                const id = String(b.id);
                return (
                  <label key={`building-modal-${id}`} className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                    <input
                      type="checkbox"
                      checked={fcSelectedBuilding === id}
                      onChange={() => {
                        setFcSelectedBuilding(id);
                        setBuildingIdInput(id);
                        if (b?.companyId || b?.company_id) {
                          setSelectedCompany(String(b.companyId || b.company_id));
                        }
                        setIsBuildingModalOpen(false);
                      }}
                    />
                    <span>{b.name || `Obra ${id}`} (ID: {id})</span>
                  </label>
                );
              })}
            </div>
          </ModalComponent>
        )}

        {showBuyerRequesterFilters && isBuyerModalOpen && (
          <ModalComponent title="Selecionar comprador" onClose={() => setIsBuyerModalOpen(false)}>
            <input
              value={buyerSearch}
              onChange={(e) => setBuyerSearch(e.target.value)}
              placeholder="Buscar por nome ou ID"
              className={cn("w-full h-10 rounded-lg border px-3 text-sm", isDark ? "bg-black/30 border-white/10 text-white placeholder:text-slate-500" : "bg-white border-slate-300 text-slate-800")}
            />
            <div className="max-h-[340px] overflow-auto space-y-2">
              <label className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                <input
                  type="checkbox"
                  checked={selectedUser === 'all'}
                  onChange={() => {
                    setSelectedUser('all');
                    setIsBuyerModalOpen(false);
                  }}
                />
                Todos os compradores
              </label>
              {filteredUsers.map((u: any) => {
                const id = String(u.id);
                return (
                  <label key={`buyer-modal-${id}`} className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                    <input
                      type="checkbox"
                      checked={selectedUser === id}
                      onChange={() => {
                        setSelectedUser(id);
                        setIsBuyerModalOpen(false);
                      }}
                    />
                    <span className="truncate">{u.name || `Usuário ${id}`}</span>
                  </label>
                );
              })}
            </div>
          </ModalComponent>
        )}

        {showBuyerRequesterFilters && isRequesterModalOpen && (
          <ModalComponent title="Selecionar solicitante" onClose={() => setIsRequesterModalOpen(false)}>
            <input
              value={requesterSearch}
              onChange={(e) => setRequesterSearch(e.target.value)}
              placeholder="Buscar por nome ou ID"
              className={cn("w-full h-10 rounded-lg border px-3 text-sm", isDark ? "bg-black/30 border-white/10 text-white placeholder:text-slate-500" : "bg-white border-slate-300 text-slate-800")}
            />
            <div className="max-h-[340px] overflow-auto space-y-2">
              <label className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                <input
                  type="checkbox"
                  checked={selectedRequester === 'all'}
                  onChange={() => {
                    setSelectedRequester('all');
                    setIsRequesterModalOpen(false);
                  }}
                />
                Todos os solicitantes
              </label>
              {filteredRequesters.map((r: any) => {
                const id = String(r.id);
                return (
                  <label key={`requester-modal-${id}`} className={cn("flex items-center gap-2 text-sm cursor-pointer", isDark ? "text-slate-200" : "text-slate-700")}>
                    <input
                      type="checkbox"
                      checked={selectedRequester === id}
                      onChange={() => {
                        setSelectedRequester(id);
                        setIsRequesterModalOpen(false);
                      }}
                    />
                    <span className="truncate">{r.name || `Solicitante ${id}`}</span>
                  </label>
                );
              })}
            </div>
          </ModalComponent>
        )}

      </main>
    </div>
  );
}

