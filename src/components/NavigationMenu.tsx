import { cn } from '../lib/utils';

interface NavProps {
  activeTab: string;
  setActiveTab: (val: string) => void;
  isRestrictedUser?: boolean;
}

export function NavigationMenu({ activeTab, setActiveTab, isRestrictedUser }: NavProps) {
  if (isRestrictedUser) {
    return (
      <nav className="flex items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.02] p-1.5">
        <button
          onClick={() => setActiveTab('logistics')}
          className={cn(
            "flex items-center gap-2 px-5 py-2.5 rounded-xl text-sm font-black uppercase transition-all tracking-[0.14em]",
            activeTab === 'logistics'
              ? "bg-[#4CB232] text-white"
              : "text-slate-400 hover:text-white hover:bg-white/5"
          )}
        >
          LOGÍSTICA
        </button>
      </nav>
    );
  }

  const menuConfig = [
    {
      id: 'dashboard',
      label: 'DASHBOARD',
      options: [
        { id: 'dashboard', label: 'GERAL' },
        { id: 'dashboard-financeiro', label: 'FINANCEIROS' },
        { id: 'dashboard-obras', label: 'OBRAS' },
        { id: 'dashboard-logistica', label: 'LOGISTICA' }
      ]
    },
    {
      id: 'financeiro',
      label: 'FINANCEIRO',
      options: [
        { id: 'finance', label: 'VALORES' },
        { id: 'financeiro-fluxo', label: 'FLUXO DE CAIXA' },
        { id: 'financeiro-leandro', label: 'LEANDRO' },
        { id: 'financeiro-centro-custo', label: 'CENTRO DE CUSTO' },
        { id: 'alerts', label: 'ALERTA' }
      ]
    },
    {
      id: 'obras',
      label: 'OBRAS',
      options: [
        { id: 'obras-diario', label: 'DIARIO DE OBRAS' },
        { id: 'map', label: 'VALORES' },
        { id: 'obras-alerta', label: 'ALERTA' }
      ]
    },
    {
      id: 'logistica',
      label: 'LOGISTICA',
      options: [
        { id: 'logistics', label: 'ACOMPANHAMENTO' }
      ]
    }
  ];

  return (
    <nav className="flex items-center gap-1.5 rounded-2xl border border-white/10 bg-[#111827] p-1.5">
      {menuConfig.map(menu => {
        const isActiveContext = activeTab.startsWith(menu.id) || menu.options.some(o => o.id === activeTab);
        return (
          <div key={menu.id} className="relative group">
            <button 
              onClick={() => setActiveTab(menu.options[0].id)}
              className={cn(
                "flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-black uppercase transition-all tracking-[0.14em]",
                isActiveContext
                  ? "bg-[#4CB232] text-white"
                  : "text-slate-400 hover:text-white hover:bg-white/5"
              )}
            >
              <span>{menu.label}</span>
            </button>
            
            <div className="absolute left-0 top-full pt-2 opacity-0 translate-y-2 pointer-events-none group-hover:opacity-100 group-hover:translate-y-0 group-hover:pointer-events-auto transition-all duration-200 z-50">
              <div className="flex flex-col bg-[#161618] border border-white/10 rounded-2xl shadow-2xl p-2 min-w-[240px] gap-1">
                {menu.options.map(opt => (
                  <button
                    key={opt.id}
                    onClick={() => setActiveTab(opt.id)}
                    className={cn(
                      "text-left px-4 py-2.5 rounded-xl text-sm font-bold uppercase transition-all tracking-wider",
                      activeTab === opt.id 
                        ? "bg-[#4CB232]/15 text-[#8BE06B]" 
                        : "text-slate-300 hover:bg-white/5 hover:text-white"
                    )}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            </div>
          </div>
        );
      })}
    </nav>
  );
}
