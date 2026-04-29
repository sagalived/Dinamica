import { cn } from '../lib/utils';

interface NavProps {
  activeTab: string;
  setActiveTab: (val: string) => void;
  isRestrictedUser?: boolean;
}

export function NavigationMenu({ activeTab, setActiveTab, isRestrictedUser }: NavProps) {
  if (isRestrictedUser) {
    return (
      <nav className="hidden xl:flex items-center gap-2">
        <button
          onClick={() => setActiveTab('logistics')}
          className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold uppercase transition-all tracking-wider",
            activeTab === 'logistics' ? "bg-[#4CB232] text-white" : "text-slate-500 hover:text-[#102A40]"
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
    <nav className="hidden xl:flex items-center gap-2">
      {menuConfig.map(menu => {
        const isActiveContext = activeTab.startsWith(menu.id) || menu.options.some(o => o.id === activeTab);
        return (
          <div key={menu.id} className="relative group">
            <button 
              onClick={() => setActiveTab(menu.options[0].id)}
              className={cn(
                "flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-bold uppercase transition-all tracking-widest",
                isActiveContext
                  ? "bg-[#4CB232] text-white shadow-lg shadow-[#4CB232]/25"
                  : "text-slate-500 hover:text-[#102A40] hover:bg-slate-100"
              )}
            >
              <span>{menu.label}</span>
            </button>
            
            <div className="absolute left-0 top-full pt-2 opacity-0 translate-y-2 pointer-events-none group-hover:opacity-100 group-hover:translate-y-0 group-hover:pointer-events-auto transition-all duration-200 z-50">
              <div className="flex flex-col bg-white border border-slate-200 rounded-xl shadow-2xl p-2 min-w-[220px] gap-1">
                {menu.options.map(opt => (
                  <button
                    key={opt.id}
                    onClick={() => setActiveTab(opt.id)}
                    className={cn(
                      "text-left px-4 py-2.5 rounded-lg text-sm font-bold uppercase transition-all tracking-wider",
                      activeTab === opt.id 
                        ? "bg-[#4CB232]/15 text-[#2E861E]" 
                        : "text-slate-600 hover:bg-slate-100 hover:text-[#102A40]"
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
