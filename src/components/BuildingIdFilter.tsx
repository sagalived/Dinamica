import React, { useState, useEffect } from 'react';
import { useSienge } from '../contexts/SiengeContext';

interface BuildingIdFilterProps {
  onBuildingSelect?: (building: { id: string; name: string; company_id?: string; company_name?: string }) => void;
  placeholder?: string;
}

export function BuildingIdFilter({ onBuildingSelect, placeholder = "Digite o ID da obra..." }: BuildingIdFilterProps) {
  const [inputValue, setInputValue] = useState('');
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const { obras } = useSienge();

  // Filtrar sugestões conforme digita
  useEffect(() => {
    if (inputValue.trim().length > 0) {
      const filtered = obras.filter(obra => 
        obra.id?.toString().includes(inputValue) || 
        obra.name?.toLowerCase().includes(inputValue.toLowerCase())
      ).slice(0, 8);
      
      setSuggestions(filtered);
      setIsOpen(filtered.length > 0);
    } else {
      setSuggestions([]);
      setIsOpen(false);
    }
  }, [inputValue, obras]);

  const handleSelectBuilding = (building: any) => {
    setInputValue(`${building.id} - ${building.name}`);
    setSuggestions([]);
    setIsOpen(false);
    
    if (onBuildingSelect) {
      onBuildingSelect({
        id: building.id?.toString() || '',
        name: building.name || '',
        company_id: building.company_id,
        company_name: building.company_name
      });
    }
  };

  const handleClear = () => {
    setInputValue('');
    setSuggestions([]);
    setIsOpen(false);
    if (onBuildingSelect) {
      onBuildingSelect({ id: '', name: '', company_id: '', company_name: '' });
    }
  };

  return (
    <div className="relative">
      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <input
            type="text"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onFocus={() => inputValue.length > 0 && setIsOpen(true)}
            placeholder={placeholder}
            className="w-full px-3 py-2 bg-slate-700 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:border-blue-500 transition-colors"
          />
          
          {/* Dropdown de sugestões */}
          {isOpen && suggestions.length > 0 && (
            <div className="absolute top-full left-0 right-0 mt-1 bg-slate-700 border border-slate-600 rounded-lg shadow-lg z-50 max-h-64 overflow-y-auto">
              {suggestions.map((building) => (
                <button
                  key={building.id}
                  onClick={() => handleSelectBuilding(building)}
                  className="w-full text-left px-3 py-2 hover:bg-slate-600 transition-colors border-b border-slate-600 last:border-b-0"
                >
                  <div className="font-semibold text-sm text-gray-200">{building.id} - {building.name}</div>
                  {building.company_name && (
                    <div className="text-xs text-gray-400">{building.company_name}</div>
                  )}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Botão Limpar */}
        {inputValue && (
          <button
            onClick={handleClear}
            className="px-3 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg text-sm transition-colors"
          >
            ✕
          </button>
        )}
      </div>
    </div>
  );
}
