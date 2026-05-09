import React, { useState, useEffect } from 'react';
import { X } from 'lucide-react';
import { useSienge } from '../contexts/SiengeContext';

interface CompanyIdFilterProps {
  onCompanySelect?: (company: { id: string; name: string }) => void;
  placeholder?: string;
}

export function CompanyIdFilter({ onCompanySelect, placeholder = 'Digite o ID da empresa...' }: CompanyIdFilterProps) {
  const { companies } = useSienge();
  const [inputValue, setInputValue] = useState('');
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [filteredSuggestions, setFilteredSuggestions] = useState<any[]>([]);

  useEffect(() => {
    if (!inputValue.trim()) {
      setFilteredSuggestions([]);
      setShowSuggestions(false);
      return;
    }

    const searchTerm = inputValue.toLowerCase();
    const filtered = (companies || []).filter((company: any) => {
      const id = String(company.id).toLowerCase();
      const name = String(company.name).toLowerCase();
      return id.includes(searchTerm) || name.includes(searchTerm);
    });

    setFilteredSuggestions(filtered);
    setShowSuggestions(filtered.length > 0);
  }, [inputValue, companies]);

  const handleSelectCompany = (company: any) => {
    setInputValue('');
    setShowSuggestions(false);
    onCompanySelect?.({
      id: String(company.id),
      name: String(company.name)
    });
  };

  const handleClear = () => {
    setInputValue('');
    setShowSuggestions(false);
    onCompanySelect?.({ id: '', name: '' });
  };

  return (
    <div className="relative">
      <div className="relative">
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onFocus={() => inputValue && setShowSuggestions(true)}
          placeholder={placeholder}
          className="w-full h-11 px-3 rounded-xl bg-black/40 border border-white/10 text-white font-bold focus:outline-none focus:border-blue-500/50 placeholder-gray-500"
        />
        {inputValue && (
          <button
            onClick={handleClear}
            className="absolute right-3 top-1/2 -translate-y-1/2 p-1 hover:bg-white/10 rounded-lg transition-colors text-gray-400 hover:text-red-400"
            type="button"
          >
            <X size={16} />
          </button>
        )}
      </div>

      {/* Dropdown de sugestões */}
      {showSuggestions && filteredSuggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-2 bg-[#161618] border border-white/10 rounded-xl shadow-2xl z-50 max-h-64 overflow-y-auto">
          {filteredSuggestions.map((company: any) => (
            <button
              key={`company-${company.id}`}
              onClick={() => handleSelectCompany(company)}
              className="w-full text-left px-3 py-2.5 hover:bg-white/10 transition-colors border-b border-white/5 last:border-b-0"
            >
              <div className="text-sm font-bold text-white">{company.id} - {company.name}</div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
