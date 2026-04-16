import React, { useState, useEffect } from 'react';
import { Truck, MapPin, Wrench, Fuel, Plus, Trash2, Calendar, FileText } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from './ui/card';
import { Button } from './ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { format } from 'date-fns';

interface LogisticsEntry {
  id: string;
  date: string;
  type: 'combustivel' | 'manutencao' | 'rota' | 'outros';
  vehicle: string;
  user: string;
  cost: number;
  startAddress?: string;
  endAddress?: string;
  fuelConsumed?: number;
  notes?: string;
}

export function LogisticsTab() {
  const [entries, setEntries] = useState<LogisticsEntry[]>([]);
  const [formData, setFormData] = useState<Partial<LogisticsEntry>>({
    type: 'combustivel',
    date: new Date().toISOString().split('T')[0],
  });

  // Load from local storage
  useEffect(() => {
    try {
      const stored = localStorage.getItem('dinamica_logistics');
      if (stored) {
        setEntries(JSON.parse(stored));
      }
    } catch (e) {
      console.error(e);
    }
  }, []);

  // Save to local storage
  useEffect(() => {
    localStorage.setItem('dinamica_logistics', JSON.stringify(entries));
  }, [entries]);

  const handleCreate = () => {
    if (!formData.vehicle || !formData.user || !formData.cost || formData.cost <= 0) {
      alert("Preencha Veículo, Usuário e Valor Gasto corretamente.");
      return;
    }
    
    const newEntry: LogisticsEntry = {
      id: Date.now().toString(),
      date: formData.date || new Date().toISOString().split('T')[0],
      type: formData.type as any,
      vehicle: formData.vehicle,
      user: formData.user,
      cost: Number(formData.cost),
      startAddress: formData.startAddress,
      endAddress: formData.endAddress,
      fuelConsumed: formData.fuelConsumed ? Number(formData.fuelConsumed) : undefined,
      notes: formData.notes
    };

    setEntries(prev => [newEntry, ...prev]);
    setFormData({
      type: 'combustivel',
      date: new Date().toISOString().split('T')[0],
      vehicle: '',
      user: '',
      cost: 0,
      startAddress: '',
      endAddress: '',
      fuelConsumed: 0,
      notes: ''
    });
  };

  const removeEntry = (id: string) => {
    setEntries(prev => prev.filter(e => e.id !== id));
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'combustivel': return <Fuel fill="#10b981" className="text-emerald-500" size={16} />;
      case 'manutencao': return <Wrench fill="#f59e0b" className="text-amber-500" size={16} />;
      case 'rota': return <MapPin fill="#3b82f6" className="text-blue-500" size={16} />;
      default: return <Truck fill="#6b7280" className="text-gray-500" size={16} />;
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3 mb-6">
        <Truck className="text-orange-500" size={32} />
        <div>
          <h2 className="text-2xl font-black text-white uppercase tracking-tight">Logística e Frota</h2>
          <p className="text-gray-500 text-sm">Controle manual de gastos de veículos, manutenções e rotas.</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Formulário Novo Registro */}
        <Card className="bg-[#161618] border-white/5 shadow-2xl lg:col-span-1 h-fit">
          <CardHeader>
            <CardTitle className="text-lg font-black uppercase tracking-tight text-white flex items-center gap-2">
              <Plus size={18} className="text-orange-500" /> Novo Registro
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label className="text-xs font-bold text-gray-400 uppercase">Data</Label>
              <Input 
                type="date" 
                value={formData.date || ''} 
                onChange={(e) => setFormData({...formData, date: e.target.value})}
                className="bg-black/40 border-white/10 text-white"
              />
            </div>
            
            <div className="space-y-2">
              <Label className="text-xs font-bold text-gray-400 uppercase">Tipo de Gasto</Label>
              <Select value={formData.type || 'combustivel'} onValueChange={(val: any) => setFormData({...formData, type: val})}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-[#161618] border-white/10 text-white">
                  <SelectItem value="combustivel">Abastecimento</SelectItem>
                  <SelectItem value="manutencao">Manutenção</SelectItem>
                  <SelectItem value="rota">Deslocamento/Rota</SelectItem>
                  <SelectItem value="outros">Outros</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <Label className="text-xs font-bold text-gray-400 uppercase">Veículo / Placa</Label>
                <Input 
                  placeholder="Ex: Fiat Strada OQX-1234"
                  value={formData.vehicle || ''}
                  onChange={(e) => setFormData({...formData, vehicle: e.target.value})}
                  className="bg-black/40 border-white/10 text-white"
                />
              </div>
              <div className="space-y-2">
                <Label className="text-xs font-bold text-gray-400 uppercase">Motorista</Label>
                <Input 
                  placeholder="Quem usou?"
                  value={formData.user || ''}
                  onChange={(e) => setFormData({...formData, user: e.target.value})}
                  className="bg-black/40 border-white/10 text-white"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label className="text-xs font-bold text-gray-400 uppercase">Custo Total (R$)</Label>
              <Input 
                type="number" 
                min="0" step="0.01"
                placeholder="Ex: 150.00"
                value={formData.cost || ''}
                onChange={(e) => setFormData({...formData, cost: Number(e.target.value)})}
                className="bg-black/40 border-white/10 text-white text-orange-500 font-bold"
              />
            </div>

            {formData.type === 'rota' && (
              <div className="space-y-3 bg-black/20 p-4 rounded-xl border border-white/5 mt-2">
                <div className="space-y-2">
                  <Label className="text-xs font-bold text-gray-400 uppercase">Endereço de Saída</Label>
                  <Input 
                    placeholder="Ex: Sede da Empresa"
                    value={formData.startAddress || ''}
                    onChange={(e) => setFormData({...formData, startAddress: e.target.value})}
                    className="bg-black/40 border-white/10 text-white text-xs"
                  />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs font-bold text-gray-400 uppercase">Endereço de Chegada</Label>
                  <Input 
                    placeholder="Ex: Obra 20"
                    value={formData.endAddress || ''}
                    onChange={(e) => setFormData({...formData, endAddress: e.target.value})}
                    className="bg-black/40 border-white/10 text-white text-xs"
                  />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs font-bold text-gray-400 uppercase">Combustível Gasto (Litros/Calculado)</Label>
                  <Input 
                    type="number" 
                    placeholder="Ex: 10"
                    value={formData.fuelConsumed || ''}
                    onChange={(e) => setFormData({...formData, fuelConsumed: Number(e.target.value)})}
                    className="bg-black/40 border-white/10 text-white text-xs"
                  />
                </div>
              </div>
            )}

            <div className="space-y-2">
              <Label className="text-xs font-bold text-gray-400 uppercase">Observações Complementares</Label>
              <Input 
                placeholder="Ex: Troca de óleo, peças..."
                value={formData.notes || ''}
                onChange={(e) => setFormData({...formData, notes: e.target.value})}
                className="bg-black/40 border-white/10 text-white"
              />
            </div>

            <Button 
              onClick={handleCreate}
              className="w-full bg-orange-600 hover:bg-orange-700 text-white font-black tracking-tight"
            >
              Adicionar Registro
            </Button>
          </CardContent>
        </Card>

        {/* Tabela de Registros */}
        <Card className="bg-[#161618] border-white/5 shadow-2xl lg:col-span-2 flex flex-col h-[600px]">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <div>
              <CardTitle className="text-lg font-black uppercase tracking-tight text-white mb-1">Histórico de Movimentações</CardTitle>
              <CardDescription className="text-gray-500">Últimos gastos e viagens registradas.</CardDescription>
            </div>
            <div className="bg-black/40 px-4 py-2 border border-white/10 rounded-xl">
              <p className="text-[10px] uppercase text-gray-500 font-bold mb-1">Custo Acumulado</p>
              <p className="text-xl text-orange-500 font-black">
                {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(entries.reduce((acc, curr) => acc + curr.cost, 0))}
              </p>
            </div>
          </CardHeader>
          <CardContent className="flex-1 overflow-auto p-0">
            <Table>
              <TableHeader className="bg-black/40 sticky top-0 backdrop-blur-md">
                <TableRow className="border-white/5">
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 w-12">Tipo</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500">Data e Veículo</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500">Informações</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 text-right">Custo</TableHead>
                  <TableHead className="text-[10px] font-black uppercase text-gray-500 w-12 text-center">Ações</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {entries.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={5} className="text-center py-10">
                      <div className="flex flex-col items-center justify-center opacity-50">
                        <FileText size={32} className="mb-2" />
                        <p className="font-bold text-gray-400">Nenhum registro logístico incluído.</p>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : (
                  entries.map((entry) => (
                    <TableRow key={entry.id} className="border-white/5 hover:bg-white/5">
                      <TableCell className="align-top items-center justify-center">
                        <div className="bg-white/10 p-2 rounded-lg inline-flex" title={entry.type.toUpperCase()}>
                          {getTypeIcon(entry.type)}
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="font-bold text-sm text-gray-200">{entry.vehicle}</div>
                        <div className="flex items-center text-xs text-gray-500 mt-1 gap-2">
                          <Calendar size={12} /> {format(new Date(entry.date), 'dd/MM/yyyy')}
                        </div>
                        <div className="text-[10px] text-gray-400 mt-0.5">Motorista: {entry.user}</div>
                      </TableCell>
                      <TableCell className="align-top max-w-[200px]">
                        {entry.type === 'rota' ? (
                          <div className="text-xs space-y-1">
                            <div className="flex items-start gap-1"><span className="text-gray-500 font-bold w-12 shrink-0">Saída:</span> <span className="text-gray-300 leading-tight">{entry.startAddress}</span></div>
                            <div className="flex items-start gap-1"><span className="text-gray-500 font-bold w-12 shrink-0">Destino:</span> <span className="text-orange-300 leading-tight">{entry.endAddress}</span></div>
                            {entry.fuelConsumed ? <div className="text-[10px] bg-red-500/10 text-red-400 inline-block px-1.5 rounded border border-red-500/20 mt-1">{entry.fuelConsumed} Litros gastos</div> : null}
                          </div>
                        ) : (
                          <p className="text-xs text-gray-300 line-clamp-3 leading-relaxed mt-1">
                            {entry.notes || <span className="italic text-gray-600">Nenhuma observação</span>}
                          </p>
                        )}
                      </TableCell>
                      <TableCell className="text-right font-black text-orange-500 align-top">
                        {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(entry.cost)}
                      </TableCell>
                      <TableCell className="align-top text-center">
                        <Button variant="ghost" size="icon" onClick={() => removeEntry(entry.id)} className="h-8 w-8 text-red-500 hover:text-red-400 hover:bg-red-500/10">
                          <Trash2 size={14} />
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </div>

    </div>
  );
}
