import { useState, useEffect, useRef } from 'react';
import { api } from '../lib/api';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card';
import { Button } from './ui/button';
import { Plus, ChevronLeft, ChevronRight, Link as LinkIcon, Trash2, Camera, UploadCloud, History } from 'lucide-react';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { format } from 'date-fns';

interface KanbanTask {
  id: number;
  building_id: string;
  title: string;
  description: string;
  status: 'todo' | 'in_progress_1' | 'in_progress_2' | 'done';
  progress_pct: number;
  drive_link: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

const COLUMNS = [
  { id: 'todo', label: 'Início', defaultPct: 0 },
  { id: 'in_progress_1', label: 'Andamento 1', defaultPct: 30 },
  { id: 'in_progress_2', label: 'Andamento 2', defaultPct: 70 },
  { id: 'done', label: 'Finalizada', defaultPct: 100 }
] as const;

export function DiarioObras({ buildingId, buildingName, sessionUser }: { buildingId: string, buildingName: string, sessionUser: any }) {
  const [tasks, setTasks] = useState<KanbanTask[]>([]);
  const [globalTasks, setGlobalTasks] = useState<KanbanTask[]>([]);
  const [loading, setLoading] = useState(false);
  const [uploadingId, setUploadingId] = useState<number | null>(null);
  
  const [selectedGlobalObra, setSelectedGlobalObra] = useState<string>('all');

  useEffect(() => {
    fetchTasks();
    fetchGlobalTasks();
  }, [buildingId]);

  const fetchTasks = async () => {
    if (!buildingId) return;
    setLoading(true);
    try {
      const res = await api.get(`/sienge/obras/${buildingId === 'all' ? '123' /* fallback for UI consistency */ : buildingId}/kanban`);
      if (res.data?.results) setTasks(res.data.results);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const fetchGlobalTasks = async () => {
    try {
      const res = await api.get(`/sienge/obras/all/kanban`);
      if (res.data?.results) setGlobalTasks(res.data.results);
    } catch (e) {
      console.error(e);
    }
  };

  const createTask = async () => {
    const title = prompt("Digite o título da Tarefa da Obra:");
    if (!title?.trim()) return;
    try {
      await api.post(`/sienge/obras/${buildingId}/kanban`, {
        title, description: '', status: 'todo', progress_pct: 0, drive_link: '', created_by: sessionUser?.name || 'Engenheiro'
      });
      fetchTasks();
      fetchGlobalTasks();
    } catch (e) { console.error(e); }
  };

  const updateTask = async (taskId: number, updates: Partial<KanbanTask>) => {
    try {
      await api.put(`/sienge/obras/kanban/${taskId}`, updates);
      fetchTasks();
      fetchGlobalTasks();
    } catch (e) { console.error(e); }
  };

  const deleteTask = async (taskId: number) => {
    if (!confirm('Excluir tarefa do kanban?')) return;
    try {
      await api.delete(`/sienge/obras/kanban/${taskId}`);
      fetchTasks();
      fetchGlobalTasks();
    } catch (e) { console.error(e); }
  };

  const moveTask = (task: KanbanTask, direction: 'forward' | 'backward') => {
    const currentIndex = COLUMNS.findIndex(c => c.id === task.status);
    let newIndex = currentIndex;
    
    if (direction === 'forward' && currentIndex < COLUMNS.length - 1) newIndex = currentIndex + 1;
    else if (direction === 'backward' && currentIndex > 0) newIndex = currentIndex - 1;

    if (newIndex !== currentIndex) {
      updateTask(task.id, { status: COLUMNS[newIndex].id, progress_pct: COLUMNS[newIndex].defaultPct });
    }
  };

  const handleFileUpload = async (taskId: number, file: File) => {
    setUploadingId(taskId);
    try {
      const formData = new FormData();
      formData.append('file', file);
      await api.post(`/sienge/obras/kanban/${taskId}/upload`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      fetchTasks();
      fetchGlobalTasks();
    } catch (e) {
      alert("Erro ao subir arquivo/foto.");
      console.error(e);
    } finally {
      setUploadingId(null);
    }
  };

  const getAttachments = (linkStr: string) => {
    if (!linkStr) return [];
    return linkStr.split(',').filter(s => s.trim() !== '');
  };

  const filteredHistory = selectedGlobalObra === 'all' 
    ? globalTasks.filter(t => t.drive_link && t.drive_link.length > 5) 
    : globalTasks.filter(t => t.building_id === selectedGlobalObra && t.drive_link && t.drive_link.length > 5);

  return (
    <div className="flex flex-col gap-6 w-full h-full pb-10">
      
      {/* Top Header Buttons (Mirroring the design) */}
      <div className="flex items-center gap-4 border-b border-white/5 pb-4">
         <Button className="bg-transparent border border-white/20 text-white rounded-none px-8 font-bold hover:bg-white/5 tracking-widest text-xs">
           Sprint
         </Button>
         <Button onClick={createTask} className="bg-transparent border border-white/20 text-white rounded-none px-8 font-bold hover:bg-white/5 tracking-widest text-xs">
           nova Sprint
         </Button>
         <Button className="bg-transparent border border-white/20 text-white rounded-none px-8 font-bold hover:bg-white/5 tracking-widest text-xs">
           Obra +
         </Button>
      </div>

      <div className="flex items-center justify-between mt-2 mb-2">
        <h2 className="text-xl font-bold text-orange-500 tracking-widest uppercase">Diário Kanban: {buildingName}</h2>
      </div>

      {loading ? (
        <div className="text-center p-8 text-gray-500">Carregando Fluxos de Obras...</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 h-auto items-start">
          {COLUMNS.map((col) => (
            <div key={col.id} className="flex flex-col border border-white/20 rounded-2xl p-0 min-h-[400px] overflow-hidden">
              <div className="flex items-center justify-between bg-black/40 border-b border-white/20 px-4 py-3">
                <h3 className="font-bold text-white uppercase tracking-widest text-xs">{col.label}</h3>
                <span className="text-xs bg-white/10 px-2 py-0.5 rounded-full text-gray-400">
                  {tasks.filter(t => t.status === col.id).length}
                </span>
              </div>
              
              <div className="flex flex-col gap-3 p-4">
                {tasks.filter(t => t.status === col.id).map(task => {
                  const attachments = getAttachments(task.drive_link);
                  return (
                  <Card key={task.id} className="bg-[#1A1A1D] border-white/5 shadow-xl p-3 group hover:border-orange-500/50 transition-all rounded-xl relative">
                    {uploadingId === task.id && <div className="absolute inset-0 bg-black/60 rounded-xl z-10 flex items-center justify-center font-bold text-emerald-400">Enviando...</div>}
                    <div className="flex justify-between items-start mb-2">
                      <h4 className="font-bold text-white text-[13px] leading-tight">{task.title}</h4>
                      <button onClick={() => deleteTask(task.id)} className="text-red-500/50 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity">
                        <Trash2 size={12} />
                      </button>
                    </div>
                    
                    <div className="mt-3 text-[10px] text-gray-500 flex flex-col gap-3">
                      <div className="flex justify-between w-full items-center">
                        <span className="uppercase tracking-widest font-bold">% Concluído</span>
                        <input 
                          type="number" 
                          className="w-14 bg-black/50 border border-white/10 text-white rounded px-2 py-1 text-right"
                          value={task.progress_pct || 0}
                          onChange={(e) => updateTask(task.id, { progress_pct: parseInt(e.target.value) || 0 })}
                        />
                      </div>
                      
                      <div className="flex flex-col gap-2 p-2 bg-black/30 rounded-lg border border-white/5">
                        <span className="font-bold text-white flex items-center gap-1 uppercase tracking-widest"><Camera size={12}/> Anexar Mídia</span>
                        <div className="flex items-center gap-2 w-full mt-1">
                          <label className="flex-1 shrink-0 bg-orange-600 hover:bg-orange-700 text-white text-center py-1.5 rounded cursor-pointer transition-colors font-bold tracking-tight shadow flex justify-center items-center gap-1">
                             <UploadCloud size={12} /> Câmera / Arquivo
                             <input type="file" className="hidden" accept="image/*,video/*" capture="environment" onChange={(e) => e.target.files?.[0] && handleFileUpload(task.id, e.target.files[0])} />
                          </label>
                        </div>
                        {attachments.length > 0 && (
                          <div className="mt-2 text-emerald-400 font-bold">{attachments.length} anexo(s) salvo(s)</div>
                        )}
                      </div>

                      <div className="pt-2 mt-1 flex justify-between uppercase font-bold tracking-tight text-gray-600 border-t border-white/5">
                        <span>Por: {task.created_by.split(' ')[0]}</span>
                        <span>{task.created_at ? format(new Date(task.created_at), 'dd/MM') : ''}</span>
                      </div>
                    </div>

                    <div className="flex justify-between items-center mt-3">
                      <Button variant="ghost" size="sm" className="h-6 px-2 text-gray-500 hover:text-white text-[10px] uppercase font-bold" disabled={task.status === 'todo'} onClick={() => moveTask(task, 'backward')}>
                        <ChevronLeft size={12} /> Recuar
                      </Button>
                      <Button variant="ghost" size="sm" className="h-6 px-2 text-orange-500 hover:text-orange-400 text-[10px] uppercase font-bold" disabled={task.status === 'done'} onClick={() => moveTask(task, 'forward')}>
                        Avançar <ChevronRight size={12} />
                      </Button>
                    </div>
                  </Card>
                )})}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Selector and Main Block */}
      <div className="mt-8 flex flex-col gap-4">
        <div className="bg-black/60 border border-white/20 rounded-xl p-4 flex items-center justify-between w-full max-w-2xl">
          <h3 className="text-white font-bold tracking-widest uppercase text-sm w-48">Obras em andamento:</h3>
          <select 
            value={selectedGlobalObra}
            onChange={(e) => setSelectedGlobalObra(e.target.value)}
            className="flex-1 bg-black text-white border border-white/20 rounded-md p-2 h-10 outline-none focus:border-orange-500 uppercase text-xs font-bold tracking-tight"
          >
            <option value="all">Ver Feed de Todas as Obras</option>
            {Array.from(new Set(globalTasks.map(t => t.building_id))).map(b => (
              <option key={b} value={b}>Filtrar Obra: {b}</option>
            ))}
          </select>
        </div>

        {/* Global History Board */}
        <div className="bg-[#111] border border-white/20 rounded-3xl min-h-[400px] p-6 relative flex flex-col overflow-hidden shadow-2xl">
           <div className="flex items-center gap-3 mb-6 pb-4 border-b border-white/10">
              <History className="text-orange-500" size={24} />
              <h2 className="text-white text-xl font-black uppercase tracking-tight">Histórico Visual das Atualizações</h2>
           </div>

           <div className="flex-1 overflow-auto grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {filteredHistory.length === 0 ? (
                <div className="col-span-full text-center text-gray-500 uppercase tracking-widest font-bold flex flex-col items-center justify-center p-20 gap-4">
                  <Camera size={48} className="opacity-20" />
                  Nenhum upload de foto ou documento registrado nesta seleção.
                </div>
              ) : (
                filteredHistory.sort((a,b) => new Date(b.updated_at).getTime() - new Date(a.updated_at).getTime()).map((task) => {
                  const items = getAttachments(task.drive_link);
                  return (
                    <Card key={`hist-${task.id}`} className="bg-black/40 border border-white/10 overflow-hidden group">
                      {items.length > 0 && items[items.length - 1].match(/\.(jpg|jpeg|png|gif|webp)$/i) ? (
                        <div className="h-40 w-full overflow-hidden bg-black flex items-center justify-center relative">
                          <img src={items[items.length - 1]} alt="Preview" className="w-full h-full object-cover opacity-80 group-hover:opacity-100 transition-opacity group-hover:scale-105 duration-500" />
                          <div className="absolute top-2 right-2 bg-emerald-600 text-white text-[9px] px-2 py-0.5 rounded font-black tracking-widest">
                            {task.progress_pct}%
                          </div>
                        </div>
                      ) : (
                        <div className="h-40 w-full bg-gradient-to-br from-white/5 to-black flex items-center justify-center">
                          <LinkIcon className="text-gray-600 mb-2" size={32} />
                        </div>
                      )}
                      
                      <CardContent className="p-4 relative">
                        <p className="text-xs text-orange-500 font-black uppercase tracking-widest mb-1 truncate">{task.title}</p>
                        <p className="text-[10px] text-gray-400 leading-tight">Atualizado por {task.created_by} em {format(new Date(task.updated_at), 'dd/MM/yyyy HH:mm')}</p>
                        
                        <div className="flex flex-wrap gap-2 mt-4">
                          {items.map((url, i) => (
                             <a key={i} href={url} target="_blank" rel="noopener noreferrer" className="bg-white/10 hover:bg-white/20 text-white text-[10px] px-2 py-1 rounded font-bold transition-colors">
                               Anexo {i + 1}
                             </a>
                          ))}
                        </div>
                      </CardContent>
                    </Card>
                  );
                })
              )}
           </div>
        </div>
      </div>

    </div>
  );
}
