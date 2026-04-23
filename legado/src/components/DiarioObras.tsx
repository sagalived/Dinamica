import React, { useState, useEffect, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'motion/react';
import {
  Plus, X, Upload, Trash2, Edit3, ChevronDown, ChevronUp,
  Image as ImageIcon, FileText, Video, Calendar, User,
  Tag, AlertTriangle, Clock, CheckCircle2, Loader2,
  Kanban as LayoutKanban, Layers, Search, Camera, RefreshCw,
  GripVertical, Flag, Paperclip, ZoomIn
} from 'lucide-react';
import { kanbanApi as api } from '../lib/api';
import { cn } from '../lib/utils';

// ─── Types ────────────────────────────────────────────────────────────────────

type AttachmentType = 'image' | 'video' | 'document';

interface Attachment {
  id: string;
  filename: string;
  originalName: string;
  type: AttachmentType;
  contentType: string;
  url: string;
  uploadedAt: string;
}

type CardStatus = 'planned' | 'in_progress' | 'review' | 'done' | 'blocked';
type CardPriority = 'low' | 'medium' | 'high' | 'critical';

interface KanbanCard {
  id: string;
  sprintId: string;
  buildingId: string;
  title: string;
  description: string;
  status: CardStatus;
  priority: CardPriority;
  responsible: string;
  dueDate: string;
  tags: string[];
  attachments: Attachment[];
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

interface KanbanSprint {
  id: string;
  buildingId: string;
  name: string;
  startDate: string;
  endDate: string;
  color: string;
  createdAt: string;
  updatedAt: string;
  cards: KanbanCard[];
}

// ─── Constants ────────────────────────────────────────────────────────────────

const COLUMNS: { id: CardStatus; label: string; emoji: string; color: string; bg: string }[] = [
  { id: 'planned',     label: 'Planejado',    emoji: '📋', color: 'text-blue-400',   bg: 'bg-blue-500/10 border-blue-500/20' },
  { id: 'in_progress', label: 'Em Execução',  emoji: '🔧', color: 'text-orange-400', bg: 'bg-orange-500/10 border-orange-500/20' },
  { id: 'review',      label: 'Em Revisão',   emoji: '🔍', color: 'text-purple-400', bg: 'bg-purple-500/10 border-purple-500/20' },
  { id: 'done',        label: 'Concluído',    emoji: '✅', color: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-500/20' },
  { id: 'blocked',     label: 'Bloqueado',    emoji: '⛔', color: 'text-red-400',    bg: 'bg-red-500/10 border-red-500/20' },
];

const PRIORITIES: { id: CardPriority; label: string; color: string; dot: string }[] = [
  { id: 'low',      label: 'Baixa',    color: 'text-gray-400',   dot: 'bg-gray-400' },
  { id: 'medium',   label: 'Média',    color: 'text-blue-400',   dot: 'bg-blue-400' },
  { id: 'high',     label: 'Alta',     color: 'text-orange-400', dot: 'bg-orange-400' },
  { id: 'critical', label: 'Crítica',  color: 'text-red-400',    dot: 'bg-red-400' },
];

const SPRINT_COLORS = [
  '#f97316', '#3b82f6', '#8b5cf6', '#10b981', '#ef4444',
  '#06b6d4', '#f59e0b', '#ec4899', '#14b8a6', '#6366f1',
];

// ─── Helper functions ─────────────────────────────────────────────────────────

function formatDate(iso: string) {
  if (!iso) return '';
  try {
    return new Date(iso).toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: '2-digit' });
  } catch { return iso; }
}

function isOverdue(dueDate: string) {
  if (!dueDate) return false;
  return new Date(dueDate) < new Date();
}

function getStatusCol(id: CardStatus) {
  return COLUMNS.find(c => c.id === id) ?? COLUMNS[0];
}

function getPriorityMeta(id: CardPriority) {
  return PRIORITIES.find(p => p.id === id) ?? PRIORITIES[1];
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function AttachmentPreview({ att, onDelete }: { att: Attachment; onDelete: () => void }) {
  const [lightbox, setLightbox] = useState(false);
  const BASE = import.meta.env.VITE_API_BASE ?? '';

  return (
    <>
      <div className="group relative rounded-lg overflow-hidden border border-white/10 bg-black/30">
        {att.type === 'image' ? (
          <div
            className="w-full h-20 cursor-zoom-in relative"
            onClick={() => setLightbox(true)}
          >
            <img
              src={`${BASE}${att.url}`}
              alt={att.originalName}
              className="w-full h-full object-cover hover:scale-105 transition-transform"
            />
            <div className="absolute inset-0 bg-black/0 group-hover:bg-black/20 transition-colors flex items-center justify-center">
              <ZoomIn size={16} className="text-white opacity-0 group-hover:opacity-100 transition-opacity" />
            </div>
          </div>
        ) : att.type === 'video' ? (
          <video src={`${BASE}${att.url}`} className="w-full h-20 object-cover" controls/>
        ) : (
          <a
            href={`${BASE}${att.url}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 p-2 hover:bg-white/5 transition-colors"
          >
            <FileText size={18} className="text-orange-400 shrink-0" />
            <span className="text-xs text-gray-300 truncate">{att.originalName}</span>
          </a>
        )}
        <button
          onClick={onDelete}
          className="absolute top-1 right-1 w-5 h-5 rounded-full bg-black/70 text-red-400 hover:text-red-300 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
        >
          <X size={10} />
        </button>
      </div>
      {/* Lightbox */}
      {lightbox && (
        <div
          className="fixed inset-0 z-[9999] bg-black/95 flex items-center justify-center p-4"
          onClick={() => setLightbox(false)}
        >
          <img
            src={`${BASE}${att.url}`}
            alt={att.originalName}
            className="max-w-full max-h-full object-contain rounded-xl shadow-2xl"
            onClick={e => e.stopPropagation()}
          />
          <button
            className="absolute top-4 right-4 text-white/60 hover:text-white"
            onClick={() => setLightbox(false)}
          >
            <X size={28} />
          </button>
          <p className="absolute bottom-6 text-gray-400 text-sm">{att.originalName}</p>
        </div>
      )}
    </>
  );
}

// ─── Card Modal ───────────────────────────────────────────────────────────────

interface CardModalProps {
  card: Partial<KanbanCard> | null;
  sprintId: string;
  buildingId: string;
  sessionUser: any;
  onClose: () => void;
  onSave: (card: Partial<KanbanCard>) => Promise<void>;
  onDelete?: () => Promise<void>;
  onUpload: (cardId: string, file: File) => Promise<Attachment | null>;
  onDeleteAttachment: (cardId: string, att: Attachment) => Promise<void>;
  isNew: boolean;
}

function CardModal({
  card, sprintId, buildingId, sessionUser, onClose, onSave,
  onDelete, onUpload, onDeleteAttachment, isNew
}: CardModalProps) {
  const [form, setForm] = useState<Partial<KanbanCard>>({
    title: '', description: '', status: 'planned', priority: 'medium',
    responsible: '', dueDate: '', tags: [], attachments: [],
    sprintId, buildingId, createdBy: sessionUser?.name || '',
    ...(card || {}),
  });
  const [tagInput, setTagInput] = useState('');
  const [saving, setSaving] = useState(false);
  const [uploading, setUploading] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  const set = (k: keyof KanbanCard, v: any) => setForm(f => ({ ...f, [k]: v }));

  const handleSave = async () => {
    if (!form.title?.trim()) return;
    setSaving(true);
    try { await onSave(form); onClose(); }
    finally { setSaving(false); }
  };

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file || !card?.id) return;
    setUploading(true);
    try {
      const att = await onUpload(card.id, file);
      if (att) setForm(f => ({ ...f, attachments: [...(f.attachments || []), att] }));
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  const addTag = () => {
    const t = tagInput.trim();
    if (t && !(form.tags || []).includes(t)) {
      set('tags', [...(form.tags || []), t]);
    }
    setTagInput('');
  };

  return (
    <div
      className="fixed inset-0 z-[200] bg-black/85 backdrop-blur-sm flex items-start sm:items-center justify-center p-2 sm:p-4 overflow-y-auto"
      onClick={onClose}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 20 }}
        className="bg-[#111216] border border-white/10 rounded-2xl shadow-2xl w-full max-w-2xl my-auto"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-white/5">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-orange-500/15 border border-orange-500/25 flex items-center justify-center">
              {isNew ? <Plus size={16} className="text-orange-400" /> : <Edit3 size={16} className="text-orange-400" />}
            </div>
            <h2 className="text-lg font-black text-white uppercase tracking-tight">
              {isNew ? 'Nova Demanda' : 'Editar Demanda'}
            </h2>
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-white transition-colors p-2 rounded-lg hover:bg-white/5">
            <X size={18} />
          </button>
        </div>

        <div className="p-5 space-y-5 max-h-[80vh] overflow-y-auto custom-scrollbar">
          {/* Title */}
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500 mb-1.5 block">Título *</label>
            <input
              autoFocus
              value={form.title || ''}
              onChange={e => set('title', e.target.value)}
              placeholder="Descreva a demanda ou tarefa..."
              className="w-full bg-black/40 border border-white/10 rounded-xl px-4 py-2.5 text-white text-sm font-bold placeholder:text-gray-600 focus:outline-none focus:border-orange-500/50 transition-colors"
            />
          </div>

          {/* Description */}
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Descrição</label>
            <textarea
              value={form.description || ''}
              onChange={e => set('description', e.target.value)}
              placeholder="Detalhes, contexto e observações..."
              rows={3}
              className="w-full bg-black/40 border border-white/10 rounded-xl px-4 py-2.5 text-gray-300 text-sm placeholder:text-gray-700 focus:outline-none focus:border-orange-500/50 transition-colors resize-none"
            />
          </div>

          {/* Status + Priority */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Status</label>
              <select
                value={form.status}
                onChange={e => set('status', e.target.value as CardStatus)}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm font-bold focus:outline-none focus:border-orange-500/50 transition-colors"
              >
                {COLUMNS.map(c => <option key={c.id} value={c.id}>{c.emoji} {c.label}</option>)}
              </select>
            </div>
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Prioridade</label>
              <select
                value={form.priority}
                onChange={e => set('priority', e.target.value as CardPriority)}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm font-bold focus:outline-none focus:border-orange-500/50 transition-colors"
              >
                {PRIORITIES.map(p => <option key={p.id} value={p.id}>{p.label}</option>)}
              </select>
            </div>
          </div>

          {/* Responsible + Due Date */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block flex items-center gap-1">
                <User size={10} /> Responsável
              </label>
              <input
                value={form.responsible || ''}
                onChange={e => set('responsible', e.target.value)}
                placeholder="Nome do responsável"
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm placeholder:text-gray-700 focus:outline-none focus:border-orange-500/50 transition-colors"
              />
            </div>
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block flex items-center gap-1">
                <Calendar size={10} /> Prazo
              </label>
              <input
                type="date"
                value={form.dueDate || ''}
                onChange={e => set('dueDate', e.target.value)}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm focus:outline-none focus:border-orange-500/50 transition-colors [color-scheme:dark]"
              />
            </div>
          </div>

          {/* Tags */}
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block flex items-center gap-1">
              <Tag size={10} /> Tags
            </label>
            <div className="flex gap-2">
              <input
                value={tagInput}
                onChange={e => setTagInput(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && (e.preventDefault(), addTag())}
                placeholder="Digite e pressione Enter"
                className="flex-1 bg-black/40 border border-white/10 rounded-xl px-3 py-2 text-white text-sm placeholder:text-gray-700 focus:outline-none focus:border-orange-500/50 transition-colors"
              />
              <button
                onClick={addTag}
                className="px-4 py-2 bg-orange-600/20 text-orange-400 border border-orange-500/20 rounded-xl text-sm font-bold hover:bg-orange-600/30 transition-colors"
              >
                Add
              </button>
            </div>
            {(form.tags || []).length > 0 && (
              <div className="flex flex-wrap gap-2 mt-2">
                {(form.tags || []).map(t => (
                  <span
                    key={t}
                    className="flex items-center gap-1 px-2.5 py-1 bg-orange-500/10 border border-orange-500/20 text-orange-400 text-xs font-bold rounded-full"
                  >
                    {t}
                    <button onClick={() => set('tags', (form.tags || []).filter(x => x !== t))}>
                      <X size={10} />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>

          {/* Attachments (only for existing cards) */}
          {!isNew && card?.id && (
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-2 block flex items-center gap-1">
                <Paperclip size={10} /> Anexos (Fotos, Vídeos, Notas Fiscais)
              </label>
              <div className="grid grid-cols-3 sm:grid-cols-4 gap-2 mb-2">
                {(form.attachments || []).map(att => (
                  <React.Fragment key={att.id}>
                    <AttachmentPreview
                      att={att}
                      onDelete={async () => {
                        await onDeleteAttachment(card.id!, att);
                        setForm(f => ({ ...f, attachments: (f.attachments || []).filter(a => a.id !== att.id) }));
                      }}
                    />
                  </React.Fragment>
                ))}
                <button
                  onClick={() => fileRef.current?.click()}
                  disabled={uploading}
                  className="h-20 rounded-lg border-2 border-dashed border-white/10 hover:border-orange-500/40 flex flex-col items-center justify-center gap-1 text-gray-600 hover:text-orange-400 transition-colors"
                >
                  {uploading ? (
                    <Loader2 size={18} className="animate-spin" />
                  ) : (
                    <>
                      <Upload size={18} />
                      <span className="text-[9px] font-bold uppercase">Upload</span>
                    </>
                  )}
                </button>
              </div>
              <input
                ref={fileRef}
                type="file"
                accept="image/*,video/*,.pdf,.xml,.xlsx"
                className="hidden"
                onChange={handleFile}
              />
              <p className="text-[10px] text-gray-600">JPG, PNG, MP4, PDF, XML, XLSX — max 50 MB</p>
            </div>
          )}
          {isNew && (
            <div className="flex items-center gap-2 p-3 bg-blue-500/5 border border-blue-500/15 rounded-xl">
              <Camera size={14} className="text-blue-400 shrink-0" />
              <p className="text-xs text-blue-400">Após criar a demanda, você poderá adicionar fotos, vídeos e notas fiscais.</p>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-5 border-t border-white/5 gap-3">
          {!isNew && onDelete ? (
            <button
              onClick={async () => { if (confirm('Remover esta demanda?')) { await onDelete(); onClose(); } }}
              className="flex items-center gap-2 px-4 py-2 bg-red-500/10 text-red-400 border border-red-500/20 rounded-xl text-sm font-bold hover:bg-red-500/20 transition-colors"
            >
              <Trash2 size={14} /> Remover
            </button>
          ) : <div />}
          <div className="flex gap-3">
            <button
              onClick={onClose}
              className="px-4 py-2 text-gray-400 hover:text-white text-sm font-bold transition-colors"
            >
              Cancelar
            </button>
            <button
              onClick={handleSave}
              disabled={saving || !form.title?.trim()}
              className="flex items-center gap-2 px-6 py-2 bg-orange-600 hover:bg-orange-700 text-white rounded-xl text-sm font-black transition-colors disabled:opacity-50 disabled:cursor-not-allowed shadow-lg shadow-orange-600/20"
            >
              {saving && <Loader2 size={14} className="animate-spin" />}
              {isNew ? 'Criar Demanda' : 'Salvar Alterações'}
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}

// ─── Kanban Card Component ────────────────────────────────────────────────────

function KanbanCardComp({
  card,
  onClick,
  onDragStart,
}: {
  card: KanbanCard;
  onClick: () => void;
  onDragStart: (e: React.DragEvent) => void;
}) {
  const prio = getPriorityMeta(card.priority);
  const overdue = isOverdue(card.dueDate) && card.status !== 'done';
  const imgAtts = card.attachments.filter(a => a.type === 'image');
  const BASE = import.meta.env.VITE_API_BASE ?? '';

  return (
    <div
      draggable
      onDragStart={onDragStart}
      onClick={onClick}
      className={cn(
        'group relative bg-[#1a1d22] border rounded-xl p-3.5 cursor-pointer',
        'hover:border-orange-500/30 hover:shadow-lg hover:shadow-orange-500/5',
        'transition-all duration-200 select-none',
        overdue ? 'border-red-500/30' : 'border-white/8',
      )}
    >
      {/* Priority dot */}
      <div className="flex items-start justify-between gap-2 mb-2">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className={cn('w-2 h-2 rounded-full shrink-0', prio.dot)} />
          <p className="text-xs font-black text-white leading-tight truncate">{card.title}</p>
        </div>
        <GripVertical size={12} className="text-gray-600 shrink-0 opacity-0 group-hover:opacity-100 transition-opacity mt-0.5" />
      </div>

      {card.description && (
        <p className="text-[11px] text-gray-500 leading-relaxed mb-2.5 line-clamp-2">{card.description}</p>
      )}

      {/* Thumbnail strip */}
      {imgAtts.length > 0 && (
        <div className="flex gap-1 mb-2.5 overflow-hidden rounded-lg">
          {imgAtts.slice(0, 3).map(img => (
            <img
              key={img.id}
              src={`${BASE}${img.url}`}
              alt={img.originalName}
              className="w-12 h-9 object-cover rounded-md flex-shrink-0"
            />
          ))}
          {imgAtts.length > 3 && (
            <div className="w-12 h-9 rounded-md bg-black/40 flex items-center justify-center text-[10px] text-gray-500 font-bold">
              +{imgAtts.length - 3}
            </div>
          )}
        </div>
      )}

      {/* Tags */}
      {card.tags.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2">
          {card.tags.slice(0, 3).map(t => (
            <span key={t} className="px-1.5 py-0.5 bg-orange-500/10 text-orange-400/80 text-[9px] font-bold rounded uppercase tracking-wide">
              {t}
            </span>
          ))}
        </div>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between gap-2 mt-1">
        <div className="flex items-center gap-2 min-w-0">
          {card.responsible && (
            <div className="flex items-center gap-1">
              <div className="w-5 h-5 rounded-full bg-gradient-to-br from-orange-500 to-orange-700 flex items-center justify-center text-white text-[8px] font-black shrink-0">
                {card.responsible.charAt(0).toUpperCase()}
              </div>
              <span className="text-[9px] text-gray-500 truncate max-w-[80px]">{card.responsible}</span>
            </div>
          )}
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {card.attachments.length > 0 && (
            <div className="flex items-center gap-1 text-gray-600">
              <Paperclip size={9} />
              <span className="text-[9px] font-bold">{card.attachments.length}</span>
            </div>
          )}
          {card.dueDate && (
            <div className={cn('flex items-center gap-1', overdue ? 'text-red-400' : 'text-gray-600')}>
              {overdue ? <AlertTriangle size={9} /> : <Clock size={9} />}
              <span className="text-[9px] font-bold">{formatDate(card.dueDate)}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Sprint Modal ─────────────────────────────────────────────────────────────

function SprintModal({
  sprint,
  buildingId,
  onClose,
  onSave,
  onDelete,
  isNew,
}: {
  sprint?: KanbanSprint;
  buildingId: string;
  onClose: () => void;
  onSave: (data: any) => Promise<void>;
  onDelete?: () => Promise<void>;
  isNew: boolean;
}) {
  const [form, setForm] = useState({
    name: sprint?.name || '',
    startDate: sprint?.startDate || '',
    endDate: sprint?.endDate || '',
    color: sprint?.color || SPRINT_COLORS[0],
    buildingId,
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    if (!form.name.trim()) return;
    setSaving(true);
    try { await onSave(form); onClose(); }
    finally { setSaving(false); }
  };

  return (
    <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4" onClick={onClose}>
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.95 }}
        className="bg-[#111216] border border-white/10 rounded-2xl shadow-2xl w-full max-w-md"
        onClick={e => e.stopPropagation()}
      >
        <div className="flex items-center justify-between p-5 border-b border-white/5">
          <h2 className="text-lg font-black text-white uppercase tracking-tight">
            {isNew ? 'Novo Sprint' : 'Editar Sprint'}
          </h2>
          <button onClick={onClose} className="text-gray-500 hover:text-white p-2 rounded-lg hover:bg-white/5">
            <X size={18} />
          </button>
        </div>
        <div className="p-5 space-y-4">
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-orange-500 mb-1.5 block">Nome do Sprint *</label>
            <input
              autoFocus
              value={form.name}
              onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
              placeholder="Ex: Sprint 1 — Fundação"
              className="w-full bg-black/40 border border-white/10 rounded-xl px-4 py-2.5 text-white text-sm font-bold placeholder:text-gray-600 focus:outline-none focus:border-orange-500/50 transition-colors"
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Início</label>
              <input type="date" value={form.startDate} onChange={e => setForm(f => ({ ...f, startDate: e.target.value }))}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm focus:outline-none focus:border-orange-500/50 transition-colors [color-scheme:dark]" />
            </div>
            <div>
              <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Fim</label>
              <input type="date" value={form.endDate} onChange={e => setForm(f => ({ ...f, endDate: e.target.value }))}
                className="w-full bg-black/40 border border-white/10 rounded-xl px-3 py-2.5 text-white text-sm focus:outline-none focus:border-orange-500/50 transition-colors [color-scheme:dark]" />
            </div>
          </div>
          <div>
            <label className="text-[10px] font-black uppercase tracking-widest text-gray-500 mb-1.5 block">Cor</label>
            <div className="flex flex-wrap gap-2">
              {SPRINT_COLORS.map(c => (
                <button
                  key={c}
                  onClick={() => setForm(f => ({ ...f, color: c }))}
                  className={cn('w-8 h-8 rounded-lg transition-all', form.color === c ? 'ring-2 ring-white ring-offset-2 ring-offset-[#111216] scale-110' : 'hover:scale-110')}
                  style={{ backgroundColor: c }}
                />
              ))}
            </div>
          </div>
        </div>
        <div className="flex items-center justify-between p-5 border-t border-white/5">
          {!isNew && onDelete ? (
            <button onClick={async () => { if (confirm('Remover este sprint e todas as suas demandas?')) { await onDelete(); onClose(); } }}
              className="flex items-center gap-2 px-4 py-2 bg-red-500/10 text-red-400 border border-red-500/20 rounded-xl text-sm font-bold hover:bg-red-500/20 transition-colors">
              <Trash2 size={14} /> Remover
            </button>
          ) : <div />}
          <div className="flex gap-3">
            <button onClick={onClose} className="px-4 py-2 text-gray-400 hover:text-white text-sm font-bold transition-colors">Cancelar</button>
            <button onClick={handleSave} disabled={saving || !form.name.trim()}
              className="flex items-center gap-2 px-6 py-2 bg-orange-600 hover:bg-orange-700 text-white rounded-xl text-sm font-black transition-colors disabled:opacity-50 shadow-lg shadow-orange-600/20">
              {saving && <Loader2 size={14} className="animate-spin" />}
              {isNew ? 'Criar Sprint' : 'Salvar'}
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
}

// ─── Main KanbanBoard Component ───────────────────────────────────────────────

type DiarioObrasProps = {
  buildingId: string;
  buildingName: string;
  sessionUser: any;
  buildings?: { id: number | string; name: string }[];
};

export function DiarioObras({ buildingId, buildingName, sessionUser, buildings = [] }: DiarioObrasProps) {
  const [sprints, setSprints] = useState<KanbanSprint[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedSprint, setSelectedSprint] = useState<string | null>(null);
  const [collapsedSprints, setCollapsedSprints] = useState<Set<string>>(new Set());
  const [cardModal, setCardModal] = useState<{ card: Partial<KanbanCard> | null; sprintId: string; isNew: boolean } | null>(null);
  const [sprintModal, setSprintModal] = useState<{ sprint?: KanbanSprint; isNew: boolean } | null>(null);
  const [search, setSearch] = useState('');
  const [filterStatus, setFilterStatus] = useState<CardStatus | 'all'>('all');
  const [dragCard, setDragCard] = useState<{ cardId: string; fromSprintId: string } | null>(null);
  const [dragOver, setDragOver] = useState<CardStatus | null>(null);
  const [activeBuildingId, setActiveBuildingId] = useState(buildingId);
  const [activeBuildingName, setActiveBuildingName] = useState(buildingName);

  // Sync when parent prop changes
  useEffect(() => {
    setActiveBuildingId(buildingId);
    setActiveBuildingName(buildingName);
  }, [buildingId, buildingName]);

  const fetchData = useCallback(async () => {
    if (!activeBuildingId) return;
    setLoading(true);
    try {
      const res = await api.get(`/kanban?building_id=${activeBuildingId}`);
      const data = res.data?.buildings?.[activeBuildingId] || [];
      setSprints(data);
      if (!selectedSprint && data.length > 0) setSelectedSprint(data[0].id);
    } catch {
      setSprints([]);
    } finally {
      setLoading(false);
    }
  }, [activeBuildingId, selectedSprint]);

  useEffect(() => { fetchData(); }, [fetchData]);

  // ── SPRINT CRUD ────────────────────────────────────────────────

  const createSprint = async (data: any) => {
    await api.post('/kanban/sprint', { ...data, buildingId: activeBuildingId });
    await fetchData();
  };

  const updateSprint = async (sprintId: string, data: any) => {
    await api.patch(`/kanban/sprint/${sprintId}`, data);
    await fetchData();
  };

  const deleteSprint = async (sprintId: string) => {
    await api.delete(`/kanban/sprint/${sprintId}`);
    setSprints(s => s.filter(x => x.id !== sprintId));
    if (selectedSprint === sprintId) setSelectedSprint(null);
  };

  // ── CARD CRUD ──────────────────────────────────────────────────

  const createCard = async (data: Partial<KanbanCard>) => {
    await api.post('/kanban/card', { ...data, buildingId: activeBuildingId });
    await fetchData();
  };

  const updateCard = async (cardId: string, data: Partial<KanbanCard>) => {
    await api.patch(`/kanban/card/${cardId}`, data);
    await fetchData();
  };

  const deleteCard = async (cardId: string) => {
    await api.delete(`/kanban/card/${cardId}`);
    setSprints(s => s.map(sp => ({ ...sp, cards: sp.cards.filter(c => c.id !== cardId) })));
  };

  const saveCard = async (data: Partial<KanbanCard>) => {
    if (cardModal?.isNew) {
      await createCard(data);
    } else if (data.id) {
      await updateCard(data.id, data);
    }
  };

  // ── DRAG & DROP ────────────────────────────────────────────────

  const handleDragStart = (e: React.DragEvent, cardId: string, sprintId: string) => {
    setDragCard({ cardId, fromSprintId: sprintId });
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDrop = async (e: React.DragEvent, newStatus: CardStatus) => {
    e.preventDefault();
    setDragOver(null);
    if (!dragCard) return;
    const { cardId } = dragCard;
    setDragCard(null);
    // Optimistic update
    setSprints(s => s.map(sp => ({
      ...sp,
      cards: sp.cards.map(c => c.id === cardId ? { ...c, status: newStatus } : c)
    })));
    try {
      await api.patch(`/kanban/card/${cardId}`, { status: newStatus });
    } catch {
      await fetchData(); // revert
    }
  };

  // ── UPLOAD ─────────────────────────────────────────────────────

  const uploadFile = async (cardId: string, file: File): Promise<Attachment | null> => {
    const form = new FormData();
    form.append('file', file);
    const res = await api.post(`/kanban/upload?card_id=${cardId}`, form, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
    return res.data?.attachment || null;
  };

  const deleteAttachment = async (cardId: string, att: Attachment) => {
    await api.delete(`/kanban/upload?card_id=${cardId}&filename=${att.filename}`);
  };

  // ── Computed ───────────────────────────────────────────────────

  const currentSprint = selectedSprint ? sprints.find(s => s.id === selectedSprint) : null;

  const filteredCards = (sprintCards: KanbanCard[]): KanbanCard[] => {
    return sprintCards.filter(c => {
      const matchSearch = !search || c.title.toLowerCase().includes(search.toLowerCase())
        || c.description.toLowerCase().includes(search.toLowerCase())
        || c.responsible.toLowerCase().includes(search.toLowerCase());
      const matchStatus = filterStatus === 'all' || c.status === filterStatus;
      return matchSearch && matchStatus;
    });
  };

  const totalCards = sprints.reduce((a, s) => a + s.cards.length, 0);
  const doneCards = sprints.reduce((a, s) => a + s.cards.filter(c => c.status === 'done').length, 0);
  const progressPct = totalCards > 0 ? Math.round((doneCards / totalCards) * 100) : 0;

  return (
    <div className="flex flex-col gap-6 w-full pb-10">
      {/* ── Header ── */}
      <div className="bg-[#161618] rounded-2xl border border-white/5 shadow-xl p-5 sm:p-6">
        <div className="flex flex-col sm:flex-row sm:items-start justify-between gap-4">
          <div className="flex items-center gap-4">
            <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-orange-500 to-orange-700 flex items-center justify-center shadow-lg shadow-orange-500/20 shrink-0">
              <LayoutKanban size={22} className="text-white" />
            </div>
            <div>
              <h1 className="text-xl font-black uppercase tracking-tight text-white">Kanban de Obras</h1>
              <p className="text-xs text-orange-500 font-bold mt-0.5 tracking-wide">{activeBuildingName}</p>
            </div>
          </div>

          {/* Building selector if multiple available */}
          {buildings.length > 1 && (
            <select
              value={activeBuildingId}
              onChange={e => {
                const b = buildings.find(x => String(x.id) === e.target.value);
                setActiveBuildingId(e.target.value);
                setActiveBuildingName(b?.name || '');
                setSelectedSprint(null);
              }}
              className="bg-black/40 border border-white/10 rounded-xl px-3 py-2 text-white text-sm font-bold focus:outline-none focus:border-orange-500/50 transition-colors max-w-xs"
            >
              {buildings.map(b => (
                <option key={b.id} value={String(b.id)}>{b.name}</option>
              ))}
            </select>
          )}
        </div>

        {/* Progress bar */}
        {totalCards > 0 && (
          <div className="mt-5 space-y-1.5">
            <div className="flex items-center justify-between text-xs font-bold">
              <span className="text-gray-400">{doneCards}/{totalCards} demandas concluídas</span>
              <span className="text-orange-500">{progressPct}%</span>
            </div>
            <div className="h-2 bg-white/5 rounded-full overflow-hidden">
              <div
                className="h-full bg-gradient-to-r from-orange-500 to-orange-400 rounded-full transition-all duration-500"
                style={{ width: `${progressPct}%` }}
              />
            </div>
          </div>
        )}

        {/* Summary chips */}
        <div className="flex flex-wrap gap-2 mt-4">
          {COLUMNS.map(col => {
            const count = sprints.reduce((a, s) => a + s.cards.filter(c => c.status === col.id).length, 0);
            if (count === 0) return null;
            return (
              <button
                key={col.id}
                onClick={() => setFilterStatus(f => f === col.id ? 'all' : col.id)}
                className={cn(
                  'flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[10px] font-black uppercase tracking-wide border transition-all',
                  filterStatus === col.id ? col.bg : 'bg-white/5 border-white/5 text-gray-500 hover:bg-white/10'
                )}
              >
                <span>{col.emoji}</span>
                <span className={filterStatus === col.id ? col.color : ''}>{col.label}</span>
                <span className="bg-white/10 px-1.5 py-0.5 rounded-full">{count}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Sprint Tabs + Actions ── */}
      <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-center">
        <div className="flex-1 flex items-center gap-2 overflow-x-auto pb-1 hide-scrollbar">
          {sprints.map(sprint => (
            <button
              key={sprint.id}
              onClick={() => setSelectedSprint(sprint.id)}
              className={cn(
                'flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-black whitespace-nowrap transition-all shrink-0 border',
                selectedSprint === sprint.id
                  ? 'text-white border-transparent shadow-lg'
                  : 'bg-[#161618] text-gray-500 border-white/5 hover:text-white hover:border-white/10'
              )}
              style={selectedSprint === sprint.id ? { backgroundColor: sprint.color + '22', borderColor: sprint.color + '55', color: sprint.color } : {}}
            >
              <Layers size={14} />
              {sprint.name}
              <span className="text-[10px] bg-white/10 px-1.5 py-0.5 rounded-full text-gray-400">{sprint.cards.length}</span>
            </button>
          ))}
          <button
            onClick={() => setSprintModal({ isNew: true })}
            className="flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-black text-gray-500 border border-dashed border-white/10 hover:border-orange-500/30 hover:text-orange-400 transition-all shrink-0"
          >
            <Plus size={14} /> Novo Sprint
          </button>
        </div>

        {/* Tools */}
        <div className="flex items-center gap-2 shrink-0">
          <div className="relative">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-600" />
            <input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Buscar demanda..."
              className="bg-[#161618] border border-white/5 rounded-xl pl-9 pr-3 py-2 text-white text-xs placeholder:text-gray-600 focus:outline-none focus:border-orange-500/30 w-44 transition-colors"
            />
          </div>
          <button
            onClick={fetchData}
            disabled={loading}
            className="w-9 h-9 rounded-xl bg-[#161618] border border-white/5 flex items-center justify-center text-gray-500 hover:text-orange-400 hover:border-orange-500/20 transition-all"
          >
            <RefreshCw size={14} className={cn(loading && 'animate-spin')} />
          </button>
          {currentSprint && (
            <button
              onClick={() => setSprintModal({ sprint: currentSprint, isNew: false })}
              className="w-9 h-9 rounded-xl bg-[#161618] border border-white/5 flex items-center justify-center text-gray-500 hover:text-orange-400 hover:border-orange-500/20 transition-all"
            >
              <Edit3 size={14} />
            </button>
          )}
        </div>
      </div>

      {/* ── Kanban Board ── */}
      {loading ? (
        <div className="flex items-center justify-center py-20 gap-3">
          <Loader2 size={24} className="animate-spin text-orange-500" />
          <span className="text-gray-500 font-bold text-sm">Carregando...</span>
        </div>
      ) : sprints.length === 0 ? (
        <div className="flex flex-col items-center justify-center py-20 gap-4 bg-[#161618] rounded-2xl border border-dashed border-white/10">
          <LayoutKanban size={48} className="text-orange-500/30" />
          <div className="text-center">
            <h3 className="text-white font-black text-lg">Nenhum sprint criado</h3>
            <p className="text-gray-500 text-sm mt-1">Crie um sprint para começar a acompanhar as demandas desta obra.</p>
          </div>
          <button
            onClick={() => setSprintModal({ isNew: true })}
            className="flex items-center gap-2 px-6 py-2.5 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-black text-sm transition-colors shadow-lg shadow-orange-600/20"
          >
            <Plus size={16} /> Criar Primeiro Sprint
          </button>
        </div>
      ) : currentSprint ? (
        <div className="overflow-x-auto pb-4">
          <div className="flex gap-4 min-w-max">
            {COLUMNS.map(col => {
              const cards = filteredCards(currentSprint.cards).filter(c => c.status === col.id);
              const isDragTarget = dragOver === col.id;

              return (
                <div
                  key={col.id}
                  className={cn(
                    'flex flex-col w-72 rounded-2xl border transition-all duration-200',
                    isDragTarget ? col.bg + ' scale-[1.01]' : 'bg-[#0f1012] border-white/5'
                  )}
                  onDragOver={e => { e.preventDefault(); setDragOver(col.id); }}
                  onDragLeave={() => setDragOver(null)}
                  onDrop={e => handleDrop(e, col.id)}
                >
                  {/* Column Header */}
                  <div className="flex items-center justify-between px-4 py-3 border-b border-white/5">
                    <div className="flex items-center gap-2">
                      <span className="text-lg">{col.emoji}</span>
                      <span className={cn('text-xs font-black uppercase tracking-wider', col.color)}>{col.label}</span>
                      <span className="text-[10px] bg-white/10 px-1.5 py-0.5 rounded-full text-gray-500 font-bold">{cards.length}</span>
                    </div>
                    <button
                      onClick={() => setCardModal({ card: null, sprintId: currentSprint.id, isNew: true })}
                      className={cn('w-6 h-6 rounded-lg flex items-center justify-center transition-colors opacity-0 hover:opacity-100 group-hover:opacity-100',
                        col.id === 'planned' ? 'opacity-100 bg-orange-500/15 text-orange-400 hover:bg-orange-500/25' : 'bg-white/5 text-gray-600 hover:bg-white/10 hover:text-white'
                      )}
                      title="Adicionar demanda"
                    >
                      <Plus size={12} />
                    </button>
                  </div>

                  {/* Cards */}
                  <div className="flex flex-col gap-2.5 p-3 flex-1 min-h-[120px]">
                    <AnimatePresence>
                      {cards.map(card => (
                        <motion.div
                          key={card.id}
                          layout
                          initial={{ opacity: 0, y: 10 }}
                          animate={{ opacity: 1, y: 0 }}
                          exit={{ opacity: 0, scale: 0.95 }}
                        >
                          <KanbanCardComp
                            card={card}
                            onDragStart={e => handleDragStart(e, card.id, currentSprint.id)}
                            onClick={() => setCardModal({ card, sprintId: currentSprint.id, isNew: false })}
                          />
                        </motion.div>
                      ))}
                    </AnimatePresence>

                    {/* Add card shortcut */}
                    <button
                      onClick={() => setCardModal({ card: { status: col.id }, sprintId: currentSprint.id, isNew: true })}
                      className="flex items-center gap-2 px-3 py-2 rounded-xl border border-dashed border-white/5 text-gray-700 hover:text-gray-400 hover:border-white/15 transition-all text-xs font-bold"
                    >
                      <Plus size={12} /> Adicionar
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="flex items-center justify-center py-10 text-gray-600 font-bold text-sm gap-2">
          <Layers size={20} />
          Selecione um sprint para ver o Kanban
        </div>
      )}

      {/* ── Modals ── */}
      <AnimatePresence>
        {sprintModal && (
          <SprintModal
            sprint={sprintModal.sprint}
            buildingId={activeBuildingId}
            isNew={sprintModal.isNew}
            onClose={() => setSprintModal(null)}
            onSave={sprintModal.isNew ? createSprint : async (data) => { if (sprintModal.sprint) await updateSprint(sprintModal.sprint.id, data); }}
            onDelete={sprintModal.sprint ? async () => { await deleteSprint(sprintModal.sprint!.id); } : undefined}
          />
        )}
        {cardModal && (
          <CardModal
            card={cardModal.card}
            sprintId={cardModal.sprintId}
            buildingId={activeBuildingId}
            sessionUser={sessionUser}
            isNew={cardModal.isNew}
            onClose={() => setCardModal(null)}
            onSave={saveCard}
            onDelete={cardModal.card?.id ? async () => { await deleteCard(cardModal.card!.id!); } : undefined}
            onUpload={uploadFile}
            onDeleteAttachment={deleteAttachment}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
