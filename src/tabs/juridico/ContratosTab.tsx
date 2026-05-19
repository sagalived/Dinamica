import React, { useEffect, useMemo, useRef, useState } from 'react';

import { Plus, Trash2, Upload, Printer, Loader2, Search, Check, ChevronsUpDown, RefreshCw } from 'lucide-react';

import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Label } from '../../components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../components/ui/select';
import { Popover, PopoverContent, PopoverTrigger } from '../../components/ui/popover';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../components/ui/table';
import { cn } from '../../lib/utils';
import { listCatalogBuildings, listCatalogCompanies, createCatalogBuilding, type CatalogBuilding, type CatalogCompany } from './catalogService';

import {
  createContract,
  deleteContract,
  deleteContractDocument,
  getPrintableContractHtml,
  listContractDocuments,
  listContracts,
  syncContractsFromSiengeLive,
  uploadContractDocument,
  type ContractPayload,
} from './contractsService';

function kindLabel(kind: string | undefined | null): string {
  const k = String(kind || '').toLowerCase();
  if (k === 'suprimentos') return 'Suprimentos';
  if (k === 'medicoes' || k === 'medições') return 'Medições';
  if (k === 'licitacao' || k === 'licitação') return 'Licitação';
  return 'Privado';
}

function BuildingPicker({
  value,
  buildings,
  onChange,
  onNewBuilding,
  disabled,
}: {
  value: string;
  buildings: CatalogBuilding[];
  onChange: (value: string) => void;
  onNewBuilding: () => void;
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState('');

  const selected = useMemo(() => {
    return (buildings || []).find((b) => String(b.id) === String(value)) || null;
  }, [buildings, value]);

  const filtered = useMemo(() => {
    const t = filter.trim().toLowerCase();
    if (!t) return buildings || [];
    return (buildings || []).filter((b) => String(b.name || '').toLowerCase().includes(t));
  }, [buildings, filter]);

  const triggerLabel = selected ? selected.name : value === '' ? 'Todas' : 'Selecione';

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          disabled={disabled}
          className="w-full justify-between bg-black/40 border-white/10 text-white hover:bg-white/5"
        >
          <span className="truncate">{triggerLabel}</span>
          <ChevronsUpDown size={14} className="ml-2 shrink-0 text-slate-400" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[--radix-popover-trigger-width] p-0 border-white/10 bg-[#111216]">
        <div className="p-2 border-b border-white/10">
          <Input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Buscar obra..."
            className="bg-black/40 border-white/10 text-white"
          />
        </div>
        <div className="max-h-64 overflow-auto p-1">
          <button
            type="button"
            onClick={() => {
              onChange('');
              setOpen(false);
            }}
            className={cn(
              'w-full flex items-center gap-2 rounded-lg px-2 py-2 text-left text-sm text-slate-200 hover:bg-white/[0.03]',
              value === '' ? 'bg-white/[0.04]' : ''
            )}
          >
            <Check size={14} className={cn('text-[#4CB232]', value === '' ? 'opacity-100' : 'opacity-0')} />
            <span className="truncate">Todas</span>
          </button>
          {(filtered || []).map((b) => {
            const isSelected = String(value) === String(b.id);
            return (
              <button
                key={String(b.id)}
                type="button"
                onClick={() => {
                  onChange(String(b.id));
                  setOpen(false);
                }}
                className={cn(
                  'w-full flex items-center gap-2 rounded-lg px-2 py-2 text-left text-sm text-slate-200 hover:bg-white/[0.03]',
                  isSelected ? 'bg-white/[0.04]' : ''
                )}
              >
                <Check size={14} className={cn('text-[#4CB232]', isSelected ? 'opacity-100' : 'opacity-0')} />
                <span className="truncate">{b.name}</span>
              </button>
            );
          })}
        </div>
        <div className="p-2 border-t border-white/10">
          <Button
            type="button"
            variant="outline"
            onClick={() => {
              setOpen(false);
              onNewBuilding();
            }}
            className="w-full border-white/10 bg-white/[0.02] text-white hover:bg-white/5"
          >
            <Plus size={14} className="mr-2" /> Nova obra
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
}

function ContractModal({
  open,
  buildings,
  companies,
  buildingId,
  onClose,
  onCreated,
  onOpenNewBuilding,
}: {
  open: boolean;
  buildings: CatalogBuilding[];
  companies: CatalogCompany[];
  buildingId: number | null;
  onClose: () => void;
  onCreated: () => Promise<void>;
  onOpenNewBuilding: () => void;
}) {
  const [saving, setSaving] = useState(false);
  const [form, setForm] = useState<ContractPayload>({
    buildingId: buildingId || 0,
    title: '',
    contractNumber: '',
    kind: 'privado',
    status: 'draft',
    supplierLegalName: '',
    supplierCnpj: '',
    objectText: '',
  });

  useEffect(() => {
    setForm((prev) => ({ ...prev, buildingId: buildingId || prev.buildingId }));
  }, [buildingId]);

  if (!open) return null;

  const handleSave = async () => {
    if (!form.title.trim() || !form.buildingId) return;
    setSaving(true);
    try {
      await createContract({
        ...form,
        contractNumber: String(form.contractNumber || '').trim() || null,
        supplierLegalName: String(form.supplierLegalName || '').trim() || null,
        supplierCnpj: String(form.supplierCnpj || '').trim() || null,
        objectText: String(form.objectText || '').trim() || null,
      });
      await onCreated();
      onClose();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[200] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4" onClick={onClose}>
      <div className="w-full max-w-2xl rounded-2xl border border-white/10 bg-[#111216] shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between gap-4 border-b border-white/5 p-5">
          <div>
            <div className="text-[11px] uppercase tracking-[0.24em] text-[#4CB232] font-black">Juridico</div>
            <div className="mt-1 text-lg font-black text-white">Novo Contrato</div>
          </div>
          <Button variant="ghost" onClick={onClose}>Fechar</Button>
        </div>

        <div className="p-5 space-y-4">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Obra *</Label>
              <div className="flex items-center gap-2">
                <div className="flex-1">
                  <Select value={String(form.buildingId || '')} onValueChange={(v) => setForm((p) => ({ ...p, buildingId: Number(v) }))}>
                    <SelectTrigger className="bg-black/40 border-white/10 text-white">
                      <SelectValue placeholder="Selecione" />
                    </SelectTrigger>
                    <SelectContent>
                      {(buildings || []).map((b) => (
                        <SelectItem key={String(b.id)} value={String(b.id)}>{b.name}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  onClick={onOpenNewBuilding}
                  className="border-white/10 bg-white/[0.02] text-white hover:bg-white/5"
                >
                  Nova obra
                </Button>
              </div>
            </div>

            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Tipo</Label>
              <Select value={String(form.kind || 'privado')} onValueChange={(v) => setForm((p) => ({ ...p, kind: v }))}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue placeholder="Selecione" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="privado">Privado</SelectItem>
                  <SelectItem value="licitacao">Licitação</SelectItem>
                  <SelectItem value="suprimentos">Suprimentos</SelectItem>
                  <SelectItem value="medicoes">Medições</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Número</Label>
              <Input
                value={String(form.contractNumber || '')}
                onChange={(e) => setForm((p) => ({ ...p, contractNumber: e.target.value }))}
                placeholder="Ex: CCV-2026-001"
                className="bg-black/40 border-white/10 text-white"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Status</Label>
              <Select value={String(form.status || 'draft')} onValueChange={(v) => setForm((p) => ({ ...p, status: v }))}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="draft">Draft</SelectItem>
                  <SelectItem value="active">Ativo</SelectItem>
                  <SelectItem value="closed">Fechado</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Título *</Label>
            <Input
              value={form.title}
              onChange={(e) => setForm((p) => ({ ...p, title: e.target.value }))}
              placeholder="Ex: Contrato de empreitada - Fundação"
              className="bg-black/40 border-white/10 text-white"
            />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Fornecedor (Razão social)</Label>
              <Input
                value={String(form.supplierLegalName || '')}
                onChange={(e) => setForm((p) => ({ ...p, supplierLegalName: e.target.value }))}
                placeholder="Ex: ABC Engenharia LTDA"
                className="bg-black/40 border-white/10 text-white"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">CNPJ</Label>
              <Input
                value={String(form.supplierCnpj || '')}
                onChange={(e) => setForm((p) => ({ ...p, supplierCnpj: e.target.value }))}
                placeholder="00.000.000/0000-00"
                className="bg-black/40 border-white/10 text-white"
              />
            </div>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Objeto</Label>
            <textarea
              value={String(form.objectText || '')}
              onChange={(e) => setForm((p) => ({ ...p, objectText: e.target.value }))}
              placeholder="Descreva o objeto contratual..."
              className="w-full min-h-[90px] bg-black/40 border border-white/10 rounded-xl px-4 py-2.5 text-white text-sm placeholder:text-slate-600 focus:outline-none focus:border-[#4CB232]/50"
            />
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 border-t border-white/5 p-5">
          <Button variant="ghost" onClick={onClose}>Cancelar</Button>
          <Button
            onClick={handleSave}
            disabled={saving || !form.title.trim() || !form.buildingId}
            className="bg-[#4CB232] hover:bg-[#3f9b2a] text-white"
          >
            {saving ? <Loader2 size={14} className="animate-spin mr-2" /> : null}
            Criar
          </Button>
        </div>
      </div>
    </div>
  );
}

function DocumentUploader({
  contractId,
  onUploaded,
}: {
  contractId: number;
  onUploaded: () => Promise<void>;
}) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  const [docType, setDocType] = useState('contrato');

  const handleFile = async (file: File | null) => {
    if (!file) return;
    setUploading(true);
    try {
      await uploadContractDocument(contractId, file, { docType });
      await onUploaded();
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = '';
    }
  };

  return (
    <div className="flex items-end gap-3">
      <div className="space-y-2">
        <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Tipo</Label>
        <Select value={docType} onValueChange={setDocType}>
          <SelectTrigger className="bg-black/40 border-white/10 text-white h-10 w-[200px]">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="contrato">Contrato</SelectItem>
            <SelectItem value="aditivo">Aditivo</SelectItem>
            <SelectItem value="art_rrt">ART/RRT</SelectItem>
            <SelectItem value="licenca">Licença</SelectItem>
            <SelectItem value="seguro">Seguro</SelectItem>
            <SelectItem value="nf">Nota Fiscal</SelectItem>
            <SelectItem value="projeto">Projeto</SelectItem>
            <SelectItem value="foto">Foto</SelectItem>
            <SelectItem value="outros">Outros</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex-1">
        <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Arquivo</Label>
        <div className="mt-2 flex items-center gap-2">
          <input
            ref={fileRef}
            type="file"
            className="hidden"
            onChange={(e) => handleFile(e.target.files?.[0] || null)}
          />
          <Button
            variant="outline"
            onClick={() => fileRef.current?.click()}
            disabled={uploading}
            className="border-white/10 bg-white/[0.02] text-white hover:bg-white/5"
          >
            {uploading ? <Loader2 size={14} className="animate-spin mr-2" /> : <Upload size={14} className="mr-2" />}
            Enviar documento
          </Button>
        </div>
      </div>
    </div>
  );
}

function NewBuildingModal({
  open,
  companies,
  onClose,
  onCreated,
}: {
  open: boolean;
  companies: CatalogCompany[];
  onClose: () => void;
  onCreated: (created: CatalogBuilding) => Promise<void>;
}) {
  const [saving, setSaving] = useState(false);
  const [name, setName] = useState('');
  const [companyId, setCompanyId] = useState<string>('');
  const [address, setAddress] = useState('');
  const [buildingType, setBuildingType] = useState('');
  const [active, setActive] = useState<'true' | 'false'>('true');

  useEffect(() => {
    if (!open) return;
    setName('');
    setCompanyId('');
    setAddress('');
    setBuildingType('');
    setActive('true');
  }, [open]);

  if (!open) return null;

  const handleSave = async () => {
    if (!name.trim()) return;
    setSaving(true);
    try {
      const created = await createCatalogBuilding({
        name: name.trim(),
        companyId: companyId ? Number(companyId) : null,
        address: address.trim() || null,
        buildingType: buildingType.trim() || null,
        active: active === 'true',
      });
      await onCreated(created);
      onClose();
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-[210] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4" onClick={onClose}>
      <div className="w-full max-w-xl rounded-2xl border border-white/10 bg-[#111216] shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between gap-4 border-b border-white/5 p-5">
          <div>
            <div className="text-[11px] uppercase tracking-[0.24em] text-[#4CB232] font-black">Catálogo</div>
            <div className="mt-1 text-lg font-black text-white">Nova Obra</div>
            <div className="mt-1 text-xs text-slate-400">
              Cadastro local. Se o catálogo do Sienge estiver sincronizado, você pode vincular a empresa.
            </div>
          </div>
          <Button variant="ghost" onClick={onClose}>Fechar</Button>
        </div>

        <div className="p-5 space-y-4">
          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Nome da obra *</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} className="bg-black/40 border-white/10 text-white" />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Empresa (Sienge)</Label>
              <Select value={companyId} onValueChange={setCompanyId}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue placeholder="(opcional)" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">(sem empresa)</SelectItem>
                  {(companies || []).map((c) => (
                    <SelectItem key={String(c.id)} value={String(c.id)}>{c.trade_name || c.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Status</Label>
              <Select value={active} onValueChange={(v: any) => setActive(v)}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="true">Ativa</SelectItem>
                  <SelectItem value="false">Inativa</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Endereço</Label>
            <Input value={address} onChange={(e) => setAddress(e.target.value)} className="bg-black/40 border-white/10 text-white" />
          </div>

          <div className="space-y-2">
            <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Tipo</Label>
            <Input value={buildingType} onChange={(e) => setBuildingType(e.target.value)} placeholder="Ex: Residencial, Comercial..." className="bg-black/40 border-white/10 text-white" />
          </div>
        </div>

        <div className="flex items-center justify-end gap-3 border-t border-white/5 p-5">
          <Button variant="ghost" onClick={onClose}>Cancelar</Button>
          <Button onClick={handleSave} disabled={saving || !name.trim()} className="bg-[#4CB232] hover:bg-[#3f9b2a] text-white">
            {saving ? <Loader2 size={14} className="animate-spin mr-2" /> : null}
            Criar obra
          </Button>
        </div>
      </div>
    </div>
  );
}

export function ContratosTab() {
  const [catalogBuildings, setCatalogBuildings] = useState<CatalogBuilding[]>([]);
  const [catalogCompanies, setCatalogCompanies] = useState<CatalogCompany[]>([]);
  const [selectedBuildingId, setSelectedBuildingId] = useState<string>('');
  const [kind, setKind] = useState<string>('all');
  const [q, setQ] = useState('');
  const [loading, setLoading] = useState(false);
  const [contracts, setContracts] = useState<any[]>([]);
  const [openModal, setOpenModal] = useState(false);
  const [selectedContract, setSelectedContract] = useState<any | null>(null);
  const [documents, setDocuments] = useState<any[]>([]);
  const [loadingDocs, setLoadingDocs] = useState(false);
  const [printing, setPrinting] = useState(false);
  const [openNewBuilding, setOpenNewBuilding] = useState(false);
  const [syncingSienge, setSyncingSienge] = useState(false);

  const buildingIdNum = useMemo(() => {
    const n = Number(selectedBuildingId);
    return Number.isFinite(n) && n > 0 ? n : null;
  }, [selectedBuildingId]);

  const reloadCatalog = async () => {
    const [b, c] = await Promise.all([listCatalogBuildings({ active: 'true' }), listCatalogCompanies()]);
    setCatalogBuildings(b);
    setCatalogCompanies(c);
  };

  const reload = async () => {
    setLoading(true);
    try {
      const rows = await listContracts({
        buildingId: buildingIdNum || undefined,
        q: q.trim() || undefined,
        kind: kind !== 'all' ? kind : undefined,
      });
      setContracts(rows);
      if (selectedContract) {
        const still = rows.find((c) => Number(c.id) === Number(selectedContract.id));
        if (!still) setSelectedContract(null);
      }
    } finally {
      setLoading(false);
    }
  };

  const visibleContracts = useMemo(() => {
    const term = q.trim().toLowerCase();
    if (!term) return contracts;

    const score = (c: any): number => {
      const num = String(c.contractNumber || '').toLowerCase();
      const title = String(c.title || '').toLowerCase();
      const supplier = String(c.supplierLegalName || '').toLowerCase();
      let s = 0;
      if (num.startsWith(term)) s += 120;
      else if (num.includes(term)) s += 90;
      if (title.startsWith(term)) s += 70;
      else if (title.includes(term)) s += 50;
      if (supplier.includes(term)) s += 20;
      return s;
    };

    return [...contracts].sort((a, b) => score(b) - score(a));
  }, [contracts, q]);

  const loadDocs = async (contractId: number) => {
    setLoadingDocs(true);
    try {
      const rows = await listContractDocuments(contractId);
      setDocuments(rows);
    } finally {
      setLoadingDocs(false);
    }
  };

  useEffect(() => {
    reloadCatalog();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    reload();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedBuildingId, kind]);

  useEffect(() => {
    const t = window.setTimeout(() => {
      reload();
    }, 350);
    return () => window.clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q]);

  useEffect(() => {
    if (!selectedContract?.id) return;
    loadDocs(Number(selectedContract.id));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedContract?.id]);

  const handlePrint = async (contractId: number) => {
    setPrinting(true);
    try {
      const html = await getPrintableContractHtml(contractId);
      const w = window.open('', '_blank');
      if (!w) return;
      w.document.open();
      w.document.write(html);
      w.document.close();
      w.focus();
      w.print();
    } finally {
      setPrinting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <div className="text-[11px] uppercase tracking-[0.24em] text-[#4CB232] font-black">Jurídico</div>
        <h1 className="mt-1 text-2xl sm:text-3xl font-black text-white">Contratos</h1>
        <p className="mt-2 text-sm text-slate-400 font-semibold">
          Centralize contratos e documentação vinculados à obra.
        </p>
      </div>

      <Card className="border-white/10 bg-white/[0.02]">
        <CardHeader className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <CardTitle className="text-white">Cadastro de contratos</CardTitle>
            <div className="mt-1 text-xs text-slate-400">
              Ao cadastrar, o sistema cria automaticamente um sprint/workspace operacional no Kanban.
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              onClick={async () => {
                setSyncingSienge(true);
                try {
                  const res = await syncContractsFromSiengeLive();
                  if (res?.ok === false) {
                    alert(res?.message || 'Não foi possível sincronizar do Sienge.');
                  } else {
                    alert(`Sienge: +${res?.created || 0} criados, ${res?.updated || 0} atualizados, ${res?.skipped || 0} ignorados.`);
                  }
                  await reload();
                } finally {
                  setSyncingSienge(false);
                }
              }}
              disabled={syncingSienge}
              className="border-white/10 bg-white/[0.02] text-white hover:bg-white/5"
            >
              {syncingSienge ? <Loader2 size={14} className="animate-spin mr-2" /> : <RefreshCw size={14} className="mr-2" />}
              Sincronizar Sienge
            </Button>
            <Button onClick={() => setOpenModal(true)} className="bg-[#4CB232] hover:bg-[#3f9b2a] text-white">
              <Plus size={14} className="mr-2" /> Novo
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-3">
            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Obra</Label>
              <BuildingPicker
                value={selectedBuildingId}
                buildings={catalogBuildings || []}
                onChange={setSelectedBuildingId}
                onNewBuilding={() => setOpenNewBuilding(true)}
              />
            </div>

            <div className="space-y-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Tipo</Label>
              <Select value={kind} onValueChange={setKind}>
                <SelectTrigger className="bg-black/40 border-white/10 text-white">
                  <SelectValue placeholder="Todos" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todos</SelectItem>
                  <SelectItem value="suprimentos">Suprimentos</SelectItem>
                  <SelectItem value="medicoes">Medições</SelectItem>
                  <SelectItem value="privado">Privado</SelectItem>
                  <SelectItem value="licitacao">Licitação</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2 lg:col-span-2">
              <Label className="text-[10px] font-black uppercase tracking-widest text-slate-400">Buscar</Label>
              <div className="flex items-center gap-2">
                <div className="relative flex-1">
                  <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
                  <Input
                    value={q}
                    onChange={(e) => setQ(e.target.value)}
                    placeholder="Número, título, fornecedor..."
                    className="pl-9 bg-black/40 border-white/10 text-white"
                  />
                </div>
                <Button
                  variant="outline"
                  onClick={reload}
                  disabled={loading}
                  className="border-white/10 bg-white/[0.02] text-white hover:bg-white/5"
                >
                  {loading ? <Loader2 size={14} className="animate-spin" /> : 'Buscar'}
                </Button>
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <div className="xl:col-span-2">
              <div className="overflow-hidden rounded-2xl border border-white/10">
                <Table>
                  <TableHeader>
                    <TableRow className="border-white/10">
                      <TableHead className="text-slate-400">Número</TableHead>
                      <TableHead className="text-slate-400">Título</TableHead>
                      <TableHead className="text-slate-400">Fornecedor</TableHead>
                      <TableHead className="text-slate-400">Tipo</TableHead>
                      <TableHead className="text-slate-400">Status</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {contracts.length === 0 ? (
                      <TableRow className="border-white/10">
                        <TableCell colSpan={5} className="text-slate-500">
                          {loading ? 'Carregando...' : 'Nenhum contrato encontrado.'}
                        </TableCell>
                      </TableRow>
                    ) : (
                      visibleContracts.map((c) => (
                        <TableRow
                          key={String(c.id)}
                          className={cn(
                            "border-white/10 cursor-pointer hover:bg-white/[0.03]",
                            selectedContract?.id === c.id ? 'bg-white/[0.04]' : ''
                          )}
                          onClick={() => setSelectedContract(c)}
                        >
                          <TableCell className="text-white font-semibold">{c.contractNumber || '-'}</TableCell>
                          <TableCell className="text-slate-200">{c.title}</TableCell>
                          <TableCell className="text-slate-300">{c.supplierLegalName || '-'}</TableCell>
                          <TableCell className="text-slate-300">{kindLabel(c.kind)}</TableCell>
                          <TableCell className="text-slate-300 uppercase text-xs font-black tracking-wider">{c.status || 'draft'}</TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>

            <div className="space-y-4">
              <Card className="border-white/10 bg-white/[0.02]">
                <CardHeader>
                  <CardTitle className="text-white">Detalhes</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {!selectedContract ? (
                    <div className="text-sm text-slate-500">Selecione um contrato para ver documentos e imprimir.</div>
                  ) : (
                    <>
                      <div className="space-y-1">
                        <div className="text-[10px] uppercase tracking-widest text-slate-400 font-black">Contrato</div>
                        <div className="text-sm text-white font-semibold">{selectedContract.title}</div>
                        <div className="text-xs text-slate-400">Número: {selectedContract.contractNumber || '-'}</div>
                        <div className="text-xs text-slate-400">Sprint ID: {selectedContract.sprintId ?? '-'}</div>
                      </div>

                      <div className="flex items-center gap-2">
                        <Button
                          onClick={() => handlePrint(Number(selectedContract.id))}
                          disabled={printing}
                          className="bg-[#4CB232] hover:bg-[#3f9b2a] text-white"
                        >
                          {printing ? <Loader2 size={14} className="animate-spin mr-2" /> : <Printer size={14} className="mr-2" />}
                          Imprimir
                        </Button>
                        <Button
                          variant="outline"
                          onClick={async () => {
                            if (!confirm('Remover este contrato e seus documentos?')) return;
                            await deleteContract(Number(selectedContract.id));
                            setSelectedContract(null);
                            await reload();
                          }}
                          className="border-red-500/30 bg-red-500/10 text-red-300 hover:bg-red-500/15"
                        >
                          <Trash2 size={14} className="mr-2" /> Remover
                        </Button>
                      </div>

                      <div className="pt-2">
                        <DocumentUploader contractId={Number(selectedContract.id)} onUploaded={async () => loadDocs(Number(selectedContract.id))} />
                      </div>

                      <div className="space-y-2">
                        <div className="text-[10px] uppercase tracking-widest text-slate-400 font-black">Documentos</div>
                        {loadingDocs ? (
                          <div className="text-sm text-slate-500 flex items-center gap-2"><Loader2 size={14} className="animate-spin" /> Carregando...</div>
                        ) : documents.length === 0 ? (
                          <div className="text-sm text-slate-500">Nenhum documento enviado.</div>
                        ) : (
                          <div className="space-y-2">
                            {documents.map((d) => (
                              <div key={String(d.id)} className="flex items-center justify-between gap-3 rounded-xl border border-white/10 bg-black/30 px-3 py-2">
                                <div className="min-w-0">
                                  <div className="text-sm text-white font-semibold truncate">{d.filename}</div>
                                  <div className="text-[11px] text-slate-400 uppercase tracking-wider font-black">{d.docType} · v{d.version}</div>
                                </div>
                                <Button
                                  variant="ghost"
                                  onClick={async () => {
                                    if (!confirm('Remover documento?')) return;
                                    await deleteContractDocument(Number(selectedContract.id), Number(d.id));
                                    await loadDocs(Number(selectedContract.id));
                                  }}
                                  className="text-slate-400 hover:text-white"
                                >
                                  <Trash2 size={14} />
                                </Button>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    </>
                  )}
                </CardContent>
              </Card>
            </div>
          </div>
        </CardContent>
      </Card>

      <ContractModal
        open={openModal}
        buildings={catalogBuildings || []}
        companies={catalogCompanies || []}
        buildingId={buildingIdNum}
        onClose={() => setOpenModal(false)}
        onCreated={reload}
        onOpenNewBuilding={() => setOpenNewBuilding(true)}
      />

      <NewBuildingModal
        open={openNewBuilding}
        companies={catalogCompanies || []}
        onClose={() => setOpenNewBuilding(false)}
        onCreated={async (created) => {
          await reloadCatalog();
          setSelectedBuildingId(String(created.id));
          // se estiver com o modal de contrato aberto, já seleciona a obra criada nele também
          setOpenModal(true);
        }}
      />
    </div>
  );
}
