import { kanbanApi as api } from '../../lib/api';

import type { Attachment, KanbanCard, KanbanSprint } from './types';

export async function listarSprintsDaObra(buildingId: string | number): Promise<KanbanSprint[]> {
  const res = await api.get(`/kanban?building_id=${buildingId}`);
  return res.data?.buildings?.[buildingId] || [];
}

export async function criarSprintKanban(payload: any): Promise<void> {
  await api.post('/kanban/sprint', payload);
}

export async function atualizarSprintKanban(sprintId: string, payload: any): Promise<void> {
  await api.patch(`/kanban/sprint/${sprintId}`, payload);
}

export async function excluirSprintKanban(sprintId: string): Promise<void> {
  await api.delete(`/kanban/sprint/${sprintId}`);
}

export async function criarCardKanban(payload: Partial<KanbanCard>): Promise<void> {
  await api.post('/kanban/card', payload);
}

export async function atualizarCardKanban(cardId: string, payload: Partial<KanbanCard>): Promise<void> {
  await api.patch(`/kanban/card/${cardId}`, payload);
}

export async function excluirCardKanban(cardId: string): Promise<void> {
  await api.delete(`/kanban/card/${cardId}`);
}

export async function uploadCardAnexo(cardId: string, file: File): Promise<Attachment | null> {
  const form = new FormData();
  form.append('file', file);
  const res = await api.post(`/kanban/upload?card_id=${cardId}`, form, {
    headers: { 'Content-Type': 'multipart/form-data' },
  });
  return res.data?.attachment || null;
}

export async function excluirCardAnexo(cardId: string, attachment: Attachment): Promise<void> {
  await api.delete(`/kanban/upload?card_id=${cardId}&filename=${attachment.filename}`);
}
