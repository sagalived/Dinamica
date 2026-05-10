import { useCallback, useEffect, useMemo, useState } from 'react';

import type { CardStatus } from '../logic';
import type { Attachment, KanbanCard, KanbanSprint } from '../types';
import {
  atualizarCardKanban,
  atualizarSprintKanban,
  criarCardKanban,
  criarSprintKanban,
  excluirCardAnexo,
  excluirCardKanban,
  excluirSprintKanban,
  listarSprintsDaObra,
  uploadCardAnexo,
} from '../kanbanservice';

export function useKanbanObras({
  buildingId,
  buildingName,
  dataRevision,
}: {
  buildingId: number | null;
  buildingName: string;
  dataRevision: number;
}) {
  const [sprints, setSprints] = useState<KanbanSprint[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedSprint, setSelectedSprint] = useState<string | null>(null);
  const [cardModal, setCardModal] = useState<{ card: Partial<KanbanCard> | null; sprintId: string; isNew: boolean } | null>(null);
  const [sprintModal, setSprintModal] = useState<{ sprint?: KanbanSprint; isNew: boolean } | null>(null);
  const [search, setSearch] = useState('');
  const [filterStatus, setFilterStatus] = useState<CardStatus | 'all'>('all');
  const [dragCard, setDragCard] = useState<{ cardId: string; fromSprintId: string } | null>(null);
  const [dragOver, setDragOver] = useState<CardStatus | null>(null);
  const [activeBuildingId, setActiveBuildingId] = useState<number | null>(buildingId);
  const [activeBuildingName, setActiveBuildingName] = useState(buildingName);

  useEffect(() => {
    setActiveBuildingId(buildingId);
    setActiveBuildingName(buildingName);
  }, [buildingId, buildingName]);

  const fetchData = useCallback(async () => {
    if (!activeBuildingId) return;
    setLoading(true);
    try {
      const data = await listarSprintsDaObra(activeBuildingId);
      setSprints(data);
      if (!selectedSprint && data.length > 0) setSelectedSprint(data[0].id);
    } catch {
      setSprints([]);
    } finally {
      setLoading(false);
    }
  }, [activeBuildingId, selectedSprint]);

  useEffect(() => {
    fetchData();
  }, [fetchData, dataRevision]);

  const createSprint = useCallback(
    async (data: any) => {
      await criarSprintKanban({ ...data, buildingId: activeBuildingId });
      await fetchData();
    },
    [activeBuildingId, fetchData],
  );

  const updateSprint = useCallback(
    async (sprintId: string, data: any) => {
      await atualizarSprintKanban(sprintId, data);
      await fetchData();
    },
    [fetchData],
  );

  const deleteSprint = useCallback(
    async (sprintId: string) => {
      await excluirSprintKanban(sprintId);
      setSprints((prev) => prev.filter((x) => x.id !== sprintId));
      if (selectedSprint === sprintId) setSelectedSprint(null);
    },
    [selectedSprint],
  );

  const createCard = useCallback(
    async (data: Partial<KanbanCard>) => {
      await criarCardKanban({ ...data, buildingId: String(activeBuildingId || '') });
      await fetchData();
    },
    [activeBuildingId, fetchData],
  );

  const updateCard = useCallback(
    async (cardId: string, data: Partial<KanbanCard>) => {
      await atualizarCardKanban(cardId, data);
      await fetchData();
    },
    [fetchData],
  );

  const deleteCard = useCallback(async (cardId: string) => {
    await excluirCardKanban(cardId);
    setSprints((prev) =>
      prev.map((sp) => ({
        ...sp,
        cards: sp.cards.filter((c) => c.id !== cardId),
      })),
    );
  }, []);

  const saveCard = useCallback(
    async (data: Partial<KanbanCard>) => {
      if (cardModal?.isNew) {
        await createCard(data);
      } else if (data.id) {
        await updateCard(data.id, data);
      }
    },
    [cardModal?.isNew, createCard, updateCard],
  );

  const handleDragStart = useCallback((e: React.DragEvent, cardId: string, sprintId: string) => {
    setDragCard({ cardId, fromSprintId: sprintId });
    e.dataTransfer.effectAllowed = 'move';
  }, []);

  const handleDrop = useCallback(
    async (e: React.DragEvent, newStatus: CardStatus) => {
      e.preventDefault();
      setDragOver(null);
      if (!dragCard) return;
      const { cardId } = dragCard;
      setDragCard(null);

      setSprints((prev) =>
        prev.map((sp) => ({
          ...sp,
          cards: sp.cards.map((c) => (c.id === cardId ? { ...c, status: newStatus } : c)),
        })),
      );

      try {
        await atualizarCardKanban(cardId, { status: newStatus });
      } catch {
        await fetchData();
      }
    },
    [dragCard, fetchData],
  );

  const uploadFile = useCallback(async (cardId: string, file: File): Promise<Attachment | null> => {
    return uploadCardAnexo(cardId, file);
  }, []);

  const deleteAttachment = useCallback(async (cardId: string, att: Attachment) => {
    await excluirCardAnexo(cardId, att);
  }, []);

  const currentSprint = useMemo(
    () => (selectedSprint ? sprints.find((s) => s.id === selectedSprint) || null : null),
    [selectedSprint, sprints],
  );

  const filteredCards = useCallback(
    (sprintCards: KanbanCard[]): KanbanCard[] => {
      return sprintCards.filter((c) => {
        const matchSearch =
          !search ||
          c.title.toLowerCase().includes(search.toLowerCase()) ||
          c.description.toLowerCase().includes(search.toLowerCase()) ||
          c.responsible.toLowerCase().includes(search.toLowerCase());
        const matchStatus = filterStatus === 'all' || c.status === filterStatus;
        return matchSearch && matchStatus;
      });
    },
    [filterStatus, search],
  );

  const totalCards = useMemo(() => sprints.reduce((a, s) => a + s.cards.length, 0), [sprints]);
  const doneCards = useMemo(
    () => sprints.reduce((a, s) => a + s.cards.filter((c) => c.status === 'done').length, 0),
    [sprints],
  );
  const progressPct = totalCards > 0 ? Math.round((doneCards / totalCards) * 100) : 0;

  return {
    sprints,
    loading,
    selectedSprint,
    setSelectedSprint,
    cardModal,
    setCardModal,
    sprintModal,
    setSprintModal,
    search,
    setSearch,
    filterStatus,
    setFilterStatus,
    dragOver,
    setDragOver,
    activeBuildingId,
    setActiveBuildingId,
    activeBuildingName,
    setActiveBuildingName,
    fetchData,
    createSprint,
    updateSprint,
    deleteSprint,
    saveCard,
    deleteCard,
    handleDragStart,
    handleDrop,
    uploadFile,
    deleteAttachment,
    currentSprint,
    filteredCards,
    totalCards,
    doneCards,
    progressPct,
  };
}
