import React, { useEffect, useMemo, useState } from 'react';
import { Truck, MapPin, Wrench, Fuel, Plus, Trash2, Calendar, FileText, Route } from 'lucide-react';
import { format } from 'date-fns';
import { api, type Building, type LogisticsLocation } from '../lib/api';
import { fixText } from '../lib/text';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';

interface LogisticsEntry {
  id: string;
  date: string;
  type: 'combustivel' | 'manutencao' | 'rota' | 'outros';
  vehicle: string;
  user: string;
  cost: number;
  routeStart?: string;
  routeEnd?: string;
  fuelConsumed?: number;
  notes?: string;
}

type RouteOption = {
  value: string;
  label: string;
  code?: string;
  address?: string;
  latitude?: number;
  longitude?: number;
  type?: string;
};

type RouteEstimate = {
  km: number | null;
  provider: string;
};

type LogisticsTabProps = {
  buildings: Building[];
  readOnly?: boolean;
};

const HQ_OPTION: RouteOption = {
  value: 'hq',
  label: 'Sede',
  code: 'HQ',
  address: 'Dinamica Empreendimentos e Solucoes LTDA, Fortaleza, CE, Brasil',
  latitude: -3.7319,
  longitude: -38.5267,
};

const CITY_COORDINATES: Record<string, { latitude: number; longitude: number }> = {
  fortaleza: { latitude: -3.7319, longitude: -38.5267 },
  caucaia: { latitude: -3.7361, longitude: -38.6531 },
  maracanau: { latitude: -3.8767, longitude: -38.6256 },
  'morada nova': { latitude: -5.106, longitude: -38.3725 },
  baturite: { latitude: -4.3289, longitude: -38.8848 },
  camocim: { latitude: -2.9027, longitude: -40.8411 },
  taua: { latitude: -6.0032, longitude: -40.2928 },
  tiangua: { latitude: -3.7322, longitude: -40.9917 },
  ubajara: { latitude: -3.8548, longitude: -40.9211 },
  'tabuleiro do norte': { latitude: -5.2466, longitude: -38.1282 },
  paracuru: { latitude: -3.4142, longitude: -39.0306 },
  horizonte: { latitude: -4.0982, longitude: -38.4852 },
  quixada: { latitude: -4.9708, longitude: -39.0153 },
  sobral: { latitude: -3.688, longitude: -40.348 },
  acarau: { latitude: -2.8856, longitude: -40.1198 },
  iguatu: { latitude: -6.3598, longitude: -39.2982 },
  crateus: { latitude: -5.1787, longitude: -40.6776 },
  'limoeiro do norte': { latitude: -5.1439, longitude: -38.0847 },
  'boa viagem': { latitude: -5.1278, longitude: -39.7322 },
  jaguaribe: { latitude: -5.8905, longitude: -38.6219 },
  jaguaruana: { latitude: -4.8337, longitude: -37.7811 },
  caninde: { latitude: -4.3587, longitude: -39.3117 },
  umirim: { latitude: -3.6774, longitude: -39.3505 },
  maranguape: { latitude: -3.8914, longitude: -38.6826 },
  pentecoste: { latitude: -3.7926, longitude: -39.2692 },
};

function getNumericCoordinate(value?: number) {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function haversineKm(startLat: number, startLng: number, endLat: number, endLng: number) {
  const toRad = (value: number) => (value * Math.PI) / 180;
  const earthRadiusKm = 6371;
  const dLat = toRad(endLat - startLat);
  const dLng = toRad(endLng - startLng);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(startLat)) *
      Math.cos(toRad(endLat)) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return earthRadiusKm * c;
}

function normalizeLookupKey(value: string) {
  return fixText(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

function extractCityFromAddress(address: string) {
  const normalized = fixText(address);
  const hyphenParts = normalized.split(' - ').map((part) => part.trim()).filter(Boolean);
  const ceIndex = hyphenParts.findIndex((part) => normalizeLookupKey(part) === 'ce');
  if (ceIndex > 0) {
    return hyphenParts[ceIndex - 1];
  }

  const commaParts = normalized.split(',').map((part) => part.trim()).filter(Boolean);
  return commaParts.length > 0 ? commaParts[commaParts.length - 1] : normalized;
}

function inferCoordinatesFromText(...values: Array<string | undefined>) {
  for (const value of values) {
    const normalizedValue = fixText(value || '').trim();
    if (!normalizedValue) continue;

    const directCity = CITY_COORDINATES[normalizeLookupKey(extractCityFromAddress(normalizedValue))];
    if (directCity) return directCity;

    const normalizedKey = normalizeLookupKey(normalizedValue);
    const partialMatch = Object.entries(CITY_COORDINATES).find(([city]) => (
      normalizedKey.includes(city) || city.includes(normalizedKey)
    ));
    if (partialMatch) return partialMatch[1];
  }

  return undefined;
}

async function geocodeAddressInBrowser(address: string) {
  const query = fixText(address).trim();
  if (!query) return null;

  try {
    const response = await fetch(`https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=${encodeURIComponent(query)}`);
    const results = await response.json();
    if (Array.isArray(results) && results.length > 0) {
      const latitude = Number(results[0].lat);
      const longitude = Number(results[0].lon);
      if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
        return { latitude, longitude, provider: 'Nominatim' };
      }
    }
  } catch {
    // Fallback below.
  }

  const city = extractCityFromAddress(query);
  const fallbackCoords = CITY_COORDINATES[normalizeLookupKey(city)];
  if (fallbackCoords) {
    return { ...fallbackCoords, provider: 'Cidade aproximada' };
  }

  return null;
}

async function getRouteDistanceInBrowser(
  origin: { address: string; latitude?: number; longitude?: number },
  destination: { address: string; latitude?: number; longitude?: number }
) {
  const originCoords =
    origin.latitude !== undefined && origin.longitude !== undefined
      ? { latitude: origin.latitude, longitude: origin.longitude, provider: 'Coordenada da obra' }
      : await geocodeAddressInBrowser(origin.address);

  const destinationCoords =
    destination.latitude !== undefined && destination.longitude !== undefined
      ? { latitude: destination.latitude, longitude: destination.longitude, provider: 'Coordenada da obra' }
      : await geocodeAddressInBrowser(destination.address);

  if (!originCoords || !destinationCoords) {
    return { distanceKm: null, provider: '' };
  }

  try {
    const response = await fetch(
      `https://router.project-osrm.org/route/v1/driving/${originCoords.longitude},${originCoords.latitude};${destinationCoords.longitude},${destinationCoords.latitude}?overview=false`
    );
    const payload = await response.json();
    const distanceMeters = payload?.routes?.[0]?.distance;
    if (typeof distanceMeters === 'number') {
      return {
        distanceKm: distanceMeters / 1000,
        provider: 'OSRM',
      };
    }
  } catch {
    // Fall through to straight-line estimate below.
  }

  return {
    distanceKm: haversineKm(originCoords.latitude, originCoords.longitude, destinationCoords.latitude, destinationCoords.longitude),
    provider: originCoords.provider === 'Cidade aproximada' || destinationCoords.provider === 'Cidade aproximada'
      ? 'Aproximação por cidade'
      : 'Linha reta',
  };
}

export function LogisticsTab({ buildings, readOnly = false }: LogisticsTabProps) {
  const [entries, setEntries] = useState<LogisticsEntry[]>([]);
  const [dbLocations, setDbLocations] = useState<LogisticsLocation[]>([]);
  const [activePanel, setActivePanel] = useState<'history' | 'route'>('history');
  const [startOption, setStartOption] = useState<string>('hq');
  const [endOption, setEndOption] = useState<string>('other');
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [routeEstimate, setRouteEstimate] = useState<RouteEstimate>({ km: null, provider: '' });
  const [formData, setFormData] = useState<Partial<LogisticsEntry>>({
    type: 'combustivel',
    date: new Date().toISOString().split('T')[0],
  });

  const routeOptions = useMemo<RouteOption[]>(() => {
    const merged = new Map<string, RouteOption>();
    merged.set(HQ_OPTION.value, HQ_OPTION);

    buildings.forEach((building) => {
      const code = String(building.code || building.id);
      const label = fixText(building.name || `Obra ${code}`);
      const address = fixText(building.address || building.name);
      const inferredCoordinates = inferCoordinatesFromText(address, label, code);
      merged.set(`building:${code}`, {
        value: `building:${code}`,
        label,
        code,
        address,
        latitude: getNumericCoordinate(building.latitude) ?? inferredCoordinates?.latitude,
        longitude: getNumericCoordinate(building.longitude) ?? inferredCoordinates?.longitude,
        type: 'building',
      });
    });

    dbLocations.forEach((location) => {
      const value = location.code || `custom:${location.id}`;
      if (value === HQ_OPTION.value) {
        merged.set(HQ_OPTION.value, {
          ...HQ_OPTION,
          address: HQ_OPTION.address,
          latitude: HQ_OPTION.latitude,
          longitude: HQ_OPTION.longitude,
        });
        return;
      }

      const matchedBuilding = location.type === 'building'
        ? buildings.find((building) => String(building.code || building.id) === String(location.code))
        : undefined;
      const resolvedAddress = fixText(location.address || matchedBuilding?.address || location.name);
      const inferredCoordinates = inferCoordinatesFromText(location.address, location.name, location.code);
      merged.set(value, {
        value,
        label: fixText(location.name || matchedBuilding?.name || value),
        code: location.code,
        address: resolvedAddress,
        latitude: getNumericCoordinate(location.latitude) ?? getNumericCoordinate(matchedBuilding?.latitude) ?? inferredCoordinates?.latitude,
        longitude: getNumericCoordinate(location.longitude) ?? getNumericCoordinate(matchedBuilding?.longitude) ?? inferredCoordinates?.longitude,
        type: location.type,
      });
    });

    merged.set('other', { value: 'other', label: 'Adicionar...', code: 'NOVO', type: 'custom' });
    return Array.from(merged.values());
  }, [buildings, dbLocations]);

  const selectedStartOption = routeOptions.find((option) => option.value === startOption);
  const selectedEndOption = routeOptions.find((option) => option.value === endOption);

  const resolvedRouteStart = startOption === 'other'
    ? customStart.trim()
    : selectedStartOption?.address || selectedStartOption?.label || '';

  const resolvedRouteEnd = endOption === 'other'
    ? customEnd.trim()
    : selectedEndOption?.address || selectedEndOption?.label || '';

  const straightLineDistanceKm = useMemo(() => {
    if (
      selectedStartOption?.latitude === undefined ||
      selectedStartOption?.longitude === undefined ||
      selectedEndOption?.latitude === undefined ||
      selectedEndOption?.longitude === undefined
    ) {
      return null;
    }

    return haversineKm(
      selectedStartOption.latitude,
      selectedStartOption.longitude,
      selectedEndOption.latitude,
      selectedEndOption.longitude
    );
  }, [selectedEndOption, selectedStartOption]);

  const routeDistanceKm = routeEstimate.km ?? straightLineDistanceKm;
  const routeDistanceProvider = routeEstimate.provider || (straightLineDistanceKm !== null ? 'Linha reta' : '');

  const routeEmbedUrl = useMemo(() => {
    if (!resolvedRouteStart || !resolvedRouteEnd) return '';
    return `https://www.google.com/maps?output=embed&saddr=${encodeURIComponent(resolvedRouteStart)}&daddr=${encodeURIComponent(resolvedRouteEnd)}`;
  }, [resolvedRouteEnd, resolvedRouteStart]);

  const routeMapsUrl = useMemo(() => {
    if (!resolvedRouteStart || !resolvedRouteEnd) return '';
    return `https://www.google.com/maps/dir/?api=1&origin=${encodeURIComponent(resolvedRouteStart)}&destination=${encodeURIComponent(resolvedRouteEnd)}&travelmode=driving`;
  }, [resolvedRouteEnd, resolvedRouteStart]);

  useEffect(() => {
    try {
      const stored = localStorage.getItem('dinamica_logistics');
      if (stored) {
        setEntries(JSON.parse(stored));
      }
    } catch (error) {
      console.error(error);
    }
  }, []);

  useEffect(() => {
    localStorage.setItem('dinamica_logistics', JSON.stringify(entries));
  }, [entries]);

  useEffect(() => {
    api.get('/logistics/locations')
      .then((response) => {
        const results = response.data?.results;
        setDbLocations(Array.isArray(results) ? results : []);
      })
      .catch((error) => {
        console.error('Erro ao carregar locais logísticos:', error);
      });
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function updateRouteDistance() {
      if (!resolvedRouteStart || !resolvedRouteEnd) {
        setRouteEstimate({ km: null, provider: '' });
        return;
      }

      try {
        const payload = {
          origin: {
            address: resolvedRouteStart,
            latitude: selectedStartOption?.latitude,
            longitude: selectedStartOption?.longitude,
          },
          destination: {
            address: resolvedRouteEnd,
            latitude: selectedEndOption?.latitude,
            longitude: selectedEndOption?.longitude,
          },
        };
        console.log('[Route][UI] Iniciando cálculo', payload);

        let response: { distanceKm: number | null; provider: string };

        try {
          const serverResponse = await api.post('/logistics/route-distance', payload);
          response = serverResponse.data;
          console.log('[Route][UI] Resposta backend', response);
        } catch {
          response = await getRouteDistanceInBrowser(payload.origin, payload.destination);
          console.log('[Route][UI] Fallback navegador', response);
        }

        if (!cancelled) {
          const km = typeof response.distanceKm === 'number' ? response.distanceKm : straightLineDistanceKm;
          const provider = response.provider || (straightLineDistanceKm !== null ? 'Linha reta' : '');
          console.log('[Route][UI] Aplicando no card', { km, provider, straightLineDistanceKm });
          setRouteEstimate({ km, provider });
        }
      } catch (error) {
        if (!cancelled) {
          console.error('Erro ao calcular rota:', error);
          setRouteEstimate({
            km: straightLineDistanceKm,
            provider: straightLineDistanceKm !== null ? 'Linha reta' : '',
          });
        }
      }
    }

    void updateRouteDistance();

    return () => {
      cancelled = true;
    };
  }, [resolvedRouteEnd, resolvedRouteStart, selectedEndOption?.latitude, selectedEndOption?.longitude, selectedStartOption?.latitude, selectedStartOption?.longitude, straightLineDistanceKm]);

  const saveCustomLocation = async (name: string, address: string) => {
    const trimmedName = name.trim();
    if (!trimmedName) return null;

    try {
      const response = await api.post('/logistics/locations', {
        code: `custom:${trimmedName.toLowerCase().replace(/\s+/g, '-')}`,
        name: trimmedName,
        address: address.trim() || trimmedName,
        type: 'custom',
        source: 'manual',
      });
      const location = response.data?.location;
      if (location) {
        setDbLocations((prev) => {
          const next = prev.filter((item) => item.code !== location.code);
          return [...next, location];
        });
      }
      return location?.code || null;
    } catch (error) {
      console.error('Erro ao salvar local customizado:', error);
      return null;
    }
  };

  const handleCreate = async () => {
    if (readOnly) return;
    if (!formData.vehicle || !formData.user || !formData.cost || formData.cost <= 0) {
      alert('Preencha veículo, motorista e valor gasto corretamente.');
      return;
    }

    let savedStartCode = startOption;
    let savedEndCode = endOption;

    if (startOption === 'other' && customStart.trim()) {
      savedStartCode = (await saveCustomLocation(customStart, customStart)) || startOption;
    }
    if (endOption === 'other' && customEnd.trim()) {
      savedEndCode = (await saveCustomLocation(customEnd, customEnd)) || endOption;
    }

    const newEntry: LogisticsEntry = {
      id: Date.now().toString(),
      date: formData.date || new Date().toISOString().split('T')[0],
      type: formData.type as LogisticsEntry['type'],
      vehicle: formData.vehicle,
      user: formData.user,
      cost: Number(formData.cost),
      routeStart: resolvedRouteStart || undefined,
      routeEnd: resolvedRouteEnd || undefined,
      fuelConsumed: formData.fuelConsumed ? Number(formData.fuelConsumed) : undefined,
      notes: formData.notes,
    };

    setEntries((prev) => [newEntry, ...prev]);
    if (resolvedRouteStart || resolvedRouteEnd) {
      setActivePanel('history');
    }
    setStartOption(savedStartCode === 'other' ? 'hq' : savedStartCode || 'hq');
    setEndOption(savedEndCode === 'other' ? 'other' : savedEndCode || 'other');
    setCustomStart('');
    setCustomEnd('');
    setRouteEstimate({ km: null, provider: '' });
    setFormData({
      type: 'combustivel',
      date: new Date().toISOString().split('T')[0],
      vehicle: '',
      user: '',
      cost: 0,
      fuelConsumed: 0,
      notes: '',
    });
  };

  const removeEntry = (id: string) => {
    if (readOnly) return;
    setEntries((prev) => prev.filter((entry) => entry.id !== id));
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'combustivel':
        return <Fuel fill="#10b981" className="text-emerald-500" size={16} />;
      case 'manutencao':
        return <Wrench fill="#f59e0b" className="text-amber-500" size={16} />;
      case 'rota':
        return <MapPin fill="#3b82f6" className="text-blue-500" size={16} />;
      default:
        return <Truck fill="#6b7280" className="text-gray-500" size={16} />;
    }
  };

  const renderRouteLabel = (option?: RouteOption, placeholder?: string) => {
    if (!option) {
      return <span className="text-gray-500">{placeholder}</span>;
    }

    return (
      <div className="flex min-w-0 max-w-full flex-col items-start text-left">
        <span className="max-w-full truncate text-sm font-bold text-white">{fixText(option.label)}</span>
        <span className="max-w-full truncate text-[10px] uppercase tracking-wide text-gray-500">
          {option.code || option.type || 'LOCAL'}
        </span>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-orange-500/10">
          <Truck className="text-orange-500" size={28} />
        </div>
        <div>
          <h2 className="text-2xl font-black uppercase tracking-tight text-white">Logística e Frota</h2>
          <p className="text-sm text-gray-500">Controle manual de gastos de veículos, manutenções e rotas.</p>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,430px)_minmax(0,1fr)]">
        <Card className="h-fit border-white/5 bg-[#161618] shadow-2xl">
          <CardHeader className="pb-4">
            <CardTitle className="flex items-center gap-2 text-lg font-black uppercase tracking-tight text-white">
              <Plus size={18} className="text-orange-500" /> Novo Registro
            </CardTitle>
          </CardHeader>
          <CardContent className={readOnly ? "space-y-5 opacity-60 pointer-events-none" : "space-y-5"}>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="space-y-2">
                <Label className="text-xs font-bold uppercase text-gray-400">Data</Label>
                <Input
                  type="date"
                  value={formData.date || ''}
                  onChange={(e) => setFormData({ ...formData, date: e.target.value })}
                  className="border-white/10 bg-black/40 text-white"
                />
              </div>

              <div className="space-y-2">
                <Label className="text-xs font-bold uppercase text-gray-400">Tipo de Gasto</Label>
                <Select value={formData.type || 'combustivel'} onValueChange={(value: LogisticsEntry['type']) => setFormData({ ...formData, type: value })}>
                  <SelectTrigger className="border-white/10 bg-black/40 text-white">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent className="border-white/10 bg-[#161618] text-white">
                    <SelectItem value="combustivel">Abastecimento</SelectItem>
                    <SelectItem value="manutencao">Manutenção</SelectItem>
                    <SelectItem value="rota">Deslocamento / Rota</SelectItem>
                    <SelectItem value="outros">Outros</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="space-y-2">
                <Label className="text-xs font-bold uppercase text-gray-400">Veículo / Placa</Label>
                <Input
                  placeholder="Ex: Fiat Strada OQX-1234"
                  value={formData.vehicle || ''}
                  onChange={(e) => setFormData({ ...formData, vehicle: e.target.value })}
                  className="border-white/10 bg-black/40 text-white"
                />
              </div>
              <div className="space-y-2">
                <Label className="text-xs font-bold uppercase text-gray-400">Motorista</Label>
                <Input
                  placeholder="Quem utilizou?"
                  value={formData.user || ''}
                  onChange={(e) => setFormData({ ...formData, user: e.target.value })}
                  className="border-white/10 bg-black/40 text-white"
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase text-gray-400">Custo Total (R$)</Label>
              <Input
                type="number"
                min="0"
                step="0.01"
                placeholder="Ex: 150.00"
                value={formData.cost || ''}
                onChange={(e) => setFormData({ ...formData, cost: Number(e.target.value) })}
                className="border-white/10 bg-black/40 font-bold text-orange-500"
              />
            </div>

            <div className="space-y-4 rounded-2xl border border-white/5 bg-black/20 p-4">
              <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
                <Label className="text-xs font-bold uppercase text-gray-400">Dados da Rota</Label>
                <span className="text-[10px] font-bold uppercase tracking-wide text-gray-500">
                  {formData.type === 'rota' ? 'Usado no deslocamento' : 'Opcional'}
                </span>
              </div>

              <div className="grid grid-cols-1 gap-3">
                <div className="space-y-2">
                  <Label className="text-xs font-bold uppercase text-gray-400">Início da Rota</Label>
                  <Select value={startOption} onValueChange={setStartOption}>
                    <SelectTrigger className="w-full border-white/10 bg-black/40 text-white">
                      <SelectValue placeholder="Selecione a origem">
                        {renderRouteLabel(selectedStartOption, 'Selecione a origem')}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent className="max-w-[min(92vw,380px)] border-white/10 bg-[#161618] text-white">
                      {routeOptions.map((option) => (
                        <SelectItem key={`start-${option.value}`} value={option.value}>
                          <div className="flex max-w-full flex-col items-start py-0.5">
                            <span className="max-w-full truncate text-sm font-bold text-white">{fixText(option.label)}</span>
                            <span className="text-[10px] uppercase tracking-wide text-gray-400">{option.code || option.type || 'LOCAL'}</span>
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {startOption === 'other' ? (
                    <Input
                      placeholder="Digite o endereço de início"
                      value={customStart}
                      onChange={(e) => setCustomStart(e.target.value)}
                      className="border-white/10 bg-black/40 text-xs text-white"
                    />
                  ) : null}
                </div>

                <div className="space-y-2">
                  <Label className="text-xs font-bold uppercase text-gray-400">Fim da Rota</Label>
                  <Select value={endOption} onValueChange={setEndOption}>
                    <SelectTrigger className="w-full border-white/10 bg-black/40 text-white">
                      <SelectValue placeholder="Selecione o destino">
                        {renderRouteLabel(selectedEndOption, 'Selecione o destino')}
                      </SelectValue>
                    </SelectTrigger>
                    <SelectContent className="max-w-[min(92vw,380px)] border-white/10 bg-[#161618] text-white">
                      {routeOptions.map((option) => (
                        <SelectItem key={`end-${option.value}`} value={option.value}>
                          <div className="flex max-w-full flex-col items-start py-0.5">
                            <span className="max-w-full truncate text-sm font-bold text-white">{fixText(option.label)}</span>
                            <span className="text-[10px] uppercase tracking-wide text-gray-400">{option.code || option.type || 'LOCAL'}</span>
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {endOption === 'other' ? (
                    <Input
                      placeholder="Digite o endereço final"
                      value={customEnd}
                      onChange={(e) => setCustomEnd(e.target.value)}
                      className="border-white/10 bg-black/40 text-xs text-white"
                    />
                  ) : null}
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr)_180px]">
                <div className="space-y-2">
                  <Label className="text-xs font-bold uppercase text-gray-400">Combustível Gasto (litros / calculado)</Label>
                  <Input
                    type="number"
                    placeholder="Ex: 10"
                    value={formData.fuelConsumed || ''}
                    onChange={(e) => setFormData({ ...formData, fuelConsumed: Number(e.target.value) })}
                    className="border-white/10 bg-black/40 text-xs text-white"
                  />
                </div>

                <div className="rounded-xl border border-white/10 bg-black/30 px-4 py-3">
                  <p className="text-[10px] font-black uppercase text-gray-500">KM Estimado</p>
                  <p className="mt-1 text-2xl font-black text-orange-500">
                    {routeDistanceKm !== null ? `${routeDistanceKm.toFixed(1)} km` : '--'}
                  </p>
                  <p className="mt-1 text-[10px] uppercase tracking-wide text-gray-500">
                    {routeDistanceProvider || 'Aguardando rota'}
                  </p>
                </div>
              </div>
            </div>

            <div className="space-y-2">
              <Label className="text-xs font-bold uppercase text-gray-400">Observações Complementares</Label>
              <Input
                placeholder="Ex: Troca de óleo, peças, pedágio..."
                value={formData.notes || ''}
                onChange={(e) => setFormData({ ...formData, notes: e.target.value })}
                className="border-white/10 bg-black/40 text-white"
              />
            </div>

            <Button onClick={handleCreate} disabled={readOnly} className="w-full bg-orange-600 font-black tracking-tight text-white hover:bg-orange-700">
              {readOnly ? 'Visualização somente leitura' : 'Adicionar Registro'}
            </Button>
          </CardContent>
        </Card>

        <Card className="flex min-h-[520px] min-w-0 flex-col overflow-hidden border-white/5 bg-[#161618] shadow-2xl">
          <CardHeader className="gap-4 border-b border-white/5 pb-4">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="flex flex-wrap items-center gap-2">
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setActivePanel('history')}
                  className={activePanel === 'history' ? 'bg-white/10 font-black uppercase text-white' : 'font-black uppercase text-gray-400 hover:text-white'}
                >
                  Histórico
                </Button>
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setActivePanel('route')}
                  className={activePanel === 'route' ? 'bg-orange-600 font-black uppercase text-white hover:bg-orange-700' : 'font-black uppercase text-orange-400 hover:text-orange-300'}
                >
                  Rota
                </Button>
              </div>

              <div className="rounded-xl border border-white/10 bg-black/40 px-4 py-2">
                <p className="mb-1 text-[10px] font-bold uppercase text-gray-500">Custo Acumulado</p>
                <p className="text-xl font-black text-orange-500">
                  {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(entries.reduce((acc, curr) => acc + curr.cost, 0))}
                </p>
              </div>
            </div>
          </CardHeader>

          <CardContent className="flex-1 p-0">
            {activePanel === 'route' ? (
              <div className="flex h-full flex-col">
                <div className="flex flex-col gap-4 border-b border-white/5 px-4 py-4 sm:px-5 xl:flex-row xl:items-start xl:justify-between">
                  <div className="min-w-0">
                    <p className="text-sm font-black uppercase tracking-tight text-white">Mapa da Rota</p>
                    <p className="text-xs text-gray-500">
                      {resolvedRouteStart && resolvedRouteEnd
                        ? `${fixText(resolvedRouteStart)} → ${fixText(resolvedRouteEnd)}`
                        : 'Escolha início e fim da rota para visualizar o trajeto.'}
                    </p>
                  </div>

                  <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
                    <div className="rounded-xl border border-white/10 bg-black/30 px-4 py-2">
                      <p className="text-[10px] font-black uppercase text-gray-500">KM Estimado</p>
                      <p className="text-lg font-black text-orange-500">
                        {routeDistanceKm !== null ? `${routeDistanceKm.toFixed(1)} km` : '--'}
                      </p>
                      <p className="text-[10px] uppercase tracking-wide text-gray-500">
                        {routeDistanceProvider || 'Sem cálculo'}
                      </p>
                    </div>

                    {routeMapsUrl ? (
                      <a
                        href={routeMapsUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center justify-center rounded-lg bg-white/10 px-4 py-2 text-xs font-black uppercase text-white transition hover:bg-white/20 lg:max-w-[220px]"
                      >
                        Abrir no Google Maps
                      </a>
                    ) : null}
                  </div>
                </div>

                <div className="min-h-[320px] flex-1 bg-[#0d0d0f]">
                  {routeEmbedUrl ? (
                    <iframe title="Mapa da rota" src={routeEmbedUrl} className="h-full min-h-[320px] w-full border-0" loading="lazy" />
                  ) : (
                    <div className="flex h-full min-h-[320px] flex-col items-center justify-center px-6 text-center text-gray-500">
                      <Route size={42} className="mb-3 opacity-30" />
                      <p className="text-sm font-bold">Nenhuma rota pronta para exibir.</p>
                      <p className="mt-1 text-xs">Escolha início e fim da rota para abrir o mapa nesta área.</p>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <>
                <div className="space-y-3 p-4 md:hidden">
                  {entries.length === 0 ? (
                    <div className="flex min-h-[220px] flex-col items-center justify-center rounded-2xl border border-dashed border-white/10 px-6 text-center opacity-70">
                      <FileText size={32} className="mb-3 text-gray-500" />
                      <p className="font-bold text-gray-300">Nenhum registro logístico incluído.</p>
                    </div>
                  ) : (
                    entries.map((entry) => (
                      <div key={entry.id} className="rounded-2xl border border-white/5 bg-black/20 p-4">
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex items-start gap-3">
                            <div className="inline-flex rounded-lg bg-white/10 p-2">
                              {getTypeIcon(entry.type)}
                            </div>
                            <div className="min-w-0">
                              <p className="text-sm font-bold text-white">{entry.vehicle}</p>
                              <p className="mt-1 text-xs text-gray-500">Motorista: {entry.user}</p>
                              <p className="mt-1 flex items-center gap-2 text-xs text-gray-500">
                                <Calendar size={12} /> {format(new Date(entry.date), 'dd/MM/yyyy')}
                              </p>
                            </div>
                          </div>

                          {!readOnly ? (
                            <Button variant="ghost" size="icon" onClick={() => removeEntry(entry.id)} className="h-8 w-8 text-red-500 hover:bg-red-500/10 hover:text-red-400">
                              <Trash2 size={14} />
                            </Button>
                          ) : null}
                        </div>

                        <div className="mt-4 space-y-2 text-xs">
                          {entry.routeStart || entry.routeEnd ? (
                            <>
                              <div><span className="font-bold text-gray-500">Início:</span> <span className="text-gray-300">{entry.routeStart || 'N/A'}</span></div>
                              <div><span className="font-bold text-gray-500">Fim:</span> <span className="text-orange-300">{entry.routeEnd || 'N/A'}</span></div>
                            </>
                          ) : (
                            <p className="leading-relaxed text-gray-300">
                              {entry.notes || <span className="italic text-gray-600">Nenhuma observação</span>}
                            </p>
                          )}

                          {entry.fuelConsumed ? (
                            <div className="inline-block rounded border border-red-500/20 bg-red-500/10 px-2 py-1 text-[10px] text-red-400">
                              {entry.fuelConsumed} litros gastos
                            </div>
                          ) : null}
                        </div>

                        <div className="mt-4 text-right text-lg font-black text-orange-500">
                          {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(entry.cost)}
                        </div>
                      </div>
                    ))
                  )}
                </div>

                <div className="hidden h-full overflow-auto md:block">
                  <Table>
                    <TableHeader className="sticky top-0 bg-black/40 backdrop-blur-md">
                      <TableRow className="border-white/5">
                        <TableHead className="w-12 text-[10px] font-black uppercase text-gray-500">Tipo</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Data e Veículo</TableHead>
                        <TableHead className="text-[10px] font-black uppercase text-gray-500">Informações</TableHead>
                        <TableHead className="text-right text-[10px] font-black uppercase text-gray-500">Custo</TableHead>
                        <TableHead className="w-12 text-center text-[10px] font-black uppercase text-gray-500">Ações</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {entries.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={5} className="py-10 text-center">
                            <div className="flex flex-col items-center justify-center opacity-50">
                              <FileText size={32} className="mb-2" />
                              <p className="font-bold text-gray-400">Nenhum registro logístico incluído.</p>
                            </div>
                          </TableCell>
                        </TableRow>
                      ) : (
                        entries.map((entry) => (
                          <TableRow key={entry.id} className="border-white/5 hover:bg-white/5">
                            <TableCell className="align-top">
                              <div className="inline-flex rounded-lg bg-white/10 p-2" title={entry.type.toUpperCase()}>
                                {getTypeIcon(entry.type)}
                              </div>
                            </TableCell>
                            <TableCell className="align-top">
                              <div className="text-sm font-bold text-gray-200">{entry.vehicle}</div>
                              <div className="mt-1 flex items-center gap-2 text-xs text-gray-500">
                                <Calendar size={12} /> {format(new Date(entry.date), 'dd/MM/yyyy')}
                              </div>
                              <div className="mt-0.5 text-[10px] text-gray-400">Motorista: {entry.user}</div>
                            </TableCell>
                            <TableCell className="max-w-[260px] align-top">
                              {entry.routeStart || entry.routeEnd ? (
                                <div className="space-y-1 text-xs">
                                  <div className="flex items-start gap-1"><span className="w-12 shrink-0 font-bold text-gray-500">Início:</span> <span className="leading-tight text-gray-300">{entry.routeStart || 'N/A'}</span></div>
                                  <div className="flex items-start gap-1"><span className="w-12 shrink-0 font-bold text-gray-500">Fim:</span> <span className="leading-tight text-orange-300">{entry.routeEnd || 'N/A'}</span></div>
                                  {entry.fuelConsumed ? <div className="mt-1 inline-block rounded border border-red-500/20 bg-red-500/10 px-1.5 text-[10px] text-red-400">{entry.fuelConsumed} litros gastos</div> : null}
                                </div>
                              ) : (
                                <p className="mt-1 line-clamp-3 text-xs leading-relaxed text-gray-300">
                                  {entry.notes || <span className="italic text-gray-600">Nenhuma observação</span>}
                                </p>
                              )}
                            </TableCell>
                            <TableCell className="text-right align-top font-black text-orange-500">
                              {new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(entry.cost)}
                            </TableCell>
                            <TableCell className="align-top text-center">
                              {!readOnly ? (
                                <Button variant="ghost" size="icon" onClick={() => removeEntry(entry.id)} className="h-8 w-8 text-red-500 hover:bg-red-500/10 hover:text-red-400">
                                  <Trash2 size={14} />
                                </Button>
                              ) : null}
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
