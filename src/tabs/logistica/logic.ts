import { fixText } from '../../lib/text';

export const HQ_OPTION = {
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

export function getNumericCoordinate(value?: number) {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

export function haversineKm(startLat: number, startLng: number, endLat: number, endLng: number) {
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

export function normalizeLookupKey(value: string) {
  return fixText(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .trim();
}

export function extractCityFromAddress(address: string) {
  const normalized = fixText(address);
  const hyphenParts = normalized.split(' - ').map((part) => part.trim()).filter(Boolean);
  const ceIndex = hyphenParts.findIndex((part) => normalizeLookupKey(part) === 'ce');
  if (ceIndex > 0) {
    return hyphenParts[ceIndex - 1];
  }

  const commaParts = normalized.split(',').map((part) => part.trim()).filter(Boolean);
  return commaParts.length > 0 ? commaParts[commaParts.length - 1] : normalized;
}

export function inferCoordinatesFromText(...values: Array<string | undefined>) {
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

export async function geocodeAddressInBrowser(address: string) {
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

export async function getRouteDistanceInBrowser(
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
      ? 'Aproximacao por cidade'
      : 'Linha reta',
  };
}
