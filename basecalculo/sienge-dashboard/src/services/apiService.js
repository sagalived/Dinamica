export async function apiGet(path, params = {}) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(`/api${normalizedPath}`, window.location.origin);

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "Todos") {
      url.searchParams.set(key, value);
    }
  });

  const token = window.localStorage.getItem("access_token");
  const headers = { Accept: "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;

  const response = await fetch(url.toString(), { headers });
  if (!response.ok) {
    const text = await response.text();
    if (response.status === 401) {
      throw new Error("Backend retornou 401. Faça login primeiro no FastAPI para acessar /api/sienge/*.");
    }
    throw new Error(`Erro ${response.status} em ${path}: ${text}`);
  }

  return response.json();
}

export async function apiPost(path, body) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const url = new URL(`/api${normalizedPath}`, window.location.origin);

  const token = window.localStorage.getItem("access_token");
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
  };
  if (token) headers.Authorization = `Bearer ${token}`;

  const response = await fetch(url.toString(), {
    method: "POST",
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Erro ${response.status} em ${path}: ${text}`);
  }

  const text = await response.text();
  return text ? JSON.parse(text) : {};
}
