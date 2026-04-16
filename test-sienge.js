import dotenv from "dotenv";
import axios from "axios";

dotenv.config();

const instance = (process.env.SIENGE_INSTANCE || "dinamicaempreendimentos").split(".")[0];
const username = process.env.SIENGE_USERNAME;
const password = process.env.SIENGE_PASSWORD;

const API = axios.create({
  baseURL: `https://api.sienge.com.br/${instance}/public/api/v1`,
  headers: {
    Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`,
    "Content-Type": "application/json",
    Accept: "application/json"
  },
  timeout: 60000
});

function normalizeResults(data) {
  if (Array.isArray(data?.results)) return data.results;
  if (Array.isArray(data)) return data;
  return [];
}

async function hit(name, path, params = { limit: 1, offset: 0 }) {
  try {
    const res = await API.get(path, { params });
    const results = normalizeResults(res.data);

    console.log(`✅ ${name}: ${res.status} | itens: ${results.length}`);
    if (results[0]) {
      console.log("   campos:", Object.keys(results[0]));
    }
  } catch (e) {
    console.log(`❌ ${name}:`, e.response?.status, e.response?.data || e.message);
  }
}

async function test() {
  console.log("BaseURL:", API.defaults.baseURL);

  await hit("Units of Measure", "/units-of-measure");
  await hit("Enterprises", "/enterprises");
  await hit("Purchase Orders", "/purchase-orders");
  await hit("Bills", "/bills");
  await hit("Accounts Statements", "/accounts-statements");
  await hit("Users", "/users");
  await hit("Companies", "/companies");
}

test();