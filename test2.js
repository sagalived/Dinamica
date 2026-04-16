import axios from "axios";

const username = "dinamicaempreendimentos-jrmorais";
const password = "5jT2uxIW6YYAPL2epk9QUUvCEGM2eX9z";
const instance = "dinamicaempreendimentos";

const API = axios.create({
  baseURL: `https://api.sienge.com.br/${instance}/public/api/v1`,
  headers: {
    Authorization: `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`,
    "Content-Type": "application/json",
    Accept: "application/json"
  },
  timeout: 60000
});

async function test() {
  try {
    const res = await API.get("/creditors", { params: { limit: 1 } });
    console.log("Creditors =>", res.status, res.data.results?.[0] || res.data);
  } catch (e) {
    console.log("Creditors Error:", e.response?.status, e.response?.data);
  }
}

test();
