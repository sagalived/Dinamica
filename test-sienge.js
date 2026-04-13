import axios from 'axios';
import fs from 'fs';
const API = axios.create({
  baseURL: 'https://api.sienge.com.br/dinamicaempreendimentos',
  headers: {
    'Authorization': `Basic ${Buffer.from('dinamicaempreendimentos-jrmorais:5jT2uxIW6YYAPL2epk9QUUvCEGM2eX9z').toString("base64")}`,
    'Content-Type': 'application/json'
  }
});
async function test() {
  try {
    const legacy = await API.get('/api/v1/pedidos-compra?limit=10').catch(e=>null);
    if (legacy && legacy.data) {
        console.log("Legacy fields available in one order:", Object.keys(legacy.data.results[0]));
        console.log("Values related to requester:", {
           solicitante: legacy.data.results[0].nomeSolicitante || legacy.data.results[0].solicitante,
           idSol: legacy.data.results[0].idSolicitante,
           criador: legacy.data.results[0].createdBy || legacy.data.results[0].criadoPor,
           comprador: legacy.data.results[0].nomeComprador || legacy.data.results[0].comprador
        });
    }

  } catch(e) {
    console.error(e);
  }
}
test();
