import { LinhaCentroDeCusto } from './centrodecustonormalizador';

interface ParametrosFiltroCentroDeCusto {
  linhas: LinhaCentroDeCusto[];
  obras: any[];
  empresaSelecionada: string;
  obraSelecionada: string;
}

function construirIdsDeObras(obras: any[]): Set<string> {
  const ids = new Set<string>();
  for (const obra of obras) {
    if (obra?.id != null) ids.add(String(obra.id));
    if (obra?.code != null) ids.add(String(obra.code));
  }
  return ids;
}

export function filtrarLinhasCentroDeCusto({
  linhas,
  obras,
  empresaSelecionada,
  obraSelecionada,
}: ParametrosFiltroCentroDeCusto): LinhaCentroDeCusto[] {
  const idsValidos = construirIdsDeObras(obras);
  const filtroObraValido = obraSelecionada !== 'all' && idsValidos.has(String(obraSelecionada));

  return linhas.filter((linha) => {
    if (empresaSelecionada !== 'all' && String(linha.companyId ?? '') !== String(empresaSelecionada)) {
      return false;
    }

    if (filtroObraValido) {
      const obraAlvo = String(obraSelecionada);
      const correspondeObra =
        String(linha.buildingId ?? '') === obraAlvo ||
        String(linha.codigoObra ?? '') === obraAlvo;
      if (!correspondeObra) {
        return false;
      }
    }

    return true;
  });
}
