const decoder = new TextDecoder('utf-8');

function looksMojibake(value: string) {
  return /Ãƒ.|Ã‚.|Ã¢.|Ã¯Â¿Â½|ï¿½|�/.test(value);
}

function decodeLatin1AsUtf8(value: string) {
  const bytes = Uint8Array.from(Array.from(value).map((char) => char.charCodeAt(0) & 0xff));
  return decoder.decode(bytes);
}

function applyCommonFixes(value: string) {
  return value
    .replace(/CONSTRU��O/g, 'CONSTRUÇÃO')
    .replace(/MANUTEN��O/g, 'MANUTENÇÃO')
    .replace(/ESPA�O/g, 'ESPAÇO')
    .replace(/VIV�NCIA/g, 'VIVÊNCIA')
    .replace(/TAU�/g, 'TAUÁ')
    .replace(/TIANGU�/g, 'TIANGUÁ')
    .replace(/QUIXAD�/g, 'QUIXADÁ')
    .replace(/CANIND�/g, 'CANINDÉ')
    .replace(/MARACANA�/g, 'MARACANAÚ')
    .replace(/EDUCA��O/g, 'EDUCAÇÃO')
    .replace(/CI�NCIA/g, 'CIÊNCIA')
    .replace(/CEAR�/g, 'CEARÁ')
    .replace(/REQUALIFICA��O/g, 'REQUALIFICAÇÃO')
    .replace(/REGULARIZA��O/g, 'REGULARIZAÇÃO')
    .replace(/DUPLICA��O/g, 'DUPLICAÇÃO')
    .replace(/AMPLIA��O/g, 'AMPLIAÇÃO')
    .replace(/CONCLUS�O/g, 'CONCLUSÃO')
    .replace(/GEST�O/g, 'GESTÃO')
    .replace(/LICITA��O/g, 'LICITAÇÃO')
    .replace(/PAVIMENTA��O/g, 'PAVIMENTAÇÃO')
    .replace(/SERVI�OS/g, 'SERVIÇOS')
    .replace(/SUBESTA��O/g, 'SUBESTAÇÃO')
    .replace(/EXECU��O/g, 'EXECUÇÃO')
    .replace(/PROTE��O/g, 'PROTEÇÃO')
    .replace(/CAL�ADÃO/g, 'CALÇADÃO')
    .replace(/A�UDE/g, 'AÇUDE')
    .replace(/S�O/g, 'SÃO')
    .replace(/JO�O/g, 'JOÃO')
    .replace(/EL�SIO/g, 'ELÍSIO')
    .replace(/MUNIC�PIO/g, 'MUNICÍPIO')
    .replace(/PR�DIO/g, 'PRÉDIO')
    .replace(/INSTITUI��O/g, 'INSTITUIÇÃO');
}

export function fixText(value: unknown): string {
  if (value == null) return '';
  const text = String(value);
  if (!text) return '';

  try {
    let result = text;

    if (looksMojibake(result)) {
      const decoded = decodeLatin1AsUtf8(result);
      result = decoded || result;
    }

    return applyCommonFixes(result).trim();
  } catch {
    return applyCommonFixes(text).trim();
  }
}
