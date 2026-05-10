f = r'c:\Users\sagal\OneDrive\Documentos\GitHub\dinamica\src\tabs\financeiro\Leandro.tsx'
with open(f, 'r', encoding='utf-8-sig') as fh:
    text = fh.read()

replacements = [
    ('\u00e2\u20ac\u201d', '\u2014'),
    ('\u00e2\u20ac\u201c', '\u2013'),
    ('\u00e2\u201d\u20ac', '\u2500'),
    ('\u00c3\u00a3', '\u00e3'),
    ('\u00c3\u00a7', '\u00e7'),
    ('\u00c3\u00a9', '\u00e9'),
    ('\u00c3\u00ad', '\u00ed'),
    ('\u00c3\u00b5', '\u00f5'),
    ('\u00c3\u00ba', '\u00fa'),
    ('\u00c3\u00a1', '\u00e1'),
    ('\u00c3\u00b3', '\u00f3'),
    ('\u00c3\u00a0', '\u00e0'),
    ('\u00c3\u0087', '\u00c7'),
    ('\u00c3\u0089', '\u00c9'),
    ('\u00c3\u0081', '\u00c1'),
]

for bad, good in replacements:
    count = text.count(bad)
    if count:
        print(f'  {repr(bad)} -> {repr(good)}: {count}x')
    text = text.replace(bad, good)

with open(f, 'w', encoding='utf-8') as fh:
    fh.write(text)
print('Salvo.')
