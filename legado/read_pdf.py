import sys
try:
    import fitz
except ImportError:
    import os
    os.system("pip install pymupdf")
    import fitz

doc = fitz.open("relatorio.pdf")
text = ""
for page in doc:
    text += page.get_text()

with open("pdf_text.txt", "w", encoding="utf-8") as f:
    f.write(text)
print("Saved to pdf_text.txt")
