# -*- coding: utf-8 -*-
"""
gerar_html.py
Uso: python gerar_html.py
Le os .txt da pasta, injeta os dados no template.html e salva transferencias_ftp.html
"""
import os, re, json, sys

FOLDER   = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(FOLDER, "template.html")
OUTPUT   = os.path.join(FOLDER, "transferencias_ftp.html")

def parse_txt(filepath):
    filename = os.path.basename(filepath)
    with open(filepath, "r", encoding="utf-8-sig") as f:
        content = f.read()
    transfer = {"filename": filename, "fields": [], "linhas": None}
    m = re.search(r"Linhas de dados lidas:\s*([\d,\.]+)", content)
    if m:
        transfer["linhas"] = m.group(1).strip()
    for match in re.finditer(
        r"===\s+\w+\s+\(no ficheiro:\s*['\"]([^'\"]+)['\"]\)\s+\S+\s+(\d+)\s+valor\(es\)\s+.nico\(s\)\s+===\n([\s\S]*?)(?===|\Z)",
        content
    ):
        label  = match.group(1).strip()
        count  = int(match.group(2))
        values = [v.strip() for v in match.group(3).strip().split("\n")
                  if v.strip() and not v.startswith("Linhas")]
        transfer["fields"].append({"label": label, "count": count, "values": values})
    return transfer

def main():
    txt_files = sorted(f for f in os.listdir(FOLDER) if f.endswith(".txt"))
    if not txt_files:
        print("Nenhum .txt encontrado."); sys.exit(1)

    print(f"Encontrados {len(txt_files)} arquivo(s):\n")
    transfers = []
    for fname in txt_files:
        print(f"  Lendo: {fname} ...", end=" ", flush=True)
        t = parse_txt(os.path.join(FOLDER, fname))
        transfers.append(t)
        print(f"{len(t['fields'])} campos, linhas={t['linhas']}")

    if not os.path.exists(TEMPLATE):
        print(f"\nERRO: template.html nao encontrado em {FOLDER}")
        sys.exit(1)

    with open(TEMPLATE, "r", encoding="utf-8") as f:
        template = f.read()

    data_json = json.dumps(transfers, ensure_ascii=False)
    html = template.replace("__DATA_PLACEHOLDER__", data_json)

    with open(OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nArquivo gerado: {OUTPUT}")
    print("Abra no navegador para visualizar.")

if __name__ == "__main__":
    main()
