#!/usr/bin/env python3
"""
Encontra na pasta indicada um ficheiro cujo nome começa por um prefixo (default BR_VTA),
lê só o início do ficheiro (adequado para ficheiros de dezenas de GB) e mostra os headers
(primeira linha, delimitador inferido).

Exemplos:
  python ler_headers_br_vta.py --pasta "E:\\arquivos google\\br_ventas_unificadas_periodo_Google_ALIMENTAR_3_SEMANAL_33_ACV_20260420"
  python ler_headers_br_vta.py "D:\\outra\\pasta"
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def _encontrar_ficheiro(pasta: Path, prefixo: str) -> Path:
    if not pasta.is_dir():
        sys.exit(f"Não é uma pasta ou não existe: {pasta}")
    pref = prefixo.upper()
    candidatos = sorted(
        p
        for p in pasta.iterdir()
        if p.is_file() and p.name.upper().startswith(pref)
    )
    if not candidatos:
        sys.exit(
            f"Nenhum ficheiro com nome começando por {prefixo!r} (comparação sem distinguir maiúsculas) em:\n  {pasta}"
        )
    if len(candidatos) > 1:
        outros = ", ".join(c.name for c in candidatos[1:6])
        if len(candidatos) > 6:
            outros += ", …"
        print(
            f"Aviso: {len(candidatos)} ficheiros com esse prefixo; a analisar só o primeiro:\n  {candidatos[0].name}\n"
            f"  (outros: {outros})\n",
            file=sys.stderr,
        )
    return candidatos[0]


def _ler_amostra_binaria(caminho: Path, max_bytes: int) -> str:
    with caminho.open("rb") as f:
        bloco = f.read(max_bytes)
    if not bloco:
        sys.exit("Ficheiro vazio.")
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return bloco.decode(enc)
        except UnicodeDecodeError:
            continue
    return bloco.decode("latin-1", errors="replace")


def _primeira_linha_nao_vazia(texto: str) -> str:
    for linha in texto.splitlines():
        s = linha.strip("\r\n")
        if s:
            return s
    sys.exit("Não foi possível encontrar uma primeira linha com conteúdo na amostra.")


def _inferir_delimitador(primeira_linha: str, linhas_extra: str) -> str:
    amostra = primeira_linha + "\n" + linhas_extra
    try:
        d = csv.Sniffer().sniff(amostra[:65536], delimiters=";\t,|")
        delim = d.delimiter
        if isinstance(delim, str) and delim:
            return delim
    except csv.Error:
        pass
    contagens = {
        ";": primeira_linha.count(";"),
        "\t": primeira_linha.count("\t"),
        ",": primeira_linha.count(","),
        "|": primeira_linha.count("|"),
    }
    melhor = max(contagens, key=contagens.get)
    if contagens[melhor] == 0:
        return ";"
    return melhor


def _headers(primeira_linha: str, delimitador: str) -> list[str]:
    reader = csv.reader([primeira_linha], delimiter=delimitador)
    try:
        linha = next(reader)
    except StopIteration:
        return []
    return [c.strip() for c in linha]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Mostra os headers do ficheiro BR_VTA* numa pasta (lê só o início do ficheiro)."
    )
    ap.add_argument(
        "pasta",
        nargs="?",
        type=Path,
        help="Pasta onde procurar o ficheiro (pode omitir se usar --pasta)",
    )
    ap.add_argument(
        "--pasta",
        dest="pasta_opt",
        type=Path,
        default=None,
        help='Caminho da pasta (alternativa ao argumento posicional), ex.: --pasta "E:\\dados\\subpasta"',
    )
    ap.add_argument(
        "--prefixo",
        default="BR_VTA",
        help="Início do nome do ficheiro (default: BR_VTA; comparação sem distinguir maiúsculas no Windows)",
    )
    ap.add_argument(
        "--amostra-mb",
        type=float,
        default=4.0,
        metavar="N",
        help="Quantos MB ler no máximo do início do ficheiro para achar a primeira linha (default: 4)",
    )
    ns = ap.parse_args()
    pasta = ns.pasta_opt or ns.pasta
    if pasta is None:
        ap.error("indique a pasta: ler_headers_br_vta.py CAMINHO   ou   --pasta CAMINHO")

    pasta = pasta.expanduser()
    try:
        pasta = pasta.resolve()
    except OSError:
        pass

    ficheiro = _encontrar_ficheiro(pasta, ns.prefixo)
    max_bytes = max(64 * 1024, int(ns.amostra_mb * 1024 * 1024))

    texto = _ler_amostra_binaria(ficheiro, max_bytes)
    linhas = [ln.strip("\r\n") for ln in texto.splitlines() if ln.strip("\r\n")]
    primeira = linhas[0] if linhas else ""
    extra = "\n".join(linhas[1:5]) if len(linhas) > 1 else ""
    delim = _inferir_delimitador(primeira, extra)
    cols = _headers(primeira, delim)

    tamanho = ficheiro.stat().st_size
    print(f"Ficheiro: {ficheiro}")
    print(f"Tamanho: {tamanho / (1024**3):.2f} GB ({tamanho} bytes)")
    print(f"Delimitador inferido: {repr(delim)}")
    print(f"Número de colunas: {len(cols)}")
    print("Headers:")
    for i, h in enumerate(cols, start=1):
        print(f"  {i:4d}  {h}")


if __name__ == "__main__":
    main()
