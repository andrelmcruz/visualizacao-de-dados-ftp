#!/usr/bin/env python3
"""
Como ``valores_unicos_colunas_br_vta.py``, mas:
- Não calcula nem grava somatórios de vendas por SEGMENTACAO_CATEGORIA (só valor + quantidade).
- Gera um segundo CSV com linhas filtradas ("visão diária por categoria"):
  - PERIODO: só valores que parecem data (ex.: 2025-03-24 00:00:00); exclui agregados tipo
    FULLYEAR_ACTUAL, YTD_ANTERIOR, etc.
  - SEGMENTACAO_CATEGORIA = TOTAL
  - SEGMENTACAO_MERCADO = TOTAL GERAL
  - SAME_STORE = BASE TOTAL
  - TIPO_CONTEUDO = TOTAL
  - SEGMENTACAO_PRODUTO = CATEGORIA

  Colunas da visão: ficheiro, período, descrição (categoria), DN_TOTAL, DP_TOTAL,
  VENDAS_VALOR, VENDAS_VOLUME_gr_ml, VENDAS_UNITARIAS.

Saída em ``output/`` junto ao script (ou ``--pasta-output DIR``):
  resultado_unicos_<nome_da_pasta>_<nome_do_csv_sem_extensão>.csv
  resultado_filtrado_<nome_da_pasta>_<nome_do_csv_sem_extensão>.csv

Uso padrão (sem argumentos): processa todos os CSV/ZIP em ``input/`` junto ao script.
  python valores_unicos_br_vta_visao_filtrada.py

Com ``--pasta``, ``nome_da_pasta`` é o último segmento do caminho indicado.
Com ``--ficheiro``, é o nome da pasta que contém o CSV. Com ``--zip``, a pasta
que contém o ficheiro .zip.

``--batch`` (só com ``--pasta`` ou pasta posicional): processa ``.zip`` na pasta e ``.csv``
cujo nome começa por ``--prefixo`` (default ``BR_VTA``). Outros CSV (ex.: ``BR_PRD``) são
ignorados; zips sem membro com ``--prefixo-membro-zip`` são ignorados. Não entra em subpastas.
Mostra progresso ``[k/N]`` no stderr por ficheiro e o tempo total do lote no fim.

Exemplo:
  python valores_unicos_br_vta_visao_filtrada.py --pasta "E:\\pasta\\com\\BR_VTA"
  python valores_unicos_br_vta_visao_filtrada.py --zip "C:\\ficheiro.zip" --prefixo-membro-zip BR_VTA
  python valores_unicos_br_vta_visao_filtrada.py --pasta "E:\\historico\\alimentar_1" --batch
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from ler_headers_br_vta import (
    _encontrar_ficheiro,
    _headers,
    _inferir_delimitador,
)

_COL_RE_DATA = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\b")

_FILTROS_DIM = {
    "segmentacao_categoria": "TOTAL",
    "segmentacao_mercado": "TOTAL GERAL",
    "same_Store": "BASE TOTAL",
    "tipo_conteudo": "TOTAL",
    "segmentacao_produto": "CATEGORIA",
}

_COLS_VISAO_METRICAS = (
    "DN_TOTAL",
    "DP_TOTAL",
    "VENDAS_VALOR",
    "VENDAS_VOLUME_gr_ml",
    "VENDAS_UNITARIAS",
)

_SCRIPT_DIR = Path(__file__).resolve().parent
_INVALID_NOS_NOMES = '<>:"/\\|?*'


def _dir_output_base(ns: argparse.Namespace) -> Path:
    """Pasta dos CSV por defeito; relativos resolvem na pasta do script."""
    if ns.pasta_output is None:
        base = _SCRIPT_DIR / "output"
    else:
        p = ns.pasta_output.expanduser()
        base = p if p.is_absolute() else (_SCRIPT_DIR / p)
    try:
        return base.resolve()
    except OSError:
        return base


def _segmento_seguro(nome: str, fallback: str) -> str:
    t = nome.strip().replace("\n", "").replace("\r", "")
    for c in _INVALID_NOS_NOMES:
        t = t.replace(c, "_")
    return t if t else fallback


def _csv_unicos_padrao(dir_base: Path, nome_pasta: str, nome_ficheiro_stem: str) -> Path:
    p = _segmento_seguro(nome_pasta, "pasta")
    f = _segmento_seguro(nome_ficheiro_stem, "arquivo")
    return dir_base / f"resultado_unicos_{p}_{f}.csv"


def _csv_filtrado_padrao(dir_base: Path, nome_pasta: str, nome_ficheiro_stem: str) -> Path:
    p = _segmento_seguro(nome_pasta, "pasta")
    f = _segmento_seguro(nome_ficheiro_stem, "arquivo")
    return dir_base / f"resultado_filtrado_{p}_{f}.csv"


COLUNAS_DESEJADAS: list[str] = [
    "perido",
    "segmentacao_categoria",
    "segmentacao_mercado",
    "mercado",
    "segmentacao_produto",
    "DESCRICAO_PRODUTO",
    "same_Store",
    "tipo_conteudo",
]


def _norm(nome: str) -> str:
    return nome.strip().casefold()


def _indice_por_nome(headers: list[str], pedido: str) -> tuple[int, str]:
    ped = _norm(pedido)
    alvos = [_norm(h) for h in headers]

    def procura(needle: str) -> int | None:
        for i, h in enumerate(alvos):
            if h == needle:
                return i
        return None

    idx = procura(ped)
    if idx is not None:
        return idx, headers[idx]

    if ped == "perido":
        j = procura("periodo")
        if j is not None:
            return j, headers[j]
    if ped == "periodo":
        j = procura("perido")
        if j is not None:
            return j, headers[j]

    raise KeyError(pedido)


def _mapear_colunas(headers: list[str], pedidos: list[str]) -> dict[str, tuple[int, str]]:
    out: dict[str, tuple[int, str]] = {}
    for p in pedidos:
        try:
            out[p] = _indice_por_nome(headers, p)
        except KeyError:
            disponiveis = "\n  ".join(headers[:80])
            mais = len(headers) - 80
            tail = f"\n  … (+{mais} colunas)" if mais > 0 else ""
            sys.exit(
                f"Coluna pedida {p!r} não encontrada no header.\n"
                f"Primeiras colunas do ficheiro:\n  {disponiveis}{tail}"
            )
    return out


def _encoding_do_bloco(bloco: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            bloco.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _cell(row: list[str], idx: int) -> str:
    return row[idx].strip() if idx < len(row) else ""


def _periodo_eh_data(val: str) -> bool:
    return bool(_COL_RE_DATA.match(val or ""))


def _eq_dim(val_celula: str, esperado: str) -> bool:
    return _norm(val_celula) == _norm(esperado)


def _encontrar_membro_zip(zf: zipfile.ZipFile, prefixo: str) -> str:
    pref = prefixo.strip().upper()
    if not pref:
        sys.exit("prefixo-membro-zip vazio")
    candidatos = sorted(
        zi.filename
        for zi in zf.infolist()
        if not zi.is_dir() and Path(zi.filename).name.upper().startswith(pref)
    )
    if not candidatos:
        nomes = "\n  ".join(sorted(zi.filename for zi in zf.infolist() if not zi.is_dir())[:40])
        sys.exit(
            f"Nenhum membro no zip com nome a começar por {prefixo!r}.\n"
            f"Membros (primeiros):\n  {nomes}"
        )
    if len(candidatos) > 1:
        outros = ", ".join(Path(c).name for c in candidatos[1:6])
        if len(candidatos) > 6:
            outros += ", …"
        print(
            f"Aviso: {len(candidatos)} membros com esse prefixo; a usar só o primeiro:\n"
            f"  {Path(candidatos[0]).name}\n  (outros: {outros})\n",
            file=sys.stderr,
            flush=True,
        )
    return candidatos[0]


def _listar_csv_e_zip_na_pasta(pasta: Path, *, prefixo_nome_csv: str) -> list[Path]:
    """
    Lista .zip na pasta e .csv cujo basename começa por prefixo_nome_csv (ex.: BR_VTA).
    Ignora outros CSV (ex.: BR_PRD) para pastas com vários tipos de ficheiro.
    """
    if not pasta.is_dir():
        sys.exit(f"Não é uma pasta: {pasta}")
    pref = prefixo_nome_csv.strip().upper()
    ficheiros: list[Path] = []
    for p in pasta.iterdir():
        if not p.is_file():
            continue
        suf = p.suffix.casefold()
        if suf == ".csv":
            if pref and not p.name.upper().startswith(pref):
                continue
            ficheiros.append(p)
        elif suf == ".zip":
            ficheiros.append(p)
    ficheiros.sort(key=lambda q: q.name.casefold())
    return ficheiros


def _abrir_zip_e_membro_batch(
    caminho_zip: Path, *, prefixo_membro: str
) -> tuple[Path, str] | None:
    if not caminho_zip.is_file():
        print(f"Ignorado (não é ficheiro): {caminho_zip}", file=sys.stderr, flush=True)
        return None
    try:
        zf = zipfile.ZipFile(caminho_zip)
    except zipfile.BadZipFile as e:
        print(f"Ignorado (zip inválido) {caminho_zip.name}: {e}", file=sys.stderr, flush=True)
        return None
    with zf:
        pref = prefixo_membro.strip().upper()
        if not pref:
            print("prefixo-membro-zip vazio; zip ignorado.", file=sys.stderr, flush=True)
            return None
        candidatos = sorted(
            zi.filename
            for zi in zf.infolist()
            if not zi.is_dir() and Path(zi.filename).name.upper().startswith(pref)
        )
        if not candidatos:
            print(
                f"Ignorado (nenhum membro começando por {prefixo_membro!r}): {caminho_zip.name}",
                file=sys.stderr,
                flush=True,
            )
            return None
        if len(candidatos) > 1:
            outros = ", ".join(Path(c).name for c in candidatos[1:6])
            if len(candidatos) > 6:
                outros += ", …"
            print(
                f"Aviso: {len(candidatos)} membros com esse prefixo em {caminho_zip.name!r}; "
                f"a usar só o primeiro:\n  {Path(candidatos[0]).name}\n  (outros: {outros})\n",
                file=sys.stderr,
                flush=True,
            )
        membro = candidatos[0]
    return caminho_zip, membro


def _abrir_zip_e_membro(
    caminho_zip: Path, *, membro_fixo: str | None, prefixo_membro: str
) -> tuple[Path, str]:
    if not caminho_zip.is_file():
        sys.exit(f"Não é um ficheiro: {caminho_zip}")
    try:
        with zipfile.ZipFile(caminho_zip) as zf:
            if membro_fixo is not None:
                if membro_fixo not in zf.namelist():
                    sys.exit(f"Membro não encontrado no zip: {membro_fixo!r}")
                membro = membro_fixo
            else:
                membro = _encontrar_membro_zip(zf, prefixo_membro)
    except zipfile.BadZipFile as e:
        sys.exit(f"Zip inválido: {caminho_zip}\n{e}")
    return caminho_zip, membro


def _stem_para_nome_saida_zip_em_lote(caminho_zip: Path, membro: str) -> str:
    return f"{caminho_zip.stem}__{Path(membro).stem}"


def _saidas_um(
    ns: argparse.Namespace,
    dir_output: Path,
    nome_pasta: str,
    stem_arquivo: str,
) -> tuple[Path | None, Path | None]:
    if ns.stdout_only_unicos:
        caminho_unicos: Path | None = None
    else:
        caminho_unicos = (
            ns.out_unicos
            if ns.out_unicos is not None
            else _csv_unicos_padrao(dir_output, nome_pasta, stem_arquivo)
        )
    if ns.sem_visao:
        caminho_visao: Path | None = None
    else:
        caminho_visao = (
            ns.out_visao
            if ns.out_visao is not None
            else _csv_filtrado_padrao(dir_output, nome_pasta, stem_arquivo)
        )
    return caminho_unicos, caminho_visao


def _executar_um_job(
    ns: argparse.Namespace,
    colunas_pedidas: list[str],
    *,
    dir_output: Path,
    path_plano: Path | None,
    zip_tuplo: tuple[Path, str] | None,
    nome_pasta_saida: str,
    nome_ficheiro_stem_saida: str,
) -> None:
    caminho_unicos, caminho_visao = _saidas_um(ns, dir_output, nome_pasta_saida, nome_ficheiro_stem_saida)
    _processar_ficheiro(
        ns,
        caminho_unicos,
        caminho_visao,
        colunas_pedidas,
        path_plano=path_plano,
        zip_tuplo=zip_tuplo,
    )


def _mapear_filtros_e_visao(header_cells: list[str]) -> tuple[dict[str, tuple[int, str]], dict[str, tuple[int, str]]]:
    chaves_filtro = list(_FILTROS_DIM.keys())
    mapa_f = _mapear_colunas(header_cells, chaves_filtro)
    try:
        idx_p, nome_p = _indice_por_nome(header_cells, "perido")
    except KeyError as e:
        sys.exit(f"Coluna de período necessária para a visão: {e}")
    mapa_f["perido"] = (idx_p, nome_p)

    cols_visao = ["DESCRICAO_PRODUTO", *_COLS_VISAO_METRICAS]
    mapa_v: dict[str, tuple[int, str]] = {}
    for c in cols_visao:
        try:
            mapa_v[c] = _indice_por_nome(header_cells, c)
        except KeyError:
            sys.exit(
                f"Coluna {c!r} necessária para a visão filtrada não encontrada no header.\n"
                f"Esperadas: {cols_visao}"
            )
    return mapa_f, mapa_v


def _linha_passa_filtro(row: list[str], mapa_f: dict[str, tuple[int, str]]) -> bool:
    idx_per, _ = mapa_f["perido"]
    if not _periodo_eh_data(_cell(row, idx_per)):
        return False
    for chave, esperado in _FILTROS_DIM.items():
        idx, _ = mapa_f[chave]
        if not _eq_dim(_cell(row, idx), esperado):
            return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Valores únicos (sem somatório) + visão filtrada diária por categoria (BR_VTA)."
    )
    ap.add_argument("pasta", nargs="?", type=Path, help="Pasta com o ficheiro BR_VTA*")
    ap.add_argument("--pasta", dest="pasta_opt", type=Path, default=None)
    ap.add_argument("--ficheiro", type=Path, default=None)
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--membro-zip", type=str, default=None, metavar="NOME")
    ap.add_argument("--prefixo-membro-zip", default="BR_VTA")
    ap.add_argument("--prefixo", default="BR_VTA")
    ap.add_argument(
        "--pasta-output",
        type=Path,
        default=None,
        metavar="DIR",
        help=(
            "Pasta onde gravar resultado_unicos_* e resultado_filtrado_* "
            "(default: output/ junto ao script)."
        ),
    )
    ap.add_argument("--out-unicos", type=Path, default=None)
    ap.add_argument("--out-visao", type=Path, default=None)
    ap.add_argument("--stdout-only-unicos", action="store_true")
    ap.add_argument("--sem-visao", action="store_true")
    ap.add_argument("--amostra-mb", type=float, default=4.0)
    ap.add_argument("--progresso-a-cada", type=int, default=500_000, metavar="N")
    ap.add_argument("--colunas", default=None, metavar="NOMES")
    ap.add_argument("--batch", action="store_true")
    ns = ap.parse_args()

    # Default sem argumentos: processa input/ em batch mode
    if ns.zip is None and ns.ficheiro is None and ns.pasta is None and ns.pasta_opt is None:
        ns.pasta = _SCRIPT_DIR / "input"
        ns.batch = True

    if ns.colunas:
        colunas_pedidas = [c.strip() for c in ns.colunas.split(",") if c.strip()]
        if not colunas_pedidas:
            ap.error("--colunas não pode ficar vazio")
    else:
        colunas_pedidas = list(COLUNAS_DESEJADAS)

    dir_output = _dir_output_base(ns)

    if ns.batch:
        if ns.zip is not None or ns.ficheiro is not None:
            ap.error("--batch só pode ser usado com uma pasta (posicional ou --pasta)")
        if ns.stdout_only_unicos:
            ap.error("--batch não combina com --stdout-only-unicos")
        if ns.out_unicos is not None or ns.out_visao is not None:
            ap.error("--batch não combina com --out-unicos/--out-visao (cada ficheiro gera o seu nome)")
        if ns.membro_zip is not None:
            ap.error("--batch não combina com --membro-zip (uso --prefixo-membro-zip em cada zip)")
        pasta_bt = ns.pasta_opt or ns.pasta
        if pasta_bt is None:
            ap.error("--batch requer pasta (argumento ou --pasta)")
        pasta_bt = pasta_bt.expanduser()
        try:
            pasta_bt = pasta_bt.resolve()
        except OSError:
            pass
        lote = _listar_csv_e_zip_na_pasta(pasta_bt, prefixo_nome_csv=ns.prefixo)
        if not lote:
            sys.exit(
                f"Nenhum ficheiro .zip ou .csv com nome começando por {ns.prefixo!r} directamente em:\n"
                f"  {pasta_bt}"
            )
        n = len(lote)
        nome_pasta_saida_bt = pasta_bt.name
        t0_lote = time.perf_counter()
        print(f"Lote batch: {n} ficheiro(s), pasta nome saída: {nome_pasta_saida_bt!r}", file=sys.stderr, flush=True)
        print(f"Pasta de saída dos CSV: {dir_output}", file=sys.stderr, flush=True)
        print("", file=sys.stderr, flush=True)
        for k, caminho_item in enumerate(lote, start=1):
            print(f"======== [{k}/{n}] {caminho_item.name}", file=sys.stderr, flush=True)
            t1 = time.perf_counter()
            if caminho_item.suffix.casefold() == ".csv":
                _executar_um_job(
                    ns,
                    colunas_pedidas,
                    dir_output=dir_output,
                    path_plano=caminho_item,
                    zip_tuplo=None,
                    nome_pasta_saida=nome_pasta_saida_bt,
                    nome_ficheiro_stem_saida=caminho_item.stem,
                )
            else:
                ab = _abrir_zip_e_membro_batch(caminho_item, prefixo_membro=ns.prefixo_membro_zip)
                if ab is None:
                    dt_item = time.perf_counter() - t1
                    print(f"-------- [{k}/{n}] ignorado ({dt_item:.1f} s)\n", file=sys.stderr, flush=True)
                    continue
                zp, memb = ab
                stem_out = _stem_para_nome_saida_zip_em_lote(zp, memb)
                _executar_um_job(
                    ns,
                    colunas_pedidas,
                    dir_output=dir_output,
                    path_plano=None,
                    zip_tuplo=(zp, memb),
                    nome_pasta_saida=nome_pasta_saida_bt,
                    nome_ficheiro_stem_saida=stem_out,
                )
            dt_item = time.perf_counter() - t1
            print(f"-------- [{k}/{n}] concluído em {dt_item:.1f} s\n", file=sys.stderr, flush=True)
        dt_total = time.perf_counter() - t0_lote
        minutos = dt_total / 60.0
        print(
            f"Tempo total do lote ({n} ficheiro(s)): {dt_total:.1f} s ({minutos:.1f} min)",
            file=sys.stderr,
            flush=True,
        )
        return

    tem_zip = ns.zip is not None
    tem_ficheiro = ns.ficheiro is not None
    pasta = ns.pasta_opt or ns.pasta
    tem_pasta = pasta is not None
    if int(tem_zip) + int(tem_ficheiro) + int(tem_pasta) != 1:
        ap.error("indique exactamente uma origem: --zip, --ficheiro, ou pasta")

    zip_tuplo: tuple[Path, str] | None = None
    ficheiro: Path | None = None
    nome_pasta_saida: str
    nome_ficheiro_stem_saida: str

    if tem_zip:
        caminho_zip = ns.zip.expanduser()
        try:
            caminho_zip = caminho_zip.resolve()
        except OSError:
            pass
        caminho_zip, membro = _abrir_zip_e_membro(
            caminho_zip,
            membro_fixo=ns.membro_zip,
            prefixo_membro=ns.prefixo_membro_zip,
        )
        zip_tuplo = (caminho_zip, membro)
        nome_pasta_saida = caminho_zip.parent.name
        nome_ficheiro_stem_saida = Path(membro).stem
    elif tem_ficheiro:
        fi = ns.ficheiro.expanduser()
        try:
            fi = fi.resolve()
        except OSError:
            pass
        if not fi.is_file():
            sys.exit(f"Não é um ficheiro: {fi}")
        ficheiro = fi
        nome_pasta_saida = fi.parent.name
        nome_ficheiro_stem_saida = fi.stem
    else:
        pasta = pasta.expanduser()
        try:
            pasta = pasta.resolve()
        except OSError:
            pass
        ficheiro = _encontrar_ficheiro(pasta, ns.prefixo)
        nome_pasta_saida = pasta.name
        nome_ficheiro_stem_saida = ficheiro.stem

    _executar_um_job(
        ns,
        colunas_pedidas,
        dir_output=dir_output,
        path_plano=ficheiro,
        zip_tuplo=zip_tuplo,
        nome_pasta_saida=nome_pasta_saida,
        nome_ficheiro_stem_saida=nome_ficheiro_stem_saida,
    )


def _processar_ficheiro(
    ns: argparse.Namespace,
    saida_unicos: Path | None,
    saida_visao: Path | None,
    colunas: list[str],
    *,
    path_plano: Path | None,
    zip_tuplo: tuple[Path, str] | None,
) -> None:
    assert (path_plano is None) ^ (zip_tuplo is None)

    max_bytes = max(64 * 1024, int(ns.amostra_mb * 1024 * 1024))

    if path_plano is not None:
        with path_plano.open("rb") as bf:
            bloco = bf.read(max_bytes)
        nome_arquivo = path_plano.name
        etiqueta = str(path_plano)

        @contextmanager
        def abrir_texto_csv():
            with path_plano.open("r", encoding=encoding, newline="", errors="replace") as f:
                yield f

    else:
        assert zip_tuplo is not None
        caminho_zip, membro = zip_tuplo
        with zipfile.ZipFile(caminho_zip) as zf:
            with zf.open(membro, "r") as bf:
                bloco = bf.read(max_bytes)
        nome_arquivo = Path(membro).name
        etiqueta = f"{caminho_zip} :: {membro}"

        @contextmanager
        def abrir_texto_csv():
            with zipfile.ZipFile(caminho_zip) as zf:
                with zf.open(membro, "r") as raw:
                    with io.TextIOWrapper(
                        raw,
                        encoding=encoding,
                        newline="",
                        errors="replace",
                    ) as f:
                        yield f

    encoding = _encoding_do_bloco(bloco)
    texto = bloco.decode(encoding) if encoding != "latin-1" else bloco.decode("latin-1", errors="replace")
    linhas = [ln.strip("\r\n") for ln in texto.splitlines() if ln.strip("\r\n")]
    if not linhas:
        sys.exit("Ficheiro sem linhas na amostra inicial.")
    primeira = linhas[0]
    extra = "\n".join(linhas[1:5]) if len(linhas) > 1 else ""
    delim = _inferir_delimitador(primeira, extra)
    header_cells = _headers(primeira, delim)
    mapa = _mapear_colunas(header_cells, colunas)
    mapa_f: dict[str, tuple[int, str]] | None = None
    mapa_v: dict[str, tuple[int, str]] | None = None
    nome_header_periodo = ""
    nome_header_desc = ""
    nomes_metricas_real: list[str] = []
    if saida_visao is not None:
        mapa_f, mapa_v = _mapear_filtros_e_visao(header_cells)
        nome_header_periodo = mapa_f["perido"][1]
        nome_header_desc = mapa_v["DESCRICAO_PRODUTO"][1]
        nomes_metricas_real = [mapa_v[c][1] for c in _COLS_VISAO_METRICAS]

    print(f"Fonte: {etiqueta}", file=sys.stderr, flush=True)
    print(f"Encoding: {encoding}  Delimitador: {repr(delim)}", file=sys.stderr, flush=True)
    for pedido, (idx, nome_real) in mapa.items():
        print(f"  coluna {pedido!r} -> índice {idx} ({nome_real!r})", file=sys.stderr, flush=True)
    if saida_visao is not None:
        print("Visão filtrada: período só datas; dims como no código.", file=sys.stderr, flush=True)
    if saida_unicos is not None:
        print(f"A gravar únicos em: {saida_unicos}", file=sys.stderr, flush=True)
    if saida_visao is not None:
        print(f"A gravar visão em: {saida_visao}", file=sys.stderr, flush=True)
    print("", file=sys.stderr, flush=True)

    contagens: dict[str, dict[str, int]] = {p: defaultdict(int) for p in colunas}
    linhas_visao = 0

    cabecalho_visao: list[str] = []
    if saida_visao is not None:
        cabecalho_visao = [
            "nome_arquivo",
            nome_header_periodo,
            nome_header_desc,
            *nomes_metricas_real,
        ]

    t0 = time.perf_counter()
    linhas_dados = 0

    out_visao_fp = None
    writer_visao = None
    if saida_visao is not None:
        saida_visao.parent.mkdir(parents=True, exist_ok=True)
        out_visao_fp = saida_visao.open("w", encoding="utf-8-sig", newline="")
        writer_visao = csv.writer(out_visao_fp)
        writer_visao.writerow(cabecalho_visao)

    try:
        with abrir_texto_csv() as f:
            reader = csv.reader(f, delimiter=delim)
            try:
                next(reader)
            except StopIteration:
                sys.exit("Ficheiro sem header.")

            for row in reader:
                linhas_dados += 1
                for pedido, (idx, _nome_real) in mapa.items():
                    val = _cell(row, idx)
                    contagens[pedido][val] += 1

                if writer_visao is not None and mapa_f is not None and mapa_v is not None:
                    if _linha_passa_filtro(row, mapa_f):
                        idx_p, _ = mapa_f["perido"]
                        idx_d, _ = mapa_v["DESCRICAO_PRODUTO"]
                        metric_vals = [_cell(row, mapa_v[c][0]) for c in _COLS_VISAO_METRICAS]
                        writer_visao.writerow(
                            [nome_arquivo, _cell(row, idx_p), _cell(row, idx_d), *metric_vals]
                        )
                        linhas_visao += 1

                if ns.progresso_a_cada > 0 and linhas_dados % ns.progresso_a_cada == 0:
                    dt = time.perf_counter() - t0
                    rate = linhas_dados / dt if dt > 0 else 0
                    print(
                        f"… {linhas_dados} linhas de dados  (~{rate:.0f} linhas/s)",
                        file=sys.stderr,
                        flush=True,
                    )
    finally:
        if out_visao_fp is not None:
            out_visao_fp.close()

    elapsed = time.perf_counter() - t0
    print(f"Linhas de dados lidas: {linhas_dados}  (tempo: {elapsed:.1f} s)", file=sys.stderr, flush=True)
    print(f"Linhas na visão filtrada: {linhas_visao}", file=sys.stderr, flush=True)

    cabecalho_unicos = ["nome_arquivo", "nome_header", "valor", "quantidade"]

    def escrever_unicos(writer: csv.writer) -> None:
        writer.writerow(cabecalho_unicos)
        for pedido in colunas:
            _idx, nome_header = mapa[pedido]
            for valor, qtd in sorted(
                contagens[pedido].items(),
                key=lambda kv: (kv[0] == "", kv[0].casefold()),
            ):
                writer.writerow([nome_arquivo, nome_header, valor, qtd])

    if saida_unicos is not None:
        saida_unicos.parent.mkdir(parents=True, exist_ok=True)
        with saida_unicos.open("w", encoding="utf-8-sig", newline="") as out_fp:
            escrever_unicos(csv.writer(out_fp))
    else:
        escrever_unicos(csv.writer(sys.stdout, lineterminator="\n"))


if __name__ == "__main__":
    main()
