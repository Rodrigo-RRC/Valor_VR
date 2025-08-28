# scripts/limpeza.py
# -*- coding: utf-8 -*-
"""
Padroniza APENAS o cabeçalho dos .xlsx:
  data/raw/Originais  ->  data/clean

• Mantém dados intactos.
• Salva com sufixo _clean (ex.: ATIVOS_clean.xlsx).
• Se já existir em data/clean, IGNORA.
"""

from pathlib import Path
import unicodedata
import pandas as pd

# Pastas
RAIZ = Path(__file__).resolve().parents[1]
INPUT_DIR = RAIZ / "data" / "raw" / "Originais"
OUTPUT_DIR = RAIZ / "data" / "clean"

def garantir_pastas() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def _remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFD", texto)
    return "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")

def _normalize_name(s: str) -> str:
    if s is None:
        return "COL"
    s = str(s).strip()
    s = _remover_acentos(s)
    s = " ".join(s.split())  # colapsa espaços internos
    # troca qualquer coisa não-alfanum por underscore, sem regex
    out, prev_us = [], False
    for ch in s:
        if ch.isalnum():
            out.append(ch); prev_us = False
        else:
            if not prev_us:
                out.append("_"); prev_us = True
    nome = "".join(out).strip("_").upper()
    return nome if nome else "COL"

def padronizar_colunas(cols) -> list[str]:
    base = [_normalize_name(c) for c in cols]
    vistos, finais = {}, []
    for c in base:
        if c not in vistos:
            vistos[c] = 1; finais.append(c)
        else:
            vistos[c] += 1; finais.append(f"{c}_{vistos[c]}")
    return finais

def ler_excel_sem_mudar_dados(caminho: Path) -> pd.DataFrame:
    df = pd.read_excel(caminho, engine="openpyxl")
    if isinstance(df.columns, pd.MultiIndex):
        novas = []
        for tup in df.columns:
            partes = [str(x) for x in tup if pd.notna(x) and str(x).lower() != "nan"]
            novas.append(" ".join(partes).strip())
        df.columns = novas
    return df

def processar_arquivo(caminho: Path) -> str:
    # Define nome de saída com sufixo _clean (evita duplicar se já tiver)
    stem = caminho.stem
    if not stem.lower().endswith("_clean"):
        stem_out = f"{stem}_clean"
    else:
        stem_out = stem
    out_path = OUTPUT_DIR / f"{stem_out}{caminho.suffix}"

    # Se já existe, ignora
    if out_path.exists():
        return f"🔁 ignorado (já existe): {out_path.name}"

    df = ler_excel_sem_mudar_dados(caminho)
    df.columns = padronizar_colunas(df.columns)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)

    return f"✅ salvo: {out_path.name}"

def main() -> None:
    print(f"📂 Entrada : {INPUT_DIR}")
    print(f"📁 Saída   : {OUTPUT_DIR}")
    garantir_pastas()

    if not INPUT_DIR.exists():
        print("ERRO: pasta de entrada não existe."); return

    arquivos = sorted(INPUT_DIR.glob("*.xlsx"))
    if not arquivos:
        print("Nenhum .xlsx em data/raw/Originais."); return

    for arq in arquivos:
        try:
            msg = processar_arquivo(arq)
            print(f"{arq.name} -> {msg}")
        except Exception as e:
            print(f"{arq.name} -> ❌ erro: {e}")

if __name__ == "__main__":
    main()
