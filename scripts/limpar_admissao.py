# scripts/limpar_admissao.py
# -*- coding: utf-8 -*-
"""
Limpa a planilha ADMISSÃO ABRIL_FORM.xlsx:
- remove colunas UNNAMED
- salva em data/FORM_OK/ADMISSÃO ABRIL_FORM_OK.xlsx
"""

from pathlib import Path
import pandas as pd

# --- Caminhos ---
RAIZ = Path(__file__).resolve().parents[1]          # raiz do projeto (Desafio4_VR)
ETL_OK = RAIZ / "data" / "ETL_OK"                   # entrada (_FORM.xlsx)
FORM_OK = RAIZ / "data" / "FORM_OK"                 # saída (_FORM_OK.xlsx)
FORM_OK.mkdir(parents=True, exist_ok=True)          # cria pasta se não existir

# --- Arquivo alvo ---
file_in = ETL_OK / "ADMISSÃO ABRIL_FORM.xlsx"
file_out = FORM_OK / "ADMISSÃO ABRIL_FORM_OK.xlsx"

# --- Leitura ---
df = pd.read_excel(file_in, engine="openpyxl")

# --- Remover colunas UNNAMED ---
df_clean = df.loc[:, [c for c in df.columns if not str(c).upper().startswith("UNNAMED")]].copy()

# --- Salvar ---
with pd.ExcelWriter(file_out, engine="openpyxl") as wr:
    df_clean.to_excel(wr, index=False)

print(f"✅ Planilha limpa e salva em: {file_out}")
