# scripts/limpar_form_ok.py
# -*- coding: utf-8 -*-
#limpar_form_ok.py
"""
Limpeza das planilhas *_FORM.xlsx (varredura controlada).
- Remove colunas 'UNNAMED:*' (sobras do Excel).
- Aplica regras específicas quando necessário.
- Salva em data/FORM_OK/<NOME>_FORM_OK.xlsx, sem sobrescrever o original.

Cobertura:
  ✔ AFASTAMENTOS_FORM.xlsx
  ✔ APRENDIZ_FORM.xlsx
  ✔ ATIVOS_FORM.xlsx
  ✔ DESLIGADOS_FORM.xlsx
  ✔ EXTERIOR_FORM.xlsx  (renomeia CADASTRO->MATRICULA; VALOR numérico)
  ✔ FÉRIAS/FÉRIAS_FORM.xlsx  (DIAS_DE_FERIAS numérico)
  ✔ ESTÁGIO/ESTAGIO_FORM.xlsx  (sem regra extra, só UNNAMED)
  ✔ Base dias uteis_FORM.xlsx  (coluna de dias útil -> numérica)
  ✔ Base sindicato x valor_FORM.xlsx  (VALOR numérico; ESTADO strip)

Obs.: O arquivo "VR MENSAL 05.2025_FORM.xlsx" é layout de saída -> não passar aqui.
"""

from pathlib import Path
import pandas as pd

def _extrair_dias_uteis_de_excel(path: Path) -> int | None:
    """Acha um número de dias úteis mesmo se estiver como cabeçalho ou célula solta."""
    df = pd.read_excel(path, engine="openpyxl")
    col = next((c for c in df.columns if "DIAS" in str(c).upper()), None)
    if col:
        serie = pd.to_numeric(df[col], errors="coerce").dropna()
        if not serie.empty:
            return int(serie.iloc[0])
    df2 = pd.read_excel(path, engine="openpyxl", header=None)
    vals = pd.to_numeric(df2.stack(), errors="coerce").dropna()
    return int(vals.iloc[0]) if not vals.empty else None

# --- Pastas do projeto ---
RAIZ = Path(__file__).resolve().parents[1]   # raiz do projeto (Desafio4_VR)
ETL_OK = RAIZ / "data" / "ETL_OK"            # entrada (_FORM.xlsx)
FORM_OK = RAIZ / "data" / "FORM_OK"          # saída (_FORM_OK.xlsx)
FORM_OK.mkdir(parents=True, exist_ok=True)   # cria pasta caso não exista

def remove_unnamed(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas cujo nome começa com 'UNNAMED' (sobras do Excel)."""
    keep = [c for c in df.columns if not str(c).upper().startswith("UNNAMED")]
    return df.loc[:, keep].copy()

def to_num(s):
    """Conversão numérica tolerante: erros viram NaN (coerce)."""
    return pd.to_numeric(s, errors="coerce")

def limpar_generico(nome_arquivo: str) -> None:
    """
    Limpa UM arquivo _FORM.xlsx.
    - Remove UNNAMED
    - Aplica regras específicas por tipo
    - Salva como _FORM_OK.xlsx (sem sobrescrever)
    """
    # Ignora locks do Excel (~$arquivo.xlsx)
    if nome_arquivo.startswith("~$"):
        print(f"🔒 Ignorado (lock do Excel): {nome_arquivo}")
        return

    in_path = ETL_OK / nome_arquivo
    if not in_path.exists():
        print(f"⚠️ Arquivo não encontrado: {nome_arquivo}")
        return

    df = pd.read_excel(in_path, engine="openpyxl")
    df = remove_unnamed(df)

    # Normalização simples do nome para checagens (com ou sem acento)
    name_up = nome_arquivo.upper()

    # ---------------- Regras específicas ----------------
    # EXTERIOR: CADASTRO -> MATRICULA; VALOR numérico
    if "EXTERIOR" in name_up:
        if "CADASTRO" in df.columns:
            df = df.rename(columns={"CADASTRO": "MATRICULA"})
        if "VALOR" in df.columns:
            df["VALOR"] = to_num(df["VALOR"])

    # FÉRIAS: forçar DIAS_DE_FERIAS como numérico
    if "FÉRIAS" in name_up or "FERIAS" in name_up:
        if "DIAS_DE_FERIAS" in df.columns:
            df["DIAS_DE_FERIAS"] = to_num(df["DIAS_DE_FERIAS"])

    # ESTÁGIO: nada específico além de UNNAMED (nome pode vir sem acento)
    if "ESTÁGIO" in name_up or "ESTAGIO" in name_up:
        pass  # já removemos UNNAMED acima

        # Base dias uteis: transformar em mapeamento SINDICATO -> DIAS_UTEIS
    if "BASE DIAS UTEIS" in name_up or "BASE DIAS ÚTEIS" in name_up:
        # Releitura "crua" do arquivo para manipular linhas
        df0 = pd.read_excel(in_path, engine="openpyxl")

        if df0.shape[1] < 2:
            print("⚠️ Estrutura inesperada em 'Base dias uteis'. Mantendo como está.")
        else:
            col_sind = df0.columns[0]   # ex: BASE_DIAS_UTEIS_DE_15_04_A_15_05
            col_dias = df0.columns[1]   # ex: UNNAMED_1

            # A 1ª linha do arquivo contém os rótulos (“SINDICADO” / “DIAS UTEIS”).
            # Os dados começam na linha 2.
            sind = df0[col_sind].iloc[1:].astype(str).str.strip()
            dias = pd.to_numeric(df0[col_dias].iloc[1:], errors="coerce")

            # Normaliza nomes/colunas finais
            df = pd.DataFrame({
                "SINDICATO": sind,
                "DIAS_UTEIS": dias
            }).dropna(subset=["SINDICATO", "DIAS_UTEIS"])

            # Se quiser, padronize “SINDICADO” -> “SINDICATO” (feito acima)


    # Base sindicato x valor: VALOR numérico; ESTADO limpo
    if "BASE SINDICATO X VALOR" in name_up:
        if "VALOR" in df.columns:
            df["VALOR"] = to_num(df["VALOR"])
        if "ESTADO" in df.columns:
            df["ESTADO"] = df["ESTADO"].astype(str).str.strip()

    # ----------------------------------------------------

    out_path = FORM_OK / nome_arquivo.replace("_FORM.xlsx", "_FORM_OK.xlsx")
    if out_path.exists():
        print(f"🔁 Já existe: {out_path.name}")
        return

    with pd.ExcelWriter(out_path, engine="openpyxl") as wr:
        df.to_excel(wr, index=False)

    print(f"✅ Limpo: {out_path.name}")

def main():
    """
    Liste aqui os arquivos que quer limpar (não inclua o layout 'VR MENSAL ...').
    Você pode comentar/descomentar para rodar um a um, se preferir.
    """
    arquivos = [
        "AFASTAMENTOS_FORM.xlsx",
        "APRENDIZ_FORM.xlsx",
        "ATIVOS_FORM.xlsx",
        "DESLIGADOS_FORM.xlsx",
        "EXTERIOR_FORM.xlsx",
        "FÉRIAS_FORM.xlsx",
        "ESTÁGIO_FORM.xlsx",                 # faltante 1
        "Base dias uteis_FORM.xlsx",         # faltante 2
        "Base sindicato x valor_FORM.xlsx",  # faltante 3
        # "VR MENSAL 05.2025_FORM.xlsx"  # NÃO limpar: é layout de saída
    ]

    for arq in arquivos:
        limpar_generico(arq)

if __name__ == "__main__":
    main()
