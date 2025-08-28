# scripts/etl_clean_to_form.py
# -*- coding: utf-8 -*-
"""
OBJETIVO
--------
1) Ler TODAS as planilhas .xlsx que terminam com _clean.xlsx dentro de data/clean/
2) Fazer uma limpeza mínima e segura:
   - padronizar textos (tirar espaços extras, NBSP)
   - tentar converter colunas que PARECEM datas (heurística simples)
   - aplicar regras específicas por arquivo (se configuradas no dict SCHEMAS)
3) Salvar cada arquivo transformado em data/ETL_OK/ com sufixo _FORM.xlsx
   (ex.: ADMISSAO_ABRIL_clean.xlsx -> ADMISSAO_ABRIL_FORM.xlsx)
4) Gerar um relatório simples das colunas encontradas por arquivo:
   data/ETL_OK/_reports/cols_report.xlsx

POR QUE ASSIM?
--------------
- Você NÃO precisa criar um script para cada planilha.
- Quando precisar tratar uma planilha de forma específica, basta ajustar o dict SCHEMAS.
- Mantemos o fluxo simples e claro, usando só pandas + openpyxl.

COMO RODAR
----------
    python scripts/etl_clean_to_form.py

PRÉ-REQUISITOS
--------------
    pip install pandas openpyxl
"""

from pathlib import Path  # para manipular caminhos de forma segura e legível
import pandas as pd       # biblioteca principal para tabelas (dataframes)

# ---------------------------------------------------------------------
# PASTAS DO PROJETO
# ---------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parents[1]          # raiz do projeto (pasta que contém /data e /scripts)
CLEAN_DIR = RAIZ / "data" / "clean"                 # entrada: arquivos *_clean.xlsx
OUT_DIR = RAIZ / "data" / "ETL_OK"                  # saída: arquivos *_FORM.xlsx
REPORTS_DIR = OUT_DIR / "_reports"                  # onde salvamos o relatório de colunas


# ---------------------------------------------------------------------
# REGRAS ESPECÍFICAS POR ARQUIVO (OPCIONAL)
# - A CHAVE é um trecho do nome do arquivo (case-insensitive).
# - O script procura esse trecho no nome e aplica as regras definidas.
# - Se nada casar, aplica só a limpeza mínima e segue.
#
# CAMPOS POSSÍVEIS:
#   rename    -> renomear colunas (dict {"ANTIGO": "NOVO"})
#   force_str -> garantir tipo string nessas colunas (ex.: "MATRICULA")
#   force_date-> converter para datetime (dayfirst=True) nessas colunas
# ---------------------------------------------------------------------
SCHEMAS = {
    "ATIVOS": {
        "force_str": ["MATRICULA"],
        # "rename": {"TITULO_DO_CARGO": "CARGO"},  # exemplo de renome
    },
    "ADMISS": {  # cobre "ADMISSÃO ABRIL"
        "force_str": ["MATRICULA"],
        "force_date": ["DATA_ADMISSAO", "DT_ADMISSAO"],
    },
    "DESLIG": {
        "force_str": ["MATRICULA"],
        "force_date": ["DATA_DESLIGAMENTO", "DT_DESLIGAMENTO"],
    },
    "FERIAS": {
        "force_str": ["MATRICULA"],
        "force_date": ["DT_INICIO_FERIAS", "DT_FIM_FERIAS", "INICIO", "FIM"],
    },
    "AFAST": {
        "force_str": ["MATRICULA"],
        "force_date": ["DATA_INICIO", "DATA_FIM", "DT_INICIO", "DT_FIM"],
    },
    "APRENDIZ": {"force_str": ["MATRICULA"]},
    "ESTAGIO": {"force_str": ["MATRICULA"]},
    "EXTERIOR": {"force_str": ["MATRICULA"]},
    "BASE SINDICATO": {"force_str": []},
    "BASE DIAS UTEIS": {"force_str": ["MATRICULA"], "force_date": []},
    "VR MENSAL": {"force_str": ["MATRICULA"]},
}


def _match_schema(fname: str) -> dict:
    """
    Decide qual regra (schema) aplicar com base no nome do arquivo.
    Se nenhum padrão casar, retorna {} (sem regra específica).
    """
    up = fname.upper()
    for key, spec in SCHEMAS.items():
        if key.upper() in up:
            return spec
    return {}


def _clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpeza mínima e segura de TEXTOS:
    - garante tipo string para colunas de texto
    - remove NBSP (caractere invisível comum vindo do Excel)
    - tira espaços das pontas
    - colapsa múltiplos espaços internos em 1
    - troca string vazia por NaN (pd.NA)
    """
    for c in df.select_dtypes(include=["object", "string"]).columns:
        df[c] = (
            df[c].astype("string")
                 .str.replace("\u00a0", " ", regex=False)   # NBSP -> espaço normal
                 .str.strip()                               # tira espaços das pontas
                 .str.replace(r"\s+", " ", regex=True)      # colapsa espaços internos
                 .replace({"": pd.NA})                      # vazio -> NA
        )
    return df


def _coerce_dates_smart(df: pd.DataFrame) -> pd.DataFrame:
    """
    Heurística simples para datas:
    - Se o nome da coluna contém 'DATA', 'DT', 'INICIO' ou 'FIM', tentamos converter para datetime.
    - dayfirst=True, pois usamos padrão brasileiro (DD/MM/AAAA).
    - errors='coerce' -> valores inválidos viram NaT (data nula), sem travar o pipeline.
    """
    for c in df.columns:
        u = str(c).upper()
        if any(tok in u for tok in ("DATA", "DT", "INICIO", "FIM")):
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
            except Exception:
                # Se não der, ignora e segue (não interrompe o processo)
                pass
    return df


def _apply_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Aplica as regras específicas definidas em SCHEMAS:
    - renome de colunas
    - forçar string
    - forçar datetime
    """
    df = df.copy()

    # 1) Renomear colunas, se solicitado
    if "rename" in schema and schema["rename"]:
        df = df.rename(columns=schema["rename"])

    # 2) Forçar string
    for col in schema.get("force_str", []) or []:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # 3) Forçar datetime
    for col in schema.get("force_date", []) or []:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    return df


def _form_name(path: Path) -> str:
    """
    Gera o nome de saída com sufixo _FORM.
    - Se o nome contém _clean, troca por _FORM.
    - Caso contrário, apenas acrescenta _FORM no final do nome (antes da extensão).
    """
    stem = path.stem
    out_stem = stem.replace("_clean", "_FORM")
    if out_stem == stem:  # não tinha _clean
        out_stem = f"{stem}_FORM"
    return f"{out_stem}{path.suffix}"  # mantém a extensão original (.xlsx)


def _inventory_columns(records: list[dict]) -> None:
    """
    Salva um relatório simples das colunas de cada arquivo processado.
    - Uma linha por arquivo
    - Colunas listadas em texto separado por '; '
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    df["colunas"] = df["colunas"].apply(lambda cols: "; ".join(map(str, cols)))
    out = REPORTS_DIR / "cols_report.xlsx"
    with pd.ExcelWriter(out, engine="openpyxl") as wr:
        df.to_excel(wr, index=False)
    print(f"📝 Relatório de colunas gerado em: {out}")


def main() -> None:
    """
    Função principal:
    - garante pasta de saída
    - varre data/clean por *.xlsx
    - processa cada arquivo e grava _FORM.xlsx em data/ETL_OK
    - ao final, gera o relatório de colunas
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(CLEAN_DIR.glob("*.xlsx"))
    files = [f for f in files if not f.name.startswith("~$")]

    if not files:
        print("Nenhum .xlsx encontrado em data/clean")
        return

    inventory = []  # lista de dicionários com informações para o relatório

    for f in files:
        if f.name.startswith("~$"):
          print(f"Ignorado (lock do Excel): {f.name}")  # <-- indentado
          continue
        
        try:
            # 1) Leitura
            df = pd.read_excel(f, engine="openpyxl")

            # 2) Registrar inventário básico (para relatório)
            inventory.append({
                "arquivo": f.name,
                "linhas": int(df.shape[0]),
                "colunas_qtd": int(df.shape[1]),
                "colunas": list(df.columns),
            })

            # 3) Limpeza mínima comum (segura)
            df = _clean_strings(df)

            # 4) Tentativa heurística de datas
            df = _coerce_dates_smart(df)

            # 5) Aplicar regras específicas, se houver match no nome do arquivo
            schema = _match_schema(f.name)
            if schema:
                df = _apply_schema(df, schema)

            # 6) Definir nome de saída (_FORM.xlsx)
            out_name = _form_name(f)
            out_path = OUT_DIR / out_name

            # 7) Se já existir, não sobrescreve (evita perda acidental)
            if out_path.exists():
                print(f"🔁 Ignorado (já existe): {out_path.name}")
                continue

            # 8) Gravar
            with pd.ExcelWriter(out_path, engine="openpyxl") as wr:
                df.to_excel(wr, index=False)

            print(f"✅ {f.name}  ->  {out_path.name}")

        except Exception as e:
            # Qualquer erro em uma planilha NÃO paralisa as demais
            print(f"❌ {f.name}  ->  {e}")

    # 9) Ao final, gravar o relatório de colunas
    _inventory_columns(inventory)


# Ponto de entrada quando você roda: python scripts/etl_clean_to_form.py
if __name__ == "__main__":
    main()
