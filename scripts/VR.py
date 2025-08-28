# scripts/VR.py
# -*- coding: utf-8 -*-
"""
Desafio 4 — Consolidação do VR (V2.2)
Incrementos:
  1) Desligados tolerante (OK/SIM/TRUE/1) com normalização
  2) Proporcional >=16 (por dias úteis ou calendário)
  3) 80/20 (empresa/profissional) após VR_COLAB
  4) Layout final sem linha "UNNAMED"
  5) ADMISSÃO preenchida (varredura robusta de *ADMISS*FORM_OK.xlsx + fallback em ATIVOS) e formatada dd/mm/aaaa
  6) Mapeamento VALOR por UF via ESTADO→UF_REF (não usa 'SINDICATO' na tabela de valores)
  7) Gravação segura: se o arquivo estiver aberto, salva *_NEW.xlsx
"""

from pathlib import Path
import pandas as pd
import unicodedata, re
from datetime import date

# ===========================
#   CONFIGURAÇÕES DA REGRA
# ===========================
PROPORCIONAL_DESLIGADOS = True        # proporcional >=16
PROPORCAO_BASE          = "UTEIS"     # "UTEIS" (recomendado) ou "CALENDARIO"
ARREDONDAR_DIAS         = "round"     # "round" | "floor" | "ceil"

# Pastas
RAIZ    = Path(__file__).resolve().parents[1]
FORM_OK = RAIZ / "data" / "FORM_OK"
OUT_DIR = RAIZ / "data" / "ETL_OK"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------
# Utilitários
# ------------------------------------------------
def read_xlsx(p: Path, tag: str) -> pd.DataFrame:
    print(f"📥 Lendo: {tag} -> {p}")
    df = pd.read_excel(p, engine="openpyxl")
    print(f"   linhas={len(df)}, colunas={len(df.columns)}")
    return df

def safe_to_excel(df: pd.DataFrame, path: Path, *, label: str) -> None:
    try:
        df.to_excel(path, index=False)
        print(f"💾 Salvo ({label}): {path.resolve()}")
    except PermissionError:
        alt = path.with_name(path.stem + "_NEW.xlsx")
        df.to_excel(alt, index=False)
        print(f"⚠️ Arquivo estava aberto. Salvei ({label}) como: {alt.resolve()}")

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

UF_SET = {
    "AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
    "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"
}
NOME2UF = {
    "ACRE":"AC","ALAGOAS":"AL","AMAPA":"AP","AMAZONAS":"AM","BAHIA":"BA","CEARA":"CE",
    "DISTRITOFEDERAL":"DF","ESPIRITOSANTO":"ES","GOIAS":"GO","MARANHAO":"MA","MINASGERAIS":"MG",
    "MATOGROSSO":"MT","MATOGROSSODOSUL":"MS","PARA":"PA","PARAIBA":"PB","PERNAMBUCO":"PE","PIAUI":"PI",
    "PARANA":"PR","RIODEJANEIRO":"RJ","RIOGRANDEDONORTE":"RN","RONDONIA":"RO","RORAIMA":"RR",
    "RIOGRANDEDOSUL":"RS","SANTACATARINA":"SC","SERGIPE":"SE","SAOPAULO":"SP","TOCANTINS":"TO"
}

def uf_from_sindicato(txt: str) -> str | None:
    """Extrai a UF do texto do sindicato (sigla, sufixo ou nome por extenso)."""
    if not isinstance(txt, str):
        return None
    t = txt.strip().upper()
    if not t:
        return None
    # tokens (siglas)
    for tok in re.split(r"[^\w]+", t):
        if tok in UF_SET:
            return tok
    # sufixo de 2 chars
    if len(t) >= 2 and t[-2:] in UF_SET:
        return t[-2:]
    # nome por extenso (remove termos comuns e procura nome do estado)
    t2 = strip_accents(t).replace(" ", "")
    t2 = re.sub(r"(SINDICATO|SIND|TRABALHADOR(ES)?|TRAB|RURAL(IS)?|URBANO(S)?|INDUSTRIA(L)?|COMERCIO|SERVICO(S)?|PROC(ESSO)?(S)?|DADOS)", "", t2)
    for nome, uf in NOME2UF.items():
        if nome in t2:
            return uf
    return None

def nome_estado_para_uf(nome: str) -> str | None:
    if not isinstance(nome, str):
        return None
    t = strip_accents(nome.strip().upper()).replace(" ", "")
    return NOME2UF.get(t)

def last_day_of_month(ano: int, mes: int) -> date:
    import calendar as _cal
    return date(ano, mes, _cal.monthrange(ano, mes)[1])

def bdays_count(start: date, end: date) -> int:
    """Dias úteis (Mon–Fri) inclusivos."""
    if start > end:
        return 0
    rng = pd.bdate_range(start, end, freq="C")
    return len(rng)

def arredonda(valor: float) -> int:
    import math
    if ARREDONDAR_DIAS == "floor":
        return math.floor(valor)
    if ARREDONDAR_DIAS == "ceil":
        return math.ceil(valor)
    return int(round(valor))

def core_matricula(s: pd.Series) -> pd.Series:
    ss = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    ss = ss.str.lstrip("0").mask(ss=="", "0")
    return ss

# ======================
# 1) Leituras das bases
# ======================
ativos     = read_xlsx(FORM_OK / "ATIVOS_FORM_OK.xlsx", "ATIVOS")
aprendiz   = read_xlsx(FORM_OK / "APRENDIZ_FORM_OK.xlsx", "APRENDIZ")

estagio_p1 = FORM_OK / "ESTÁGIO_FORM_OK.xlsx"
estagio_p2 = FORM_OK / "ESTAGIO_FORM_OK.xlsx"
estagio    = read_xlsx(estagio_p1, "ESTÁGIO") if estagio_p1.exists() else read_xlsx(estagio_p2, "ESTAGIO")

exterior   = read_xlsx(FORM_OK / "EXTERIOR_FORM_OK.xlsx", "EXTERIOR")
afast      = read_xlsx(FORM_OK / "AFASTAMENTOS_FORM_OK.xlsx", "AFASTAMENTOS")
sindvalor  = read_xlsx(FORM_OK / "Base sindicato x valor_FORM_OK.xlsx", "SINDICATOxVALOR")
diasuteis  = read_xlsx(FORM_OK / "Base dias uteis_FORM_OK.xlsx", "DIAS_UTEIS")

ferias_p1 = FORM_OK / "FÉRIAS_FORM_OK.xlsx"
ferias_p2 = FORM_OK / "FERIAS_FORM_OK.xlsx"
ferias    = read_xlsx(ferias_p1, "FÉRIAS") if ferias_p1.exists() else read_xlsx(ferias_p2, "FERIAS") if ferias_p2.exists() else pd.DataFrame()
deslig    = read_xlsx(FORM_OK / "DESLIGADOS_FORM_OK.xlsx", "DESLIGADOS") if (FORM_OK / "DESLIGADOS_FORM_OK.xlsx").exists() else pd.DataFrame()

# --- ADMISSÕES (arquivos com "ADMISS" no nome, com/sem acento) ---
import glob
from pathlib import Path as _Path

def detectar_col_admissao(df: pd.DataFrame) -> str | None:
    """Retorna o nome da coluna que representa ADMISSÃO (tolerante)."""
    if df.empty:
        return None
    normalized = {c: strip_accents(str(c)).upper().replace(" ", "").replace("_","") for c in df.columns}
    candidatos = ["ADMISSAO","ADMISSAOABRIL","DATAADMISSAO","DATAADMISSAOABRIL","DTADMISSAO"]
    for c, norm in normalized.items():
        if norm in candidatos:
            return c
    for c, norm in normalized.items():
        if "ADMIS" in norm:
            return c
    return None

admiss_list = []
padroes = [str(FORM_OK / "*ADMISS*FORM_OK.xlsx"), str(FORM_OK / "*ADMISSÃO*FORM_OK.xlsx")]
vistos = set()
for patt in padroes:
    for fp in glob.glob(patt):
        if fp in vistos: 
            continue
        vistos.add(fp)
        adm = read_xlsx(_Path(fp), f"ADMISSAO ({_Path(fp).name})")
        if adm.empty or "MATRICULA" not in adm.columns:
            continue
        col_adm = detectar_col_admissao(adm)
        if not col_adm:
            for cand in ["ADMISSAO","ADMISSÃO","DATA_ADMISSAO","DATA ADMISSAO","DT_ADMISSAO"]:
                if cand in adm.columns:
                    col_adm = cand
                    break
        if not col_adm:
            print(f"   (ADMISS) Não encontrei coluna de admissão em {fp}.")
            continue
        adm = adm.rename(columns={col_adm: "ADMISSAO"})
        adm["MATRICULA"] = adm["MATRICULA"].astype("string")
        adm["ADMISSAO"]  = pd.to_datetime(adm["ADMISSAO"], errors="coerce")
        adm = adm.dropna(subset=["MATRICULA"])
        admiss_list.append(adm[["MATRICULA","ADMISSAO"]])

if admiss_list:
    admissao = pd.concat(admiss_list, ignore_index=True)
    admissao = admissao.sort_values(["MATRICULA","ADMISSAO"]).drop_duplicates("MATRICULA", keep="first")
    print(f"🔗 ADMISSAO: {len(admissao)} matrículas detectadas nas planilhas de admissão.")
else:
    admissao = pd.DataFrame(columns=["MATRICULA","ADMISSAO"])
    print("ℹ️ ADMISSAO: nenhuma planilha *ADMISS* encontrada/útil; usarei fallback em ATIVOS se possível.")

# =======================
# 2) Normalizações básicas
# =======================
for df in (ativos, aprendiz, estagio, exterior, afast, ferias, deslig, admissao):
    if not df.empty and "MATRICULA" in df.columns:
        df["MATRICULA"] = df["MATRICULA"].astype("string")

for col in ("SINDICATO", "ESTADO"):
    if col in ativos.columns:    ativos[col]    = ativos[col].astype(str).str.upper().str.strip()
    if col in sindvalor.columns: sindvalor[col] = sindvalor[col].astype(str).str.upper().str.strip()
    if col in diasuteis.columns: diasuteis[col] = diasuteis[col].astype(str).str.upper().str.strip()

if "VALOR" in sindvalor.columns:
    sindvalor["VALOR"] = to_num(sindvalor["VALOR"])

# Mapeia ESTADO (por extenso) -> UF_REF para casar com UF_BASE
if "ESTADO" in sindvalor.columns:
    sindvalor["UF_REF"] = sindvalor["ESTADO"].apply(nome_estado_para_uf)
    sindvalor = sindvalor.dropna(subset=["UF_REF","VALOR"])

# =================
# 3) Base principal
# =================
base = ativos.copy()
print(f"🔹 Base inicial (ATIVOS): {len(base)} linhas")

# ============ 
# 4) Exclusões
# ============
def exclui(b: pd.DataFrame, df_remove: pd.DataFrame, motivo: str) -> pd.DataFrame:
    if df_remove.empty or "MATRICULA" not in df_remove.columns or "MATRICULA" not in b.columns:
        print(f"   (pula {motivo}: sem dados/coluna)")
        return b
    antes = len(b)
    b = b[~b["MATRICULA"].isin(df_remove["MATRICULA"])]
    print(f"   - {motivo}: removidos {antes - len(b)}")
    return b

base = exclui(base, aprendiz, "APRENDIZ")
base = exclui(base, estagio,  "ESTÁGIO")
base = exclui(base, exterior, "EXTERIOR")

if not afast.empty and "NA_COMPRA" in afast.columns:
    af2 = afast.copy()
    af2["NA_COMPRA"] = af2["NA_COMPRA"].astype(str).str.upper().str.strip()
    lista_nao = af2.loc[af2["NA_COMPRA"].isin(["NAO","NÃO","FALSE","0"]), "MATRICULA"]
    antes = len(base)
    base = base[~base["MATRICULA"].isin(lista_nao)]
    print(f"   - AFASTAMENTOS (NA_COMPRA=Não): removidos {antes - len(base)}")
else:
    print("   (pula AFASTAMENTOS: coluna NA_COMPRA ausente ou base vazia)")

print(f"✅ Após exclusões: {len(base)} linhas")

# ====================================
# 5) VALOR_UNITARIO por UF do sindicato
# ====================================
if "SINDICATO" not in base.columns:
    print("⚠️ Coluna SINDICATO ausente em ATIVOS; VALOR_UNITARIO ficará 0.")
    base["UF_BASE"] = pd.NA
else:
    base["UF_BASE"] = base["SINDICATO"].apply(uf_from_sindicato)

base["VALOR_UNITARIO"] = pd.NA
if "UF_REF" in sindvalor.columns and "VALOR" in sindvalor.columns:
    ref = sindvalor[["UF_REF","VALOR"]].dropna()
    antes = base["VALOR_UNITARIO"].isna().sum()
    base  = base.merge(ref, left_on="UF_BASE", right_on="UF_REF", how="left")
    mask = base["VALOR_UNITARIO"].isna() & base["VALOR"].notna()
    base.loc[mask, "VALOR_UNITARIO"] = base.loc[mask, "VALOR"]
    base = base.drop(columns=[c for c in ("UF_REF","VALOR") if c in base.columns])
    preenchidos = antes - base["VALOR_UNITARIO"].isna().sum()
    print(f"🔗 Merge VALOR por UF (UF_BASE ↔ ESTADO→UF_REF) concluído. (preenchidos={preenchidos})")
else:
    print("⚠️ Referência de VALOR sem colunas esperadas (UF_REF/VALOR).")
    base["VALOR_UNITARIO"] = to_num(base["VALOR_UNITARIO"]).fillna(0)

faltam = base["VALOR_UNITARIO"].isna().sum()
if faltam > 0:
    print(f"⚠️ {faltam} linhas sem VALOR_UNITARIO (UF não detectada). Preenchendo com 0.")
    base["VALOR_UNITARIO"] = to_num(base["VALOR_UNITARIO"]).fillna(0)

# ==================================
# 6) DIAS_UTEIS por SINDICATO (merge)
# ==================================
if "SINDICATO" in base.columns and {"SINDICATO","DIAS_UTEIS"}.issubset(diasuteis.columns):
    base = base.merge(diasuteis[["SINDICATO","DIAS_UTEIS"]], on="SINDICATO", how="left")
    print("🔗 Merge DIAS_UTEIS por SINDICATO concluído.")
else:
    print("⚠️ Não foi possível casar DIAS_UTEIS por SINDICATO. Preencherei DIAS_UTEIS=0.")
    base["DIAS_UTEIS"] = 0

# ========================
# 7) FÉRIAS
# ========================
if not ferias.empty and {"MATRICULA","DIAS_DE_FERIAS"}.issubset(ferias.columns):
    fsum = (ferias[["MATRICULA","DIAS_DE_FERIAS"]]
            .dropna(subset=["MATRICULA"])
            .groupby("MATRICULA", as_index=False)["DIAS_DE_FERIAS"].sum())
    base = base.merge(fsum, on="MATRICULA", how="left")
    base["DIAS_DE_FERIAS"] = to_num(base["DIAS_DE_FERIAS"]).fillna(0)
    print(f"🔗 FÉRIAS consolidadas: {len(fsum)} matrículas")
else:
    base["DIAS_DE_FERIAS"] = 0
    print("ℹ️ FÉRIAS ausentes ou sem colunas necessárias — assumindo 0.")

# ==================================================
# 8) DIAS_ELEGIVEIS (após férias; antes de desligados)
# ==================================================
base["DIAS_UTEIS"]     = to_num(base["DIAS_UTEIS"]).fillna(0)
base["DIAS_ELEGIVEIS"] = (base["DIAS_UTEIS"] - to_num(base["DIAS_DE_FERIAS"])).clip(lower=0)

# ======================================================
# 9) DESLIGADOS (regra do dia 15 + proporcional >=16)
# ======================================================
base["DATA_DESLIGAMENTO"] = pd.NaT
base["REGRA_DESLIGADOS_APLICADA"] = pd.NA

if not deslig.empty and "MATRICULA" in deslig.columns:
    D = deslig.copy()
    D.columns = D.columns.str.upper().str.strip()

    base["MAT_CORE"] = core_matricula(base["MATRICULA"])
    D["MAT_CORE"]    = core_matricula(D["MATRICULA"])

    # Normalização tolerante do comunicado (OK/SIM/TRUE/1)
    if "COMUNICADO_DE_DESLIGAMENTO" in D.columns:
        D["COMUNICADO_DE_DESLIGAMENTO"] = (
            D["COMUNICADO_DE_DESLIGAMENTO"]
              .astype(str).str.upper().str.strip()
              .str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('ascii')
        )
    else:
        D["COMUNICADO_DE_DESLIGAMENTO"] = ""

    # Data de desligamento (DATA_DEMISSAO -> DATA_DESLIGAMENTO)
    if "DATA_DEMISSAO" in D.columns:
        D["DATA_DESLIGAMENTO"] = pd.to_datetime(D["DATA_DEMISSAO"], errors="coerce")
    else:
        D["DATA_DESLIGAMENTO"] = pd.to_datetime(D.get("DATA_DESLIGAMENTO"), errors="coerce")

    ok_vals = {"OK","SIM","TRUE","1"}
    D_ok = D[D["COMUNICADO_DE_DESLIGAMENTO"].isin(ok_vals)][["MAT_CORE","DATA_DESLIGAMENTO"]].copy()

    # Merge traz a data
    base = base.merge(D_ok, on="MAT_CORE", how="left", suffixes=("","_DE"))
    if "DATA_DESLIGAMENTO_DE" in base.columns:
        base["DATA_DESLIGAMENTO"] = base["DATA_DESLIGAMENTO"].combine_first(base["DATA_DESLIGAMENTO_DE"])
        base = base.drop(columns=["DATA_DESLIGAMENTO_DE"])
    base = base.drop(columns=["MAT_CORE"], errors="ignore")

    # Aplica regras
    dia = base["DATA_DESLIGAMENTO"].dt.day
    ate_15  = dia.notna() & (dia <= 15)
    apos_15 = dia.notna() & (dia >= 16)

    # Até o dia 15: zera
    base.loc[ate_15, "DIAS_ELEGIVEIS"] = 0
    base.loc[ate_15, "REGRA_DESLIGADOS_APLICADA"] = "ATE_15=NAO_COMPRA"

    # >=16: proporcional
    if PROPORCIONAL_DESLIGADOS:
        def fracao_desl(ts: pd.Timestamp) -> float:
            if pd.isna(ts): return 1.0
            ano, mes, d = ts.year, ts.month, ts.day
            inicio = date(ano, mes, 16)
            fim    = last_day_of_month(ano, mes)
            if PROPORCAO_BASE.upper() == "UTEIS":
                den = bdays_count(inicio, fim)
                num = bdays_count(inicio, date(ano, mes, d))
            else:
                den = (fim - inicio).days + 1
                num = (date(ano, mes, d) - inicio).days + 1
            if den <= 0: return 1.0
            return max(min(num/den, 1.0), 0.0)

        frac_desl = base["DATA_DESLIGAMENTO"].apply(fracao_desl)
        base.loc[apos_15, "DIAS_ELEGIVEIS"] = [
            arredonda(de * fr) for de, fr in zip(base.loc[apos_15, "DIAS_ELEGIVEIS"], frac_desl[apos_15])
        ]
        base.loc[apos_15, "REGRA_DESLIGADOS_APLICADA"] = "APOS_15=PROPORCIONAL"
    else:
        base.loc[apos_15, "REGRA_DESLIGADOS_APLICADA"] = "APOS_15=COMPRA"

    # Quem não teve comunicado OK → NAO_APLICA
    sem = base["DATA_DESLIGAMENTO"].isna()
    base.loc[sem, "REGRA_DESLIGADOS_APLICADA"] = base.loc[sem, "REGRA_DESLIGADOS_APLICADA"].fillna("NAO_APLICA")

else:
    print("ℹ️ DESLIGADOS ausente(s) ou sem colunas necessárias — regra não aplicada.")

# =========================
# 10) ADMISSAO (trazer para base + fallback ATIVOS)
# =========================
if "ADMISSAO" not in base.columns:
    base["ADMISSAO"] = pd.NaT

# 10.1) Preferência: planilhas *ADMISS* lidas acima
if not admissao.empty:
    antes_na = base["ADMISSAO"].isna().sum()
    base = base.merge(admissao, on="MATRICULA", how="left", suffixes=("", "_SRC"))
    mask_new = base["ADMISSAO_SRC"].notna()
    base.loc[mask_new, "ADMISSAO"] = base.loc[mask_new, "ADMISSAO_SRC"]
    base = base.drop(columns=["ADMISSAO_SRC"], errors="ignore")
    apos_na = base["ADMISSAO"].isna().sum()
    print(f"🔗 ADMISSAO via planilhas: preenchidos {antes_na - apos_na}")

# 10.2) Fallback: tentar alguma coluna de admissão existente em ATIVOS
if base["ADMISSAO"].isna().all():
    for cand in ["ADMISSAO","ADMISSÃO","DATA_ADMISSAO","DATA ADMISSAO","DT_ADMISSAO"]:
        if cand in ativos.columns:
            base["ADMISSAO"] = pd.to_datetime(ativos[cand], errors="coerce")
            print(f"ℹ️ ADMISSAO fallback a partir de ATIVOS: coluna '{cand}'")
            break

# =========================
# 11) VR final + divisão 80/20
# =========================
base["VALOR_UNITARIO"] = to_num(base["VALOR_UNITARIO"]).fillna(0)
base["DIAS_ELEGIVEIS"] = to_num(base["DIAS_ELEGIVEIS"]).fillna(0)
base["VR_COLAB"]       = (base["DIAS_ELEGIVEIS"] * base["VALOR_UNITARIO"]).round(2)
base["VR_EMPRESA"]     = (base["VR_COLAB"].fillna(0) * 0.80).round(2)
base["VR_PROFISSIONAL"]= (base["VR_COLAB"].fillna(0) * 0.20).round(2)

# ============
# 12) RESULT
# ============
out_path = OUT_DIR / "VR_MENSAL_RESULT.xlsx"
cols = [c for c in [
    "MATRICULA","EMPRESA","SINDICATO","UF_BASE",
    "DIAS_UTEIS","DIAS_DE_FERIAS","DIAS_ELEGIVEIS",
    "VALOR_UNITARIO","VR_COLAB","VR_EMPRESA","VR_PROFISSIONAL",
    "DATA_DESLIGAMENTO","REGRA_DESLIGADOS_APLICADA",
    "ADMISSAO"
] if c in base.columns]
safe_to_excel(base[cols], out_path, label="base técnica")

# ===============================================
# 13) LAYOUT final (sem linha UNNAMED) + Admissão formatada
# ===============================================
LAYOUT_HEADERS = [
    "Matricula",
    "Admissão",
    "Sindicato do Colaborador",
    "Competência",
    "Dias",
    "VALOR DIÁRIO VR",
    "TOTAL",
    "Custo empresa",
    "Desconto profissional",
    "OBS GERAL"
]

layout_df = pd.DataFrame({
    "Matricula":               base.get("MATRICULA"),
    "Admissão":                base.get("ADMISSAO", pd.NaT),
    "Sindicato do Colaborador":base.get("SINDICATO"),
    "Competência":             base.get("COMPETENCIA", None),
    "Dias":                    base.get("DIAS_ELEGIVEIS"),
    "VALOR DIÁRIO VR":         base.get("VALOR_UNITARIO"),
    "TOTAL":                   base.get("VR_COLAB"),
    "Custo empresa":           base.get("VR_EMPRESA"),
    "Desconto profissional":   base.get("VR_PROFISSIONAL"),
    "OBS GERAL":               None
})[LAYOUT_HEADERS]

# Formata Admissão como dd/mm/aaaa (vazios permanecem vazios)
layout_df["Admissão"] = pd.to_datetime(layout_df["Admissão"], errors="coerce")
layout_df["Admissão"] = layout_df["Admissão"].dt.strftime("%d/%m/%Y").fillna("")

layout_path = OUT_DIR / "VR_MENSAL_LAYOUT.xlsx"
safe_to_excel(layout_df, layout_path, label="layout final")
