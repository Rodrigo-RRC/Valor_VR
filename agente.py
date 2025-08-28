# agente.py
# -------------------------------------------
# Agente de VR (LangChain + Tools pandas) – inteligência por ferramentas genéricas
# -------------------------------------------

import os
import json
import yaml
import pandas as pd
from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="langchain")

from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain.agents import initialize_agent, AgentType
from langchain.tools import tool

load_dotenv()

# --------- Config ---------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
CHAT_MODEL = os.getenv("CHAT_MODEL", "gpt-4o-mini").strip()
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small").strip()
REGRAS_YAML_PATH = os.getenv("REGRAS_YAML", "./regras.yml")

if not OPENAI_API_KEY:
    raise RuntimeError("Defina OPENAI_API_KEY no .env")
if not os.path.exists(REGRAS_YAML_PATH):
    raise FileNotFoundError(f"Arquivo de regras não encontrado: {REGRAS_YAML_PATH}")

with open(REGRAS_YAML_PATH, "r", encoding="utf-8") as f:
    REGRAS = yaml.safe_load(f)

RESULT_XLSX = REGRAS["arquivos"]["result_xlsx"]

# --------- Modelos ---------
llm = ChatOpenAI(model=CHAT_MODEL, temperature=0)
emb = OpenAIEmbeddings(model=EMBEDDING_MODEL)  # reservado para retriever futuro

# --------- Utils ---------
def _carregar_result() -> pd.DataFrame:
    """Carrega o RESULT e normaliza colunas numéricas/chave."""
    df = pd.read_excel(RESULT_XLSX, engine="openpyxl")
    for col in ["VR_COLAB", "VR_EMPRESA", "VR_PROFISSIONAL", "VALOR_UNITARIO", "DIAS_ELEGIVEIS", "DIAS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "MATRICULA" in df.columns:
        df["MATRICULA"] = df["MATRICULA"].astype(str).str.strip()
    return df

def _fmt(v: float) -> str:
    """Formata moeda pt-BR."""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _safe_str(x):
    return "—" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x)

def _apply_filters(df: pd.DataFrame, filters_json: str) -> pd.DataFrame:
    """
    Aplica filtros no formato JSON, ex.:
    [
      {"col":"VR_COLAB","op":">","value":0},
      {"col":"SINDICATO","op":"in","value":["SINDPD RJ","SINDPD SP"]}
    ]
    Ops suportados: ==, !=, >, >=, <, <=, in, not_in
    """
    if not filters_json:
        return df
    try:
        flt = json.loads(filters_json)
    except Exception:
        return df
    if not isinstance(flt, list):
        return df

    out = df.copy()
    for cond in flt:
        col, op, val = cond.get("col"), cond.get("op"), cond.get("value")
        if col not in out.columns:  # ignora filtros inválidos
            continue
        series = out[col]
        if op == "==":
            out = out[series == val]
        elif op == "!=":
            out = out[series != val]
        elif op == ">":
            out = out[pd.to_numeric(series, errors="coerce") > float(val)]
        elif op == ">=":
            out = out[pd.to_numeric(series, errors="coerce") >= float(val)]
        elif op == "<":
            out = out[pd.to_numeric(series, errors="coerce") < float(val)]
        elif op == "<=":
            out = out[pd.to_numeric(series, errors="coerce") <= float(val)]
        elif op == "in":
            out = out[series.astype(str).isin([str(x) for x in (val if isinstance(val, list) else [val])])]
        elif op == "not_in":
            out = out[~series.astype(str).isin([str(x) for x in (val if isinstance(val, list) else [val])])]
    return out

# --------- Ferramentas genéricas ---------
@tool
def schema_info(_: str = "") -> str:
    """Retorna o esquema disponível (colunas) e sinônimos úteis.
    JSON: {"ok":true,"colunas":[...],"sugestoes":{"destinatarios":"SINDICATO","funcionario":"MATRICULA"}}"""
    df = _carregar_result()
    sugestoes = {
        "destinatarios": "SINDICATO",
        "sindicatos": "SINDICATO",
        "colaborador": "MATRICULA",
        "funcionario": "MATRICULA",
        "valor unitario": "VALOR_UNITARIO",
        "valor diario": "VALOR_UNITARIO",
        "total colaborador": "VR_COLAB",
        "custo empresa": "VR_EMPRESA",
        "desconto profissional": "VR_PROFISSIONAL",
    }
    return json.dumps({"ok": True, "colunas": list(df.columns), "sugestoes": sugestoes}, ensure_ascii=False)

@tool
def aggregate(op: str, column: str, filters_json: str = "", positive_only: bool = False) -> str:
    """Agrega uma coluna (sum|mean|min|max|count) com filtros opcionais.
    Args:
      op: 'sum'|'mean'|'min'|'max'|'count'
      column: alvo da agregação (para count pode ser qualquer coluna existente)
      filters_json: ver _apply_filters docstring
      positive_only: se True, ignora valores <= 0 (útil para média de quem recebeu VR)
    Retorna JSON: {"ok":true,"op":"sum","column":"VR_COLAB","valor":<float>,"fmt":"R$ ...","denominador":<int>}
    """
    df = _apply_filters(_carregar_result(), filters_json)
    if column not in df.columns:
        return json.dumps({"ok": False, "erro": f"Coluna {column} ausente."}, ensure_ascii=False)

    s = pd.to_numeric(df[column], errors="coerce")
    if positive_only:
        s = s[s > 0]

    if op == "sum":
        val = float(s.sum())
        return json.dumps({"ok": True, "op": op, "column": column, "valor": val, "fmt": _fmt(val)}, ensure_ascii=False)
    if op == "mean":
        denom = int(s.count())
        media = float(s.mean()) if denom > 0 else 0.0
        return json.dumps({"ok": True, "op": op, "column": column, "valor": media, "fmt": _fmt(media), "denominador": denom}, ensure_ascii=False)
    if op == "min":
        v = float(s.min()) if len(s) else 0.0
        return json.dumps({"ok": True, "op": op, "column": column, "valor": v, "fmt": _fmt(v)}, ensure_ascii=False)
    if op == "max":
        v = float(s.max()) if len(s) else 0.0
        return json.dumps({"ok": True, "op": op, "column": column, "valor": v, "fmt": _fmt(v)}, ensure_ascii=False)
    if op == "count":
        return json.dumps({"ok": True, "op": op, "column": column, "valor": int(s.count())}, ensure_ascii=False)
    return json.dumps({"ok": False, "erro": f"Operação {op} inválida."}, ensure_ascii=False)

@tool("gerar_arquivo_layout")
def gerar_arquivo_layout(_: str = "") -> str:
    """Gera o XLSX final conforme regras.yml/layout e devolve o caminho salvo."""
    try:
        with open(REGRAS_YAML_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        result_path = cfg["arquivos"]["result_xlsx"]
        layout = cfg["layout"]
        export_cols = layout.get("export_columns", [])
        mapping = layout.get("mapping", {})
        out_path = layout.get("arquivo_export", "./data/ETL_OK/VR_MENSAL_EXPORT.xlsx")

        df = pd.read_excel(result_path, engine="openpyxl")

        out = pd.DataFrame()
        for col in export_cols:
            src = mapping.get(col, {})
            if "from" in src and src["from"] in df.columns:
                out[col] = df[src["from"]]
            else:
                out[col] = src.get("default", "")

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        out.to_excel(out_path, index=False)

        return json.dumps(
            {"ok": True, "path": out_path, "linhas": int(len(out)), "colunas": list(out.columns)},
            ensure_ascii=False
        )
    except Exception as e:
        return json.dumps({"ok": False, "erro": str(e)}, ensure_ascii=False)


@tool
def group_aggregate(op: str, target: str, group_by: str, k: int = 5, order: str = "desc", filters_json: str = "") -> str:
    """Agrega por grupo e devolve TOP-K (ou todos se k<=0).
    Args:
      op: 'sum'|'mean'|'min'|'max'
      target: coluna a agregar (ex.: 'VR_COLAB')
      group_by: coluna de agrupamento (ex.: 'MATRICULA' ou 'SINDICATO')
      k: quantidade de itens (1 para “quem foi o maior”)
      order: 'desc' ou 'asc'
      filters_json: filtros opcionais
    Retorna JSON: {"ok":true,"itens":[{"grupo":"...", "valor":<float>, "fmt":"R$ ...", "matricula":"...", "nome":"..."}]}
    """
    df = _apply_filters(_carregar_result(), filters_json)
    for c in [target, group_by]:
        if c not in df.columns:
            return json.dumps({"ok": False, "erro": f"Coluna {c} ausente."}, ensure_ascii=False)

    s = pd.to_numeric(df[target], errors="coerce").fillna(0.0)
    grp = df.copy()
    grp[target] = s
    if op == "sum":
        agg = grp.groupby(group_by, as_index=False)[target].sum()
    elif op == "mean":
        agg = grp.groupby(group_by, as_index=False)[target].mean()
    elif op == "min":
        agg = grp.groupby(group_by, as_index=False)[target].min()
    elif op == "max":
        agg = grp.groupby(group_by, as_index=False)[target].max()
    else:
        return json.dumps({"ok": False, "erro": f"Operação {op} inválida."}, ensure_ascii=False)

    agg = agg.sort_values(target, ascending=(order == "asc"))
    if k and k > 0:
        agg = agg.head(int(k))

    itens = []
    for _, r in agg.iterrows():
        item = {"grupo": _safe_str(r[group_by]), "valor": float(r[target]), "fmt": _fmt(float(r[target]))}
        # enriquecimento para matrícula/nome
        if group_by.upper() == "MATRICULA":
            mat = str(r[group_by])
            item["matricula"] = mat
            if "NOME" in df.columns:
                n = df[df["MATRICULA"] == mat]["NOME"].dropna()
                if not n.empty:
                    item["nome"] = str(n.iloc[0])
        itens.append(item)

    return json.dumps({"ok": True, "itens": itens}, ensure_ascii=False)

@tool
def vr_por_matricula(matricula: str) -> str:
    """Consulta detalhada por matrícula; soma valores se houver múltiplas linhas.
    JSON: {ok, matricula, nome?, sindicato?, fmt_vr_colaborador, fmt_vr_empresa, fmt_vr_profissional}"""
    df = _carregar_result()
    if "MATRICULA" not in df.columns:
        return json.dumps({"ok": False, "erro": "MATRICULA ausente."}, ensure_ascii=False)

    mat = str(matricula).strip()
    dfm = df[df["MATRICULA"] == mat]
    if dfm.empty:
        return json.dumps({"ok": False, "erro": f"Matrícula {mat} não encontrada."}, ensure_ascii=False)

    vr_colab = float(dfm.get("VR_COLAB", 0).sum())
    vr_emp   = float(dfm.get("VR_EMPRESA", 0).sum())
    vr_prof  = float(dfm.get("VR_PROFISSIONAL", 0).sum())

    nome = None
    if "NOME" in dfm.columns:
        nn = dfm["NOME"].dropna()
        if not nn.empty: nome = str(nn.iloc[0])
    sindicato = None
    if "SINDICATO" in dfm.columns and not dfm["SINDICATO"].isna().all():
        sindicato = str(dfm["SINDICATO"].iloc[0])

    return json.dumps({
        "ok": True,
        "matricula": mat,
        "nome": nome,
        "sindicato": sindicato,
        "vr_colaborador": vr_colab, "fmt_vr_colaborador": _fmt(vr_colab),
        "vr_empresa": vr_emp,       "fmt_vr_empresa": _fmt(vr_emp),
        "vr_profissional": vr_prof, "fmt_vr_profissional": _fmt(vr_prof),
    }, ensure_ascii=False)

@tool
def analise_zerados(_: str = "") -> str:
    """Conta VR zerado e aponta possíveis causas (heurísticas).
    JSON: {"ok": true, "qtd_zerados": <int>, "causas": [{"causa":"...", "qtd": <int>}]}"""
    df = _carregar_result()
    if "VR_COLAB" not in df.columns:
        return json.dumps({"ok": False, "erro": "VR_COLAB ausente."}, ensure_ascii=False)
    base = df[df["VR_COLAB"] == 0]
    qtd = int(len(base))
    causas = []
    if "DIAS_ELEGIVEIS" in base.columns:
        causas.append({"causa": "Dias elegíveis = 0", "qtd": int((base["DIAS_ELEGIVEIS"] == 0).sum())})
    if "VALOR_UNITARIO" in base.columns:
        causas.append({"causa": "Valor unitário = 0", "qtd": int((base["VALOR_UNITARIO"] == 0).sum())})
    return json.dumps({"ok": True, "qtd_zerados": qtd, "causas": causas}, ensure_ascii=False)

@tool
def regras_resumo(_: str = "") -> str:
    """Resumo das regras do YAML (split 80/20 e desligamento).
    JSON: {"ok":true,"empresa_pct":0.8,"prof_pct":0.2,"texto":"..."}"""
    try:
        with open(REGRAS_YAML_PATH, "r", encoding="utf-8") as f:
            R = yaml.safe_load(f)
        P = R.get("parametros", {})
        split = P.get("split_percentual", {"empresa": 0.80, "profissional": 0.20})
        reg  = P.get("regra_desligamento", {"ate_dia_15_paga": False, "acima_dia_15_proporcional": True})
        texto = []
        texto.append("Desligamento no mês:")
        texto.append("- Até o dia 15: não paga VR." if not reg.get("ate_dia_15_paga", False) else "- Até o dia 15: paga.")
        texto.append("- Após o dia 15: proporcional." if reg.get("acima_dia_15_proporcional", True) else "- Após o dia 15: integral.")
        return json.dumps({"ok": True, "empresa_pct": float(split.get("empresa", 0.8)),
                           "prof_pct": float(split.get("profissional", 0.2)), "texto": " ".join(texto)}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"ok": False, "erro": f"Falha ao ler regras.yml: {e}"}, ensure_ascii=False)

# --------- SYSTEM_PROMPT (intenção → ferramenta) ---------
SYSTEM_PROMPT = """
Você é um AGENTE DE VR (Vale-Refeição). A sua inteligência está em ENTENDER a intenção e ORQUESTRAR ferramentas genéricas
para produzir números exatos, sem alucinação.

REGRAS
- A base é de VR; não existem "notas fiscais".
- Qualquer número (soma, média, máximo, mínimo, contagem, top-k) deve vir das tools.
- Valores monetários: SEMPRE use a chave "fmt" retornada.

SINÔNIMOS/ONTOLOGIA (use schema_info se tiver dúvida):
- "destinatários" → SINDICATO
- "colaborador/funcionário" → matrícula (MATRICULA)
- "valor diário" → VALOR_UNITARIO
- "total de VR do colaborador" → VR_COLAB
- "custo da empresa" → VR_EMPRESA
- "desconto do profissional" → VR_PROFISSIONAL

ROTEAMENTO DE INTENÇÃO
- "total pago/VR total" → aggregate("sum","VR_COLAB")
- "custo total da empresa" → aggregate("sum","VR_EMPRESA")
- "quanto os profissionais pagaram" → aggregate("sum","VR_PROFISSIONAL")
- "média por colaborador" → aggregate("mean","VR_COLAB", positive_only=True se mencionar "que receberam")
- "maior/menor valor de VR por colaborador" → group_aggregate("sum","VR_COLAB","MATRICULA", k=1, order="desc" ou "asc")
- "top N colaboradores com maiores valores" → group_aggregate("sum","VR_COLAB","MATRICULA", k=N)
- "principais destinatários / top sindicatos" → group_aggregate("sum","VR_COLAB","SINDICATO", k=5)
- "quantos com VR zerado e por quê" → analise_zerados
- "regra do dia 15 / proporcional / percentuais" → regras_resumo
- "VR da matrícula 12345" → vr_por_matricula("12345")

ESTILO
- Português formal e direto. Liste coleções em linhas: "MATRÍCULA – Nome: R$ ...".
- Combine varias tools quando a pergunta pedir múltiplos números.
"""

# --------- Agente ---------
TOOLS = [
    schema_info,
    aggregate,
    group_aggregate,
    vr_por_matricula,
    analise_zerados,
    regras_resumo,
    gerar_arquivo_layout,
]

agent = initialize_agent(
    TOOLS,
    llm,
    agent=AgentType.OPENAI_FUNCTIONS,
    verbose=False,
    handle_parsing_errors=True,
    agent_kwargs={"system_message": SYSTEM_PROMPT},
)

def responder_pergunta(pergunta: str) -> str:
    """Invoca o agente e devolve apenas o texto final (com fallback em caso de erro)."""
    try:
        resp = agent.invoke({"input": pergunta})
        return resp["output"] if isinstance(resp, dict) and "output" in resp else str(resp)
    except Exception as e:
        return f"Ocorreu um erro ao processar a pergunta. Detalhe técnico: {e}"

if __name__ == "__main__":
    print("\n=== Agente VR – pronto. Digite sua pergunta (ou 'sair') ===")
    while True:
        q = input("> ").strip()
        if q.lower() in {"sair", "exit", "quit"}:
            break
        print(responder_pergunta(q))
