# app.py — Dark Pro + Download do EXPORT
import os, json, yaml, streamlit as st
from dotenv import load_dotenv
from agente import responder_pergunta, vr_por_matricula

# ------------- Setup -------------
load_dotenv()
st.set_page_config(page_title="Agente de VR", page_icon="🍱", layout="wide")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    st.error("Defina OPENAI_API_KEY no .env")
    st.stop()

REGRAS_YAML_PATH = os.getenv("REGRAS_YAML", "./regras.yml")
if not os.path.exists(REGRAS_YAML_PATH):
    st.error(f"Arquivo de regras não encontrado: {REGRAS_YAML_PATH}")
    st.stop()

with open(REGRAS_YAML_PATH, "r", encoding="utf-8") as f:
    REGRAS = yaml.safe_load(f) or {}

# Caminho do arquivo final (tenta pelas duas chaves usadas no seu YAML)
export_path = (
    REGRAS.get("layout", {}).get("arquivo_export")
    or REGRAS.get("arquivos", {}).get("export_xlsx")
    or "./data/ETL_OK/VR_MENSAL_EXPORT.xlsx"
)

# ------------- Estilo -------------
st.markdown("""
<style>
header, #MainMenu, footer, div[data-testid="stStatusWidget"]{display:none!important;}
.block-container{max-width:1200px; padding-top:.4rem; padding-bottom:1rem}
:root{
  --bg:#0d1116; --panel:#121a24; --panel2:#0f161e;
  --txt:#eef4fb; --muted:#a8b9c9;
  --accent:#1ef0a0; --accent2:#ff8a2a; --resp:#2b2f36;
}
html, body, [data-testid="stApp"]{background:var(--bg)!important;}
h1,h2,h3{color:var(--txt); margin:0}
p, label, span, div{color:var(--txt)}
.hero{display:flex; align-items:center; justify-content:space-between;
  background:linear-gradient(180deg, var(--panel), var(--panel2));
  border:1px solid rgba(30,240,160,.18); border-radius:18px; padding:18px 20px; gap:16px;
  box-shadow:0 12px 32px rgba(0,0,0,.35), inset 0 0 10px rgba(30,240,160,.08);}
.escudo{width:56px; height:56px; border-radius:50%;
  background:radial-gradient(circle at 30% 30%, rgba(30,240,160,.38), transparent 52%),
             conic-gradient(from 210deg, rgba(30,240,160,.30), rgba(30,240,160,.06));
  border:2px solid rgba(30,240,160,.45);}
.badge{display:inline-flex; align-items:center; gap:8px; padding:.45rem .75rem; border-radius:999px; font-weight:800;
  background:linear-gradient(90deg, var(--accent), var(--accent2)); color:#081017;
  border:none; box-shadow:0 0 0 2px rgba(30,240,160,.18);}
.headerline{height:2px; background:linear-gradient(90deg, var(--accent), transparent, var(--accent2)); margin:1rem 0 1.2rem}
.card{background:linear-gradient(180deg, #151d28, #101722); border:1px solid rgba(30,240,160,.22);
  border-radius:16px; padding:16px; box-shadow:0 0 16px rgba(30,240,160,.10), inset 0 0 8px rgba(30,240,160,.05);}
.stTextInput>div>div>input{
  background:#101822!important; color:#f7fbff!important;
  border:1px solid rgba(30,240,160,.38)!important; border-radius:12px!important; padding:.6rem .8rem!important;}
.stTextInput>div>div>input::placeholder{color:#93a7b9!important}
.stButton>button{background:linear-gradient(90deg, var(--accent), var(--accent2));
  color:#071018; border:none; border-radius:12px; font-weight:800; padding:.6rem 1rem}
.stButton>button:hover{filter:brightness(1.07)}
.resp{background:var(--resp); color:#fff; padding:16px 18px; border-radius:14px; line-height:1.6;
  border:1px solid rgba(255,255,255,.06); box-shadow:0 0 0 1px rgba(30,240,160,.18) inset, 0 6px 20px rgba(0,0,0,.18);}
.hint{color:var(--muted); font-size:.9rem}
</style>
""", unsafe_allow_html=True)

# ------------- HERO -------------
st.markdown(
    '<div class="hero">'
    '  <div style="display:flex;align-items:center;gap:12px">'
    '    <div class="escudo"></div>'
    '    <div>'
    '      <div style="font-size:30px;font-weight:800">Agente de VR | Consulta Inteligente</div>'
    '      <div style="color:#a8b9c9;margin-top:2px">Pergunte em linguagem natural ou pesquise por matrícula. Cálculo exato, com números legíveis.</div>'
    '    </div>'
    '  </div>'
    '  <div><span class="badge">🧠 Agente ativo</span></div>'
    '</div>', unsafe_allow_html=True
)
st.markdown('<div class="headerline"></div>', unsafe_allow_html=True)

left, right = st.columns(2)

# ------------- Coluna Esquerda: Chat -------------
with left:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 🤖 Chat do Agente")

    c1,c2,c3,c4,c5 = st.columns(5)
    if c1.button("Custo total"): st.session_state["q"] = "Qual o custo total da empresa?"
    if c2.button("VR médio"):   st.session_state["q"] = "Mostre o valor médio de VR por colaborador neste mês."
    if c3.button("Top 5 VR"):   st.session_state["q"] = "Liste os 5 colaboradores com os maiores valores de VR, com matrícula e valor."
    if c4.button("Zerados"):    st.session_state["q"] = "Quantos colaboradores estão com VR zerado e por quê?"
    if c5.button("Regra 15"):   st.session_state["q"] = "Como ficou o cálculo para desligados até e após o dia 15?"

    pergunta = st.text_input(
        "Pergunte em linguagem natural",
        value=st.session_state.get("q",""),
        placeholder="Ex.: 'Qual o custo total da empresa?' ou 'Qual o VR da matrícula 12345?'"
    )

    if st.button("Perguntar ao agente", type="primary", use_container_width=True):
        if not pergunta.strip():
            st.warning("Digite uma pergunta.")
        else:
            with st.spinner("Calculando..."):
                saida = responder_pergunta(pergunta.strip())
            st.markdown("##### Resposta")
            st.markdown(f"<div class='resp'>{saida}</div>", unsafe_allow_html=True)

    # 🔽 Se o arquivo final existir, mostra botão de download
    st.markdown("#### 📦 Arquivo final (layout da operadora)")
    if os.path.exists(export_path):
        st.markdown(f"<div class='hint'>Disponível em: <code>{export_path}</code></div>", unsafe_allow_html=True)
        try:
            with open(export_path, "rb") as f:
                st.download_button(
                    label="📥 Baixar arquivo final (VR_MENSAL_EXPORT.xlsx)",
                    data=f,
                    file_name=os.path.basename(export_path),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key="dl_export"
                )
        except Exception as e:
            st.error(f"Não consegui abrir o arquivo para download: {e}")
    else:
        st.markdown("<div class='hint'>Arquivo ainda não encontrado. Gere pelo agente (Q12) ou execute o ETL.</div>", unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

# ------------- Coluna Direita: Matrícula -------------
with right:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("### 🔎 Consulta rápida por matrícula")
    mat = st.text_input("Matrícula", placeholder="Ex.: 12345")
    if st.button("Consultar matrícula", use_container_width=True):
        m = (mat or "").strip()
        if not m:
            st.warning("Informe a matrícula.")
        else:
            with st.spinner("Buscando dados..."):
                try:
                    raw = vr_por_matricula.run(m)  # ferramenta definida no agente
                    data = json.loads(raw)
                except Exception as e:
                    st.error(f"Erro ao consultar: {e}")
                    data = {"ok": False}

            if data.get("ok"):
                st.markdown("##### Resultado")
                st.markdown(
                    f"<div class='resp'>"
                    f"<b>Matrícula:</b> {data.get('matricula')}<br>"
                    f"<b>Nome:</b> {data.get('nome') or '—'}<br>"
                    f"<b>Sindicato:</b> {data.get('sindicato') or '—'}<br>"
                    f"<b>VR (colaborador):</b> {data.get('fmt_vr_colaborador')}<br>"
                    f"<b>Custo empresa:</b> {data.get('fmt_vr_empresa')}<br>"
                    f"<b>Desconto profissional:</b> {data.get('fmt_vr_profissional')}"
                    f"</div>", unsafe_allow_html=True
                )
            else:
                st.error(data.get("erro", "Consulta não retornou resultados."))
    st.markdown('</div>', unsafe_allow_html=True)
