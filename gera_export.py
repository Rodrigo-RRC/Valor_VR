# gera_export.py
import yaml, pandas as pd

REGRAS = yaml.safe_load(open("regras.yml", "r", encoding="utf-8"))
result_path  = REGRAS["arquivos"]["result_xlsx"]
export_path  = REGRAS["layout"]["arquivo_export"]
mapping      = REGRAS["layout"]["mapping"]
export_cols  = REGRAS["layout"]["export_columns"]

df = pd.read_excel(result_path)

out = pd.DataFrame()
for col in export_cols:
    src = mapping.get(col, {})
    if "from" in src and src["from"] in df.columns:
        out[col] = df[src["from"]]
    else:
        out[col] = src.get("default", "")

# garantia de tipos básicos (opcional)
num_cols = ["Dias", "VALOR DIÁRIO VR", "TOTAL", "Custo empresa", "Desconto profissional"]
for c in num_cols:
    if c in out.columns: out[c] = pd.to_numeric(out[c], errors="coerce")

# grava
out.to_excel(export_path, index=False)
print(f"[OK] Exportado: {export_path}  ({len(out):,} linhas)")
