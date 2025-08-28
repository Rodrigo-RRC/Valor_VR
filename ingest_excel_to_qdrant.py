"""
Lê as duas planilhas já tratadas.

Concatena as linhas como “registro textual” (coluna: valor) e guarda metadados.

Cria a collection no Qdrant Cloud se não existir e upserteia os vetores.

Usa OpenAI Embeddings text-embedding-3-small.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv

from qdrant_client import QdrantClient
from qdrant_client.http import models as rest

from langchain_openai import OpenAIEmbeddings
from langchain_qdrant import Qdrant  # <-- pacote novo
from langchain_core.documents import Document

def die(msg: str, code: int = 1):
    print(f"[ERRO] {msg}", file=sys.stderr)
    sys.exit(code)

def serialize_row(row: pd.Series) -> str:
    parts = []
    for k, v in row.items():
        if pd.isna(v):
            continue
        parts.append(f"{k}: {v}")
    return "\n".join(parts)

def load_frames(result_path: str, layout_path: str):
    if not os.path.exists(result_path):
        die(f"Arquivo não encontrado: {result_path}")
    if not os.path.exists(layout_path):
        die(f"Arquivo não encontrado: {layout_path}")

    df_res = pd.read_excel(result_path, engine="openpyxl")
    df_lay = pd.read_excel(layout_path, engine="openpyxl")

    return df_res, df_lay

def build_documents(df: pd.DataFrame, source_name: str) -> list:
    docs = []
    for idx, row in df.iterrows():
        text = serialize_row(row)
        meta = {"source": source_name, "row_index": int(idx)}
        docs.append(Document(page_content=text, metadata=meta))
    return docs

def ensure_collection(client: QdrantClient, collection: str, vector_size: int):
    existing = [c.name for c in client.get_collections().collections]
    if collection in existing:
        print(f"[INFO] Collection '{collection}' já existe. Usando existente.")
        return

    print(f"[INFO] Criando collection '{collection}' (dim={vector_size}, métrica=Cosine)")
    client.create_collection(
        collection_name=collection,
        vectors_config=rest.VectorParams(
            size=vector_size,
            distance=rest.Distance.COSINE,
        ),
    )

def main():
    load_dotenv()

    url = os.getenv("QDRANT_URL", "").strip()
    api_key = os.getenv("QDRANT_API_KEY", "").strip()
    collection = os.getenv("QDRANT_COLLECTION", "desafio4_vr").strip()

    result_xlsx = os.getenv("RESULT_XLSX", "./data/ETL_OK/VR_MENSAL_RESULT.xlsx")
    layout_xlsx = os.getenv("LAYOUT_XLSX", "./data/ETL_OK/VR_MENSAL_LAYOUT.xlsx")

    embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

    if not url or not api_key:
        die("Defina QDRANT_URL e QDRANT_API_KEY no .env")

    df_res, df_lay = load_frames(result_xlsx, layout_xlsx)
    docs = build_documents(df_res, "VR_MENSAL_RESULT") + build_documents(df_lay, "VR_MENSAL_LAYOUT")
    print(f"[INFO] Total de documentos a inserir: {len(docs)}")

    client = QdrantClient(url=url, api_key=api_key, prefer_grpc=False, timeout=120.0)

    embeddings = OpenAIEmbeddings(model=embedding_model)

    dim = len(embeddings.embed_query("dimensão de teste"))
    ensure_collection(client, collection, dim)

    vs = Qdrant(client=client, collection_name=collection, embeddings=embeddings)
    ids = vs.add_documents(docs, batch_size=128)
    print(f"[OK] Inseridos/atualizados {len(ids)} vetores em '{collection}'.")

    count = client.count(collection, count_filter=None, exact=True).count
    print(f"[OK] Total na collection '{collection}': {count}")

if __name__ == "__main__":
    main()
