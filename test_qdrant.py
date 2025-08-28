import os
import sys
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

def die(msg: str, code: int = 1):
    print(f"[ERRO] {msg}", file=sys.stderr)
    sys.exit(code)

def main():
    load_dotenv()

    url = os.getenv("QDRANT_URL", "").strip()
    api_key = os.getenv("QDRANT_API_KEY", "").strip()

    if not url or not api_key:
        die("Defina QDRANT_URL e QDRANT_API_KEY no .env")

    if not url.startswith("http"):
        die("QDRANT_URL deve iniciar com http(s):// e incluir :6333 no Qdrant Cloud")

    print(f"[INFO] Conectando em: {url}")

    # Força REST/HTTPS. Evita gRPC/6334 para não bater no erro 10061.
    client = QdrantClient(
        url=url,
        api_key=api_key,
        prefer_grpc=False,
        timeout=60.0,
    )

    # Teste de saúde simples
    try:
        cols = client.get_collections()
        print("[OK] Conexão bem-sucedida. Collections disponíveis:", [c.name for c in cols.collections])

    except Exception as e:
        die(f"Falha no get_version(): {e}")

    # Lista collections
    try:
        cols = client.get_collections().collections
        names = [c.name for c in cols]
        print("[OK] Collections existentes:", names if names else "(nenhuma)")
    except UnexpectedResponse as e:
        die(f"Falha ao listar collections (UnexpectedResponse): {e}")
    except Exception as e:
        die(f"Falha ao listar collections: {e}")

if __name__ == "__main__":
    main()
