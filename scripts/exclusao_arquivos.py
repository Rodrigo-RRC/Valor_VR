import os
from pathlib import Path

# Definindo caminho do arquivo
arquivo=Path("data/clean/ADMISSÃO ABRIL_clean.xlsx")

# Verificar se o arquivo existe antes de excluir
if arquivo.exists():
    os.remove(arquivo)
    print(f"Arquivo {arquivo} removido com sucesso")
else:
    print(f"Arquivo não encontrado")
