#teste_pandas.py
from scripts.limpeza import carregar_excel, salvar_excel

#Exemplo de Uso
df = carregar_excel("ADMISSÃO ABRIL.xlsx")
print(df.head())

#Depois de Limpar
salvar_excel(df, "ADMISSÃO ABRIL.xlsx")
