# 🚀 Projeto VR - Análise e Inteligência com Agentes Autônomos

## 📌 Sobre o projeto
Este projeto foi desenvolvido como parte do programa **I2A2 – Criando Agentes Inteligentes com IA Generativa**.  
O objetivo é mostrar como técnicas modernas de **ETL, embeddings, bancos vetoriais e agentes inteligentes** podem transformar dados estáticos (planilhas de VR - Vale Refeição/Alimentação) em **informações estratégicas** para empresas.

Embora tenha sido implementado sobre **tabelas estáticas**, a arquitetura é facilmente adaptável para **bancos de dados dinâmicos (SQL/NoSQL)** ou integração com **APIs corporativas**.

## 🎯 Benefícios
- 🔎 Padronização e limpeza de dados de benefícios (VR/VA).
- 📊 Criação de dashboards gerenciais para RH/Financeiro.
- 🤖 Utilização de agentes inteligentes para consultas em linguagem natural.
- 🌐 Escalável para bases dinâmicas, facilitando auditoria e controle de gastos.

## 🛠️ Tecnologias Utilizadas
- **Python** 🐍 (pandas, streamlit, etc.)
- **LangChain** 🤖 (agente inteligente com LLM)
- **OpenAI API** 🔑 (modelos GPT e embeddings)
- **Qdrant** 🗄️ (banco vetorial para busca semântica)
- **Streamlit** 📊 (dashboards interativos e interface do agente)

## 📂 Estrutura, Configuração e Execução
```bash
# Estrutura de pastas do projeto
VR/
├── data/                  # Planilhas originais e processadas
│   ├── FORM_OK/
│   └── ...
├── scripts/               # Códigos principais
│   ├── etl_clean_to_form.py
│   ├── exclusao_arquivos.py
│   ├── limpar_admissao.py
│   ├── limpar_form_ok.py
│   ├── limpeza.py
│   ├── VR.py
│   └── ingest_excel_to_qdrant.py
├── app.py                 # Interface Streamlit do agente
├── agente.py              # Lógica do agente inteligente
├── regras.yml             # Regras de negócio do agente
├── .env                   # Variáveis de ambiente (não versionar)
└── README.md              # Documentação do projeto

# Arquivo .env
OPENAI_API_KEY=sk-...
CHAT_MODEL=gpt-4o-mini
EMBEDDING_MODEL=text-embedding-3-small
QDRANT_URL=https://xxxxxxxx.qdrant.tech
QDRANT_API_KEY=xxxxxxxxx
QDRANT_COLLECTION=vr_mensal_docs

# Execução passo a passo
## 1. Executar ETL
python scripts/VR.py

## 2. Gerar layout final
python scripts/gera_export.py

## 3. Rodar agente no Streamlit
streamlit run scripts/app.py

## 4. Ingerir vetores no Qdrant
python scripts/ingest_excel_to_qdrant.py

# Roadmap de evolução
- 🔗 Conexão com bancos de dados dinâmicos (SQL/NoSQL).
- ⚙️ Integração com APIs de folha/benefícios.
- 📊 Dashboards gerenciais completos no Streamlit.
- ✅ Testes unitários para validação de regras de negócio.

# Conclusão
Este projeto demonstra como a **IA Generativa** e **bancos vetoriais** podem agregar valor no tratamento de dados corporativos.  
Mesmo partindo de **planilhas estáticas**, conseguimos construir uma solução que **simula cenários reais de empresas de benefícios (VR/VA)**.  
Com evolução para bancos dinâmicos, a solução se torna aplicável em **ambientes empresariais de grande escala**.

---

<p align="center">
  <a href="https://rodrigo-rrc.github.io/Projetos_IA/" target="_blank">
    <img src="https://img.shields.io/badge/⬅️ Voltar para o índice interativo-blue?style=for-the-badge" alt="Voltar para o índice interativo"/>
  </a>
</p>


## 👨‍💻 Autor

**Rodrigo Ribeiro Carvalho**  
GitHub: [Rodrigo-RRC](https://github.com/Rodrigo-RRC)  
LinkedIn: [linkedin.com/in/rodrigo-ribeiro-datascience](https://linkedin.com/in/rodrigo-ribeiro-datascience)  
WhatsApp: [Clique aqui para conversar](https://wa.me/5547991820339)

---

> Projeto desenvolvido para o **I2A2 – Criando Agentes Inteligentes com IA Generativa**.  
Mostra na prática como **dados comuns podem se transformar em inteligência de negócios** usando IA. 🚀
