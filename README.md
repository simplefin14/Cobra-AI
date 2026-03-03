 Projeto desenvolvido do zero para automatizar processos de cobrança e reduzir trabalho repetitivo de equipes.

📝 Descrição do Projeto

Este projeto oferece uma solução robusta e escalável para a automação do processo de cobrança e comunicação com clientes via WhatsApp. 
Utilizando Inteligência Artificial (IA) para gerar mensagens personalizadas e humanizadas, a plataforma permite que empresas otimizem seus processos de recuperação de crédito, garantindo eficiência e evitando o risco de banimento através de controles de frequência.

Funcionalidades

🔑 Login com telefone corporativo + geração de QR Code para conectar o WhatsApp	✅

📤 Upload de planilha Excel/CSV com clientes inadimplentes	✅

🤖 Geração automática de mensagens personalizadas por IA	✅

🎚️ Seleção de tom da mensagem (Empático / Formal / Urgente)	✅

📊 Agrupamento automático de múltiplos boletos por cliente	✅

👀 Pré-visualização de todas as mensagens em formato de tabela	✅

✏️ Edição de mensagens antes do envio	✅

⏱️ Envio em lote com intervalo humanizado automático anti-banimento	✅

📝 Armazenamento de todos os registros no Google Sheets	✅

⚡ Arquitetura assíncrona, sem erros de timeout	✅

⚖️ Balanceamento automático entre múltiplas chaves da API do Groq para evitar rate limit	✅

🧹 Normalização automática de números de telefone e códigos de cliente	✅


Tecnologias Utilizadas

Python: Linguagem principal para o frontend (Streamlit) e scripts auxiliares.

Streamlit: Framework para construção da interface web interativa.

Pandas: Manipulação e análise de dados (planilhas Excel/CSV).

Requests: Para comunicação HTTP com APIs externas (n8n, Evolution API).

gspread: Integração com Google Sheets API.

n8n: Plataforma de automação de workflow (self-hosted) para orquestrar todo o processo (leitura de dados, chamadas à IA, lógica de loop, 
integração com Evolution API).

Groq API: Provedor de Inteligência Artificial para geração de texto (modelos llama-3.1-8b-instant, llama-3.3-70b-versatile).

Evolution API: Gateway de WhatsApp (self-hosted) para gerenciamento de instâncias e envio de mensagens.

Google Sheets: Banco de dados flexível para entrada de dados dos clientes e armazenamento temporário das mensagens geradas pela IA.

Docker / Docker Compose: Para containerização e orquestração dos serviços (n8n, Evolution API, Redis).
