# BreweriesCase
### BEES Data Engineering Breweries Case
##### Descrição do Projeto:
Este projeto consiste em uma pipeline ETL que consome dados da API Open Brewery DB, realiza transformações e persiste os dados em um data lake usando a arquitetura de medalhão (Bronze, Prata, Ouro) no databricks community. O objetivo é facilitar análises avançadas e fornecer insights sobre cervejarias nos EUA.

##### Arquitetura e Tecnologias Utilizadas
###### Componentes
- Databricks Community Edition: Plataforma para orquestração e execução de notebooks PySpark.
- PySpark: Linguagem usada para processamento de dados.
- Delta Lake: Formato de armazenamento para a camada Ouro.
- Cloud Service: (AWS S3: Utilizado para armazenar dados em diferentes camadas; AWS RDS: Banco de dados para armazenamento e análise de dados transformados; AWS Lambda: Gerencia a orquestração dos processos ETL).

##### Configuração e Instalação
###### Configurar AWS CLI (caso esteja implantando na AWS):
- Instale e configure o AWS CLI com suas credenciais.
###### Configuração do Ambiente Databricks:
- Crie um cluster e instale as bibliotecas necessárias (PySpark, requests, etc.).
###### Clone o Repositório:
- Clone este repositório para ter acesso a todos os scripts e notebooks.

##### Fluxo Geral da Pipeline
1. Coleta de Dados: Execute o fetch_data.py para coletar e armazenar dados na camada Bronze.
2. Transformação de Dados: Após a coleta, execute o transform.py para limpar e transformar os dados, movendo-os para a camada Prata.
3. Agregação de Dados: Finalmente, execute o aggreg_gold_data.py para agregar os dados e armazená-los na camada Ouro.

### Documentação dos Scripts 
#### 1. fetch_data.py
##### Descrição
- Este script é responsável por coletar dados da API Open Brewery DB. Ele faz requisições HTTP à API e salva os dados brutos recebidos diretamente no armazenamento do tipo Bronze no Databricks.

##### Execução
1. Configure seu cluster no Databricks para usar o Python 3.
2. Importe o script para o seu workspace no Databricks.
3. Execute o script no cluster configurado. Ele automaticamente fará as requisições e armazenará os dados em formato JSON.
##### Dependências
1. requests: Para realizar requisições HTTP.
2. Configuração de acesso à internet no cluster para acessar a API.


#### 2. transform.py
##### Descrição
- Este script transforma os dados brutos armazenados na camada Bronze, aplicando limpezas e transformações necessárias. O resultado é um conjunto de dados mais estruturado e limpo, que é então armazenado na camada Prata.

##### Execução
1. Certifique-se de que o fetch_data.py foi executado e os dados estão disponíveis na camada Bronze.
2. Importe e execute este script no Databricks para processar os dados.
3. Os dados transformados serão salvos automaticamente na camada Prata.

##### Dependências
1. pandas: Para manipulação de dados.
2. pyarrow: Para integração com armazenamento de dados no formato Parquet.


#### 3. aggreg_gold_data.py
##### Descrição
- Este script agrega os dados da camada Prata, realizando operações de agregação como contagens e médias, essencial para análises avançadas. Os dados agregados são então armazenados na camada Ouro, prontos para serem utilizados em relatórios e dashboards.

##### Execução
1. Assegure-se de que os dados na camada Prata estão atualizados e acessíveis.
2. Importe e execute este script no Databricks.
3. Os dados resultantes serão armazenados na camada Ouro.

##### Dependências
1. pyspark.sql: Para operações de DataFrame e SQL.
2. pyspark.sql.functions: Para funções de agregação.
