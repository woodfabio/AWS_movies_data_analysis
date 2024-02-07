# Projeto 2 - Criando um data lake na AWS 📈
Neste projeto faremos uma análise buscando gerar insights sobre dados a respeito da filmografia do ator Eric Roberts obtidos de arquivos locais e oriundos da API do IMDB usando recursos da AWS. O ator Eric Robets foi escolhido porque é atualmente o ator com participação no maior número de filmes, aumentando a quantidade de dados para serem usados neste projeto.
</br>

## Etapa I - Ingestão de dados para um AWS S3 em modo batch (camada Raw)
Arquivos: _Dockerfile_, _requirements.txt_, _upload_to_s3.py_
- Primeiramente utiliza-se um documento de texto "requirements.txt" que informa os recursos e respectivas versões que serão usados pelo Dockerfile.
- Foi criado também um código em Python chamado "upload_to_s3.py" para realizar a transferência de dois arquivos "movies.csv" e "series.csv" para um bucket Amazon S3.
- Utiliza-se um documento de texto "keys.txt" (não disponibilizado no github por razões de segurança) que contém o ID da chave de acesso, a chave secreta de acesso e o token da sessão AWS. Este documento precisa ser editado sempre que uma nova chave é utilizada. Este documento é lido no código de "upldoad_to_s3.py" e as suas informações são salvas em variáveis.
- Após, o código armazena os diretórios de origem e destino de ambos os arquivos em variáveis. O diretório de destino é estabelecido utilizando os valores de dia, mês e ano da data atual da execução para particionamento, obtidos utilizando-se a função "datetime.now()".
- Depois disso instancia-se um objeto utilizando-se a função "boto3.client()", o qual recebe as chaves de acesso como parâmetros.
- Por fim, utiliza-se do objeto instanciado pela função "boto3.client()" para realizar o upload dos arquivos para o S3, recebendo os diretórios de origem e destino como parâmetros.
</br>

## Etapa II - Ingestão de dados para o AWS S3 usando AWS Lambda (camada Trusted)
Arquivos: get_api_function.py_, _TMDB_API_key_
```
# Get current date for partitioning
current_date = datetime.now()
year = current_date.strftime("%Y")
month = current_date.strftime("%m")
day = current_date.strftime("%d")
```
 Aqui usamos a função "datetime.now()" para obter a data do momento de execução do código, quue será usada posteriormente para particionar o diretório do S3 onde os dados serão salvos
 ```
# AWS S3 client
s3_client = boto3.client('s3')
    
# --------------------------------------------------------------------------
# S3 bucket and keys
bucket_name = 'projeto2-bucket'
movies_csv_key = 'raw/local/CSV/Movies/2023/11/06/movies.csv'
series_csv_key = 'raw/local/CSV/Series/2023/11/06/series.csv'
default_api_movies_data_key = f"raw/TMDB/JSON/Movies/{year}/{month}/{day}/api_data_movies_"
default_api_series_data_key = f"raw/TMDB/JSON/Series/{year}/{month}/{day}/api_data_series_"

# --------------------------------------------------------------------------
# TMDB keys
TMDB_API_KEY = os.environ['api_key']
TMDB_READ_ACCESS_TOKEN = os.environ['api_read_access_token']
TMDB_API_ENDPOINT = 'https://api.themoviedb.org/3/movie/'
```
Aqui criamos variáveis para acessar o bucket S3, incluindo o cliente boto3, chaves de acesso e os diretórios onde os dados serão salvos.
```
# Function to download and read CSV files from S3
def read_csv_from_s3(bucket_name, file_key):
    object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pandas.read_csv(object['Body'], sep='|')
    return df
```
Aqui criamos uma função que usa o cliente boto3 para acessar o bucket S3, fazer download dos arquivos CSV e tranformá-los em DataFrames Pandas.
```
# Read "movies.csv"
movies_df = read_csv_from_s3(bucket_name, movies_csv_key)
    
# Filter movies with Eric Roberts
eric_roberts_movies = movies_df[movies_df['nomeArtista'] == 'Eric Roberts']
movie_ids = eric_roberts_movies['id'].tolist()
    
# --------------------------------------------------------------------------
# Read "series.csv"
series_df = read_csv_from_s3(bucket_name, series_csv_key)
    
# Filter series with Eric Roberts
eric_roberts_series = series_df[series_df['nomeArtista'] == 'Eric Roberts']
series_ids = eric_roberts_series['id'].tolist()
```
Agora usamos a função criada anteriormente para fazer o download dos arquivos "movies.csv" e "series.csv" do bucket S3 e depois filtramos apenas os registros cujo valor da coluna "nomeArtista" seja "Eric Roberts". Depois criamos uma lista que recebe apenas os valores da coluna "id" destes registros filtrados.
```
# Function to get movie details from API
def get_api_details(item_id):
    response = requests.get(f'{TMDB_API_ENDPOINT}{item_id}?api_key={TMDB_API_KEY}')
    if response.status_code == 200:
        item_data = response.json()
        return item_data
    else:
        return None    
```
Aqui criamos uma função que usa o pacote "requests" para fazer uma chamada pela API do TMDB para obter dados sobre filmes e séries. A função usa o id do filme e a chave de acesso da API para fazer a chamada e depois converte os dados para JSON.
No caso, não foram encontradas informações sobre as séries filtradas do arquivo "series.csv".
```
# Fetch detailed movies information and save as JSON in a list
api_movies_data_list = []
    
for movie_id in movie_ids:
    api_data = get_api_details(movie_id)
    if api_data:
        api_movies_data_list.append(api_data)
    
# --------------------------------------------------------------------------
# Fetch detailed series information and save as JSON in a list
api_series_data_list = []
    
for series_id in series_ids:
    api_data = get_api_details(series_id)
    if api_data:
        api_series_data_list.append(api_data)    
```
Agora usamos a função criada anteriormente para fazer chamadas pela API para cada filme e cada série filtrada e salvamos os dados obtidos em listas.
```
# Function to group data by schema
def group_records_by_schema(api_data):
    schema_grouped_data = {}
   
    for record in api_data:
        schema_key = tuple(sorted(record.keys()))
        if schema_key not in schema_grouped_data:
            schema_grouped_data[schema_key] = []
        schema_grouped_data[schema_key].append(record)
    
    return schema_grouped_data    
```
Aqui criamos uma função que agrupa os registros obtidos pela API por schema. A função analisa cada registro do input, organizando suas chaves em uma tupla e verificando se existem valores idênticos no dicionário "schema_grouped_data". Se não existirem, ela adiciona uma lista vazia referente a esse novo schema no dicionário. Após, ela salva as informações do registro da vez na lista do dicionário referente ao schema do registro.
```
# Group movies data by schema
schema_grouped_movies_api_data = group_records_by_schema(api_movies_data_list)
    
# --------------------------------------------------------------------------
# Group series data by schema
schema_grouped_series_api_data = group_records_by_schema(api_series_data_list)    
```
Agora usamos a função criada anteriormente para agrupar os dados obtidos pela API de acordo com os schemas.
```
# Function to upload API data to S3
def api_data_to_s3(data, bucket_name, file_key, s3_client):
    
    # Group movies data into chunks of 100 (or less) registers
    for schema, records in data.items():
        chunks = [records[i:i + 100] for i in range(0, len(records), 100)]
            
        # Save each chunk to S3
        for index, chunk in enumerate(chunks):
            updated_file_key = f"{file_key}{index+1}.json"
            json_data = json.dumps(chunk)
            s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=updated_file_key)      
```
Aqui criamos uma função para enviar os dados obtidos pela API para o bucket S3. Primeiro agrupamos os registros (já agrupados por schema) em conjuntos (chunks) de no máximo 100 registros. Depois salvamos cada "chunk" na forma de arquivo JSON no bucket S3 usando o cliente boto3. Usamos a variável "updated_file_key" para escrever o diretório de acordo com o número do "chunk".
```
# Save movies
api_data_to_s3(schema_grouped_movies_api_data, bucket_name, default_api_movies_data_key, s3_client)
    
# --------------------------------------------------------------------------
# Save series
api_data_to_s3(schema_grouped_series_api_data, bucket_name, default_api_series_data_key, s3_client)   
```
Agora usamos a função criada anteriormente para fazer o upload dos dados de filmes e séries obtidos pela API para o bucket S3. No caso, não foram obtidos dados pela API sobre séries.
```
# Function to download and read JSON files from S3 and turn into CSV
def read_json_from_s3(bucket_name, s3_client, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_content = response['Body'].read().decode('utf-8')
        #api_data = json.loads(json_content)
        #return api_data
        return json_content

    except Exception as e:
        print(f"Error reading JSON from S3: {e}")
        return None
```
Por fim, criamos uma função que recupera os dados obtidos pela API e salvos no bucket s3. A função usa o cliente boto3 para acessar o bucket S3 e obter os dados em formato JSON. ESta função não foi utilizada na versão final desta função Lambda, mas foi utilizada como alternativa para testes com os dados obtidos pela API sem a necessidade de fazer muitas requisições.
</br>

## Etapa III - Desenvolvimento das camadas Trusted e Refined do Data Lake usando AWS Glue
Arquivos: 
- **trusted_zone**: _projeto2_trusted_API_movies_1.py_, _projeto2_trusted_API_movies_2.py_, _projeto2_trusted_API_movies_3.py_, _projeto2_trusted_local_movies.py_, _projeto2_trusted_local_series.py_
- **refined_zone**: _projeto2_refined_movies.py_, _projeto2_refined_series.py_

### Camada Trusted:
#### projeto2_trusted_local_movies.py e projeto2_trusted_local_series.py

Ambos os arquivos citados acima são idênticos com a única diferença de trabalharem em cima de arquivos diferentes. Aqui usaremos _projeto2_trusted_local_movies.py_ como exemplo; o arquivo _projeto2_trusted_local_series.py_ difere apenas no fato de importar o arquivo _series.csv_ no lugar de _movies.csv_.

Após fazer as importações necessárias, inciar os contextos Glue e Spark, iniciar a sessão Spark e importar o arquivo _series.csv_ nós primeiramente eliminamos as colunas que não são do nosso interesse para a camada Trusted, no caso, apenas a coluna "titulosMaisConhecidos":
```
# specify useless columns to remove
columns_to_remove = ["titulosMaisConhecidos"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)
```

Em seguida nós mudamos os nomes das colunas para inglês de forma a uniformizar a nomenclatura em relação às demais tabelas da camada, pois as tabelas oriundas da API estão em inglês. Isto é feito criando um dicionário com os nomes originais e os novos nomes das colunas. Em seguida usamos um loop for para iterar sobre cada par chave-valor do dicionário de nomes, usando o par de nomes como parâmetros da função "rename_field" para trocar os nomes das colunas da tabela:
```
# Change column names to english and snake case for uniformization with other tables in the layer
columns_to_rename  = {"id": "imdb_id",
                        "tituloPincipal": "title", 
                        "tituloOriginal": "original_title", 
                        "anoLancamento": "release_year",
                        "tempoMinutos": "runtime",
                        "genero": "genre",
                        "notaMedia": "vote_average",
                        "numeroVotos": "vote_count",
                        "generoArtista": "artist_gender",
                        "personagem": "character",
                        "nomeArtista": "artist_name",
                        "anoNascimento": "birth_year",
                        "anoFalecimento": "death_year",
                        "profissao": "profession"
                    }
                
for old_name, new_name in columns_to_rename.items():
    dynamic_frame = dynamic_frame.rename_field(old_name, new_name)
```
Agora nós transformamos o DynamicFrame em um DataFrame para podermos executar determinadas funções neste tipo de tabela, como a filtragem de registros mantendo apenas aqueles que possuem o valor "Eric Roberts" na coluna "artist_name", pois nossa análise será sobre a filmografia deste ator.
```
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()
spark_df.printSchema()

# --------------------------------------------------------------------------
# filter movies with Eric Roberts
eric_roberts_movies = spark_df.filter(spark_df['artist_name'] == 'Eric Roberts')
```
Outra função que usaremos usando DataFrames é a função "explode", a qual cria cópias de uma linha que possua um valor em forma de array em alguma coluna (no caso, nas colunas "genre" e "profession") mantendo apenas um valor do array por linha. A função "split" divide a string contendo os valores baseada no caractere ",".
```
# un-nest nested columns and split them if they have more than one value

# split the "genre" column and explode the resulting array into separate rows
eric_roberts_movies = eric_roberts_movies.withColumn("genre", explode(split("genre", ",")))

# split the "profession" column and explode the resulting array into separate rows
eric_roberts_movies = eric_roberts_movies.withColumn("profession", explode(split("profession", ",")))
```
Agora podemos transformar o DataFrame novamente em um DynamicFrame para ser salvo no bucket S3 na pasta da camada Trusted, bem como registrar a tabela no Glue Catalog.
```
# repartition the DataFrame to a single partition
eric_roberts_movies = eric_roberts_movies.repartition(1)

# --------------------------------------------------------------------------
# save filtered DataFrame in parquet format [step substituted by the following steps due to using legacy functions]
#eric_roberts_movies.write.parquet(target_path, mode='overwrite')

# --------------------------------------------------------------------------
# turn DataFrame into glue DynamicFrame
new_dynamic_frame = DynamicFrame.fromDF(eric_roberts_movies, glueContext, "new_dynamic_frame")

# --------------------------------------------------------------------------
# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_trusted_zone", catalogTableName="trusted_local_movies")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(new_dynamic_frame)
```
O arquivo _projeto2_trusted_local_series.py_ possui basicamente a mesma estrutura, diferindo apenas no fato de importar o arquivo _series.csv_ no lugar de _movies.csv_.

#### projeto2_trusted_API_movies_1.py, projeto2_trusted_API_movies_2.py e projeto2_trusted_API_movies_3.py

Os três arquivos citados acima são idênticos com a única diferença de trabalharem em cima de cada um dos 3 arquivos JSON oriundos da API do IMDB (cada um com até 100 registros). Como exemplo descreveremos o arquivo projeto2_trusted_API_movies_1.py.
Assim como em _projeto2_trusted_local_movies.py_, após fazer as importações necessárias, inciar os contextos Glue e Spark, iniciar a sessão Spark e importar o arquivo _api_data_movies_1.json_ nós primeiramente eliminamos as colunas que não são do nosso interesse para a camada Trusted, no caso, as colunas "backdrop_path", "belongs_to_collection" e "id":
```
# specify useless columns to remove
columns_to_remove = ["backdrop_path", "belongs_to_collection", "id"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)
```
Assim como em _projeto2_trusted_local_movies.py_, após fazer as importações necessárias, inciar os contextos Glue e Spark, iniciar a sessão Spark e importar o arquivo _api_data_movies_1.json_ nós primeiramente eliminamos as colunas que não são do nosso interesse para a camada Trusted, no caso, as colunas "backdrop_path", "belongs_to_collection" e "id":
```
# specify useless columns to remove
columns_to_remove = ["backdrop_path", "belongs_to_collection", "id"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)
```
Novamente, assim como em _projeto2_trusted_local_movies.py_, usaremos a função "explode", a qual cria cópias de uma linha que possua um valor em forma de array em alguma coluna (no caso, nas colunas "genre", "production_companies" e "production_countries") mantendo apenas um valor do array por linha. Repare que enste caso os arrays possuem arrays aninhados, o que torna a extração um pouco mais complexa. Além disso, especificamente na coluna "production_country_name" uniformizamos alguns valores que se apresentavam com mais de uma forma na tabela original (no caso, o mesmo país aparecia como "US" em alguns registros e como "United States of America" em outros):
```
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

# "genres" column
spark_df = spark_df.select("*", explode("genres").alias("genre"))
spark_df = spark_df.withColumn("genre_id", col("genre.id"))
spark_df = spark_df.withColumn("genre_name", col("genre.name"))
spark_df = spark_df.drop("genres", "genre")

# "production_companies" column
spark_df = spark_df.select("*", explode("production_companies").alias("production_company"))
spark_df = spark_df.withColumn("company_id", col("production_company.id"))
spark_df = spark_df.withColumn("company_logo_path", col("production_company.logo_path"))
spark_df = spark_df.withColumn("company_name", col("production_company.name"))
spark_df = spark_df.withColumn("company_origin_country", col("production_company.origin_country"))
spark_df = spark_df.drop("production_companies", "production_company", "company_logo_path")

# "production_countries" column
spark_df = spark_df.select("*", explode("production_countries").alias("production_country"))
spark_df = spark_df.withColumn("production_country_iso_3166_1", col("production_country.iso_3166_1"))
spark_df = spark_df.withColumn("production_country_name", col("production_country.name"))
# =====
# uniformize column "production_country_name"
spark_df = spark_df.withColumn("production_country_name", when(col("production_country_name") == "United States of America", "US").otherwise(col("production_country_name")))
# =====
spark_df = spark_df.drop("production_countries", "production_country")

# "spoken_languages" column
spark_df = spark_df.select("*", explode("spoken_languages").alias("spoken_language"))
spark_df = spark_df.withColumn("spoken_language_english_name", col("spoken_language.english_name"))
spark_df = spark_df.withColumn("spoken_language_iso_639_1", col("spoken_language.iso_639_1"))
spark_df = spark_df.withColumn("spoken_language_name", col("spoken_language.name"))
spark_df = spark_df.drop("spoken_languages", "spoken_language")
```
Por fim, transformamos o DataFrame novamente em um DynamicFrame para ser salvo no bucket S3 na pasta da camada Trusted, bem como registramos a tabela no Glue Catalog.
```
# turn DataFrame into glue DynamicFrame
new_dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "new_dynamic_frame")

# --------------------------------------------------------------------------
# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_trusted_zone", catalogTableName="trusted_api_movies_1")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(new_dynamic_frame)
```
</br>

### Camada Refined:
#### projeto2_refined_movies.py

Após fazer as importações necessárias, inciar os contextos AWS Glue e Spark, iniciar a sessão Spark nós importamos as tabelas oriundas dos arquivos _projeto2_trusted_local_movies.py_, _projeto2_trusted_API_movies_1.py_, _projeto2_trusted_API_movies_2.py_, e _projeto2_trusted_API_movies_3.py_:
```
# read data from S3 and create DynamicFrames

# local
dynamic_frame_local = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_local
        ]
    },
    "parquet"
    )

# API 1
dynamic_frame_api_1 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_1
        ]
    },
    "parquet"
    )

# API 2
dynamic_frame_api_2 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_2
        ]
    },
    "parquet"
    )

# API 3
dynamic_frame_api_3 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_3
        ]
    },
    "parquet"
    )
```
Agora elimnamos colunas que não são do interessa da camada Refined. No caso, como estamos analisando a filmografia de Eric Roberts todos os registros fazem referência a ele, razão pela qual informações pessoais sobre ele são redundantes na tabela:
```
# specify useless columns to remove

# local
local_columns_to_remove = ["title", "artist_gender", "character", "artist_name", "birth_year", "death_year"]
dynamic_frame_local = DropFields.apply(frame = dynamic_frame_local, paths = local_columns_to_remove)

# APIs
api_columns_to_remove = ["homepage", "overview", "poster_path", "tagline", "title", "video", "genre_id", "company_id", "production_country_iso_3166_1", "spoken_language_iso_639_1", "spoken_language_name"]
dynamic_frame_api_1 = DropFields.apply(frame = dynamic_frame_api_1, paths = api_columns_to_remove)
dynamic_frame_api_2 = DropFields.apply(frame = dynamic_frame_api_2, paths = api_columns_to_remove)
dynamic_frame_api_3 = DropFields.apply(frame = dynamic_frame_api_3, paths = api_columns_to_remove)
```
Após, modificamos o nome da coluna "genre_name" para uniformidade entre as tabelas:
```
# change API DynamicFrames column names for uniformization
columns_to_rename  = {"genre_name": "genre"}
                
for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_1 = dynamic_frame_api_1.rename_field(old_name, new_name)

for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_2 = dynamic_frame_api_2.rename_field(old_name, new_name)

for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_3 = dynamic_frame_api_3.rename_field(old_name, new_name)
```
Agora transformamos os DynamicFrames em DataFrames para podermos executar comandos SQL. Repare que nós garantimos que cada tabela esterá em uma única partição para evitar inconsistências nas operações:
```
# turn DynamicFrames into spark DataFrames
spark_df_local = dynamic_frame_local.toDF()
spark_df_api_1 = dynamic_frame_api_1.toDF()
spark_df_api_2 = dynamic_frame_api_2.toDF()
spark_df_api_3 = dynamic_frame_api_3.toDF()

# --------------------------------------------------------------------------
# repartition the DataFrames to a single partition
spark_df_local = spark_df_local.repartition(1)
spark_df_api_1 = spark_df_api_1.repartition(1)
spark_df_api_2 = spark_df_api_2.repartition(1)
spark_df_api_3 = spark_df_api_3.repartition(1)

```
Iremos aplicar comandos SQL para coletar todos os valores de "company_name" referentes a cada valor de "imdb_id" (ou seja, referente a cada filme) em cada tabela oriunda dos dados de API e salvá-las em arrays referentes a cada filme. Depois nós pegamos as tabelas resultantes de cada uma destas operações e as unificamos. Faremos isto para posteriormente associarmos estes arrays a cada filme na tabela "dynamic_frame_local" e usarmos a função "explode" para multiplicar os registros individualizando cada valor nos arrays.
Repare que mesmo usando o comando "DISTINCT" na query SQL nós ainda assim usamos o comando "dropDuplicates" para garantir que não teremos registros duplicados que distorceriam nossas análises.
Repare também que o comando "reateOrReplaceTempView" serve para criar ou atualizar uma "view temporária", ou seja, uma tabela referenciável por comandos SQL oriunda do DataFrame.
```
# Run Spark SQL queries in API tables to group by "imdb_id" and collect_list "company_name"

spark_df_api_1.createOrReplaceTempView("spark_df_api_1")
collected_cn_api_1 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_1
    GROUP BY
        imdb_id
""")
collected_cn_api_1 = collected_cn_api_1.dropDuplicates(collected_cn_api_1.columns)
collected_cn_api_1.createOrReplaceTempView("collected_cn_api_1")

spark_df_api_2.createOrReplaceTempView("spark_df_api_2")
collected_cn_api_2 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_2
    GROUP BY
        imdb_id
""")
collected_cn_api_2 = collected_cn_api_2.dropDuplicates(collected_cn_api_2.columns)
collected_cn_api_2.createOrReplaceTempView("collected_cn_api_2")

spark_df_api_3.createOrReplaceTempView("spark_df_api_3")
collected_cn_api_3 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_3
    GROUP BY
        imdb_id
""")
collected_cn_api_3 = collected_cn_api_3.dropDuplicates(collected_cn_api_3.columns)
collected_cn_api_3.createOrReplaceTempView("collected_cn_api_3")

# --------------------------------------------------------------------------
#Unify API data

unified_api = spark.sql("""
    SELECT * FROM collected_cn_api_1
    UNION ALL
    SELECT * FROM collected_cn_api_2
    UNION ALL
    SELECT * FROM collected_cn_api_3;
""")
unified_api.createOrReplaceTempView("unified_api")
```
Após isso, rodamos outro comando SQL para adicionar a tabela unificada "unified_api" à tabela "spark_df_local" usando um left join baseado nos valores de "imdb_id":
```
# Run SQL INSERT INTO statement to add companies_names to "spark_df_local" as "final_table"
spark_df_local.createOrReplaceTempView("spark_df_local")
joined_table_cn_1 = spark.sql("""
    SELECT
        spark_df_local.*,
        unified_api.companies_names
    FROM
        spark_df_local
    LEFT JOIN
        unified_api
    ON
        spark_df_local.imdb_id = unified_api.imdb_id
""")
joined_table_cn_1 = joined_table_cn_1.dropDuplicates(joined_table_cn_1.columns)
joined_table_cn_1.createOrReplaceTempView("joined_table_cn_1")

```
Após isso, rodamos outro comando SQL para adicionar a tabela unificada "unified_api" à tabela "spark_df_local" usando um left join baseado nos valores de "imdb_id":
```
# Run SQL INSERT INTO statement to add companies_names to "spark_df_local" as "final_table"
spark_df_local.createOrReplaceTempView("spark_df_local")
joined_table_cn_1 = spark.sql("""
    SELECT
        spark_df_local.*,
        unified_api.companies_names
    FROM
        spark_df_local
    LEFT JOIN
        unified_api
    ON
        spark_df_local.imdb_id = unified_api.imdb_id
""")
joined_table_cn_1 = joined_table_cn_1.dropDuplicates(joined_table_cn_1.columns)
joined_table_cn_1.createOrReplaceTempView("joined_table_cn_1")

```
Agora podemos eliminar as views temporárias criadas anteriormente (para evitar conflitos de nomes em comandos SQL futuros) e usar a função "explode" para multiplicar os registros desaninhando os valores nos arrays da coluna "companies_names":
```
# un-nest nested columns and split them if they have more than one value

spark.catalog.dropTempView("collected_cn_api_1")
spark.catalog.dropTempView("collected_cn_api_2")
spark.catalog.dropTempView("collected_cn_api_3")
spark.catalog.dropTempView("unified_api")

# explode the arrays in "companies_names" into separate rows
joined_table_cn_1 = joined_table_cn_1.withColumn("company_name", explode("companies_names"))
joined_table_cn_1 = joined_table_cn_1.drop("companies_names")
joined_table_cn_1 = joined_table_cn_1.dropDuplicates(joined_table_cn_1.columns)
joined_table_cn_1.createOrReplaceTempView("joined_table_cn_1")

```
Já tendo os valores de "company_name" associados aos filmes, podemos realizar left joins entre a nossa tabela e cada uma das tabelas oriundas de dados da API para associar os valores de "company_origin_country" a cada "company_name". Novamente usamos "DISTINCT" e "dropDuplicates" para assegurar que não teremos registros duplicados:
```
# Run SQL INSERT INTO statement to add company_origin_country to "joined_table_cn_1" as "joined_table_cc_3"

joined_table_cc_1 = spark.sql("""
    SELECT DISTINCT
        joined_table_cn_1.*,
        spark_df_api_1.company_origin_country
    FROM
        joined_table_cn_1
    LEFT JOIN
        spark_df_api_1
    ON
        joined_table_cn_1.company_name = spark_df_api_1.company_name
""")
joined_table_cc_1 = joined_table_cc_1.dropDuplicates(joined_table_cc_1.columns)
joined_table_cc_1.createOrReplaceTempView("joined_table_cc_1")

joined_table_cc_2 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_1.*,
        spark_df_api_2.company_origin_country
    FROM
        joined_table_cc_1
    LEFT JOIN
        spark_df_api_2
    ON
        joined_table_cc_1.company_name = spark_df_api_2.company_name
""")
joined_table_cc_2 = joined_table_cc_2.dropDuplicates(joined_table_cc_2.columns)
joined_table_cc_2.createOrReplaceTempView("joined_table_cc_2")

joined_table_cc_3 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_2.*,
        spark_df_api_3.company_origin_country
    FROM
        joined_table_cc_2
    LEFT JOIN
        spark_df_api_3
    ON
        joined_table_cc_2.company_name = spark_df_api_3.company_name
""")
joined_table_cc_3 = joined_table_cc_3.dropDuplicates(joined_table_cc_3.columns)
joined_table_cc_3.createOrReplaceTempView("joined_table_cc_3")
```
Agora realizamos um passo-a-passo semelhante ao feito com a coluna "companies_names" para adicionar os valores de "production_countries_names" à nossa tablea:
```
# Run Spark SQL queries in API tables to group by "imdb_id" and collect_list "production_countries_names"

collected_pc_api_1 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_1
    GROUP BY
        imdb_id
""")
collected_pc_api_1 = collected_pc_api_1.dropDuplicates(collected_pc_api_1.columns)
collected_pc_api_1.createOrReplaceTempView("collected_pc_api_1")

collected_pc_api_2 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_2
    GROUP BY
        imdb_id
""")
collected_pc_api_2 = collected_pc_api_2.dropDuplicates(collected_pc_api_2.columns)
collected_pc_api_2.createOrReplaceTempView("collected_pc_api_2")

collected_pc_api_3 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_3
    GROUP BY
        imdb_id
""")
collected_pc_api_3 = collected_pc_api_3.dropDuplicates(collected_pc_api_3.columns)
collected_pc_api_3.createOrReplaceTempView("collected_pc_api_3")

# --------------------------------------------------------------------------
#Unify API data

unified_api_2 = spark.sql("""
    SELECT * FROM collected_pc_api_1
    UNION ALL
    SELECT * FROM collected_pc_api_2
    UNION ALL
    SELECT * FROM collected_pc_api_3;
""")
unified_api_2 = unified_api_2.dropDuplicates(unified_api_2.columns)
unified_api_2.createOrReplaceTempView("unified_api_2")

# --------------------------------------------------------------------------
# Run SQL INSERT INTO statement to add production_countries_names to "joined_table_cc_3" as "joined_table_pc_1"

joined_table_pc_1 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_3.*,
        unified_api_2.production_countries_names
    FROM
        joined_table_cc_3
    LEFT JOIN
        unified_api_2
    ON
        joined_table_cc_3.imdb_id = unified_api_2.imdb_id
""")
joined_table_pc_1 = joined_table_pc_1.dropDuplicates(joined_table_pc_1.columns)
joined_table_pc_1.createOrReplaceTempView("joined_table_pc_1")

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

spark.catalog.dropTempView("collected_pc_api_1")
spark.catalog.dropTempView("collected_pc_api_2")
spark.catalog.dropTempView("collected_pc_api_3")
spark.catalog.dropTempView("unified_api_2")

# explode the arrays in "production_countries_names" into separate rows
joined_table_pc_1 = joined_table_pc_1.withColumn("production_country_name", explode("production_countries_names"))
joined_table_pc_1 = joined_table_pc_1.drop("production_countries_names")
joined_table_pc_1 = joined_table_pc_1.dropDuplicates(joined_table_pc_1.columns)
joined_table_pc_1.createOrReplaceTempView("joined_table_pc_1")

```
Vamos agora fazer um casting para mudar o tipo da coluna "runtime" de string para inteiro, uma vez que é um valor numérico que indica oa quantidade em minutos de duração do filme:
```
# --------------------------------------------------------------------------
# cast the "runtime" column from string to integer
joined_table_pc_1 = joined_table_pc_1.withColumn("runtime", col("runtime").cast("int"))
```
Agora temos uma única tabela com os dados que nos interessam sobre os filmes de Eric Roberts pronta para ser utilizada pelo Amazon QuickSight.
Por fim, asseguramos que nosso DataFrame está particionado em uma única partição e o transformamos novamente em um DynamicFrame para ser salvo no bucket S3 na pasta da camada Refined, bem como registramos a tabela no Glue Catalog. 
```
# repartition the DataFrames to a single partition
joined_table_pc_1 = joined_table_pc_1.repartition(1)

# --------------------------------------------------------------------------
# turn DataFrames into glue DynamicFrames
unified_movies_table = DynamicFrame.fromDF(joined_table_pc_1, glueContext, "unified_movies_table")

# --------------------------------------------------------------------------
# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_refined_zone", catalogTableName="unified_movies_table")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(unified_movies_table)

```
</br>

#### projeto2_refined_series.py
Este código é uma versão muito mais simples do código anterior. Ele lé o arquivo oriundo do script _projeto2_refined_series.py_ como um DynamicFrame, tranforma em um DataFrame, remove as colunas inúteis, transforma novamente em DynamicFrame e salva no bucket S3, registrando a tabela no Glue Catalog. 

```
# specify useless columns to remove
columns_to_remove = ["title", "artist_gender", "character", "artist_name", "birth_year", "death_year"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)

# --------------------------------------------------------------------------
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()

# --------------------------------------------------------------------------
# cast the "runtime" column from string to integer
spark_df = spark_df.withColumn("runtime", col("runtime").cast("int"))

# --------------------------------------------------------------------------
# repartition the DataFrame to a single partition
spark_df = spark_df.repartition(1)

# --------------------------------------------------------------------------
# turn DataFrame into glue DynamicFrame
new_dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "new_dynamic_frame")
```
</br>

## Etapa IV - Gráficos (dashboards) usando AWS QuickSight
Arquivos: _movies_dashboard.pdf_, _series_dashboard.pdf_

Aqui temos alguns gráficos gerados a partir dos dados previamente preparados usando o AWS QuickSight, possibilidando a geração de insights a partis da informações obtidas. O primeiro arquivo (que possui informações sobre os filmes) possui informações mais detalhadas e ricas em relação ao segundo (que possui informações sobre as séries) devido à maior quantidade de dados disponibilizados.
