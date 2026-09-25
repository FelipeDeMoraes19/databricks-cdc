# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Recursos do Delta Lake — histórico, time travel e otimização
# MAGIC
# MAGIC **Objetivo deste notebook:** explorar três recursos do Delta Lake que não
# MAGIC existem em Parquet puro, e que são o motivo de usar Delta em vez de
# MAGIC arquivos soltos numa camada de dados:
# MAGIC
# MAGIC 1. **`DESCRIBE HISTORY`** — o log de transações da tabela: quem escreveu o
# MAGIC    quê, quando, e como.
# MAGIC 2. **Time travel (`VERSION AS OF`)** — consultar (ou restaurar) qualquer
# MAGIC    versão anterior da tabela, sem precisar de backup separado.
# MAGIC 3. **`OPTIMIZE`, `ZORDER BY` e Liquid Clustering** — como o Delta lida com
# MAGIC    o problema de "muitos arquivos pequenos", que o próprio projeto real já
# MAGIC    documenta como uma limitação conhecida (bronze cresce sem compactação:
# MAGIC    22.222 arquivos, 186 MB, e o job de Spark foi de menos de um minuto
# MAGIC    para mais de dez).
# MAGIC
# MAGIC Tudo isso funciona porque cada escrita em uma tabela Delta não sobrescreve
# MAGIC dados — ela **adiciona uma nova versão** ao log de transações
# MAGIC (`_delta_log/`), que é a fonte de verdade sobre quais arquivos Parquet
# MAGIC pertencem a cada versão da tabela.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "workspace"
schema = "payments_cdc_demo"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

silver_table = f"{catalog}.{schema}.silver_payments_current"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `DESCRIBE HISTORY`
# MAGIC
# MAGIC Toda operação que escreve na tabela (`WRITE`, `MERGE`, `OPTIMIZE`, ...) vira
# MAGIC uma linha aqui — com **versão**, **timestamp**, **usuário**,
# MAGIC **parâmetros da operação** e **métricas** (quantas linhas foram inseridas,
# MAGIC atualizadas, deletadas, quantos arquivos reescritos, etc).
# MAGIC
# MAGIC **Por que importa em produção:** é a resposta para "quem mudou essa tabela
# MAGIC e o quê, exatamente?" sem precisar de um sistema de auditoria à parte — o
# MAGIC log de transações do Delta *é* a auditoria. Esse notebook, por exemplo,
# MAGIC acumulou várias versões só de hoje: as cargas do `02_silver`, as tentativas
# MAGIC de merge do `03_merge` (inclusive a que falhou de propósito, que **não**
# MAGIC aparece aqui — uma transação que nunca commitou não vira versão).

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY {silver_table}")

display(
    history_df.select(
        "version", "timestamp", "operation", "operationParameters", "operationMetrics"
    ).orderBy("version")
)

print(f"Total de versões na tabela: {history_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel: comparando a versão antes e depois do `MERGE`
# MAGIC
# MAGIC Em vez de adivinhar o número da versão, vamos **encontrá-la** a partir do
# MAGIC histórico: procuramos a operação `MERGE` mais recente que de fato inseriu
# MAGIC linhas novas (`operationMetrics.numTargetRowsInserted > 0`) — isso isola a
# MAGIC execução "de verdade" do `03_merge`, e não o merge bônus de teste do
# MAGIC `DELETE` atrasado (que não muda nada, de propósito).

# COMMAND ----------

merge_version_row = (
    history_df.filter(
        (F.col("operation") == "MERGE")
        & (F.col("operationMetrics.numTargetRowsInserted").cast("int") > 0)
    )
    .orderBy(F.col("version").desc())
    .select("version")
    .first()
)

version_after = merge_version_row["version"]
version_before = version_after - 1

print(f"Versão ANTES do merge: {version_before}")
print(f"Versão DEPOIS do merge: {version_after}")

# COMMAND ----------

# MAGIC %md
# MAGIC A sintaxe `VERSION AS OF` faz exatamente o que parece: lê a tabela como
# MAGIC ela existia naquela versão específica, sem afetar a versão atual nem
# MAGIC precisar restaurar nada.

# COMMAND ----------

before_df = spark.sql(f"SELECT * FROM {silver_table} VERSION AS OF {version_before}")
after_df = spark.sql(f"SELECT * FROM {silver_table} VERSION AS OF {version_after}")

print(f"Linhas na versão {version_before} (antes do merge): {before_df.count()}")
print(f"Linhas na versão {version_after} (depois do merge):  {after_df.count()}")

# COMMAND ----------

print("payment_id = 30 ANTES do merge (deveria existir):")
display(before_df.filter(F.col("payment_id") == 30))

print("payment_id = 30 DEPOIS do merge (deveria ter sumido — foi deletado):")
display(after_df.filter(F.col("payment_id") == 30))

print("payment_id = 41 ANTES do merge (não deveria existir ainda):")
display(before_df.filter(F.col("payment_id") == 41))

print("payment_id = 41 DEPOIS do merge (pagamento novo, inserido pelo merge):")
display(after_df.filter(F.col("payment_id") == 41))

# COMMAND ----------

# MAGIC %md
# MAGIC **Por que importa em produção:**
# MAGIC - Debugar um resultado suspeito na gold: dá pra perguntar "o que a silver
# MAGIC   tinha antes desse job rodar?" sem precisar de um backup separado.
# MAGIC - Reproduzir um relatório de uma data específica.
# MAGIC - Recuperar de um bug: `RESTORE TABLE tabela TO VERSION AS OF n` volta a
# MAGIC   tabela de verdade para uma versão anterior (não fizemos isso aqui para
# MAGIC   não perder o resultado do `03_merge`, mas o comando existe e é assim).
# MAGIC
# MAGIC **A pegadinha:** time travel não é infinito. Ele depende dos arquivos
# MAGIC Parquet antigos ainda existirem — e é exatamente isso que o `VACUUM`
# MAGIC remove (por padrão, tudo com mais de 7 dias). Rodar `VACUUM` de forma
# MAGIC agressiva quebra o time travel para versões mais antigas que a retenção
# MAGIC configurada. Não rodamos `VACUUM` neste notebook de propósito, para manter
# MAGIC o histórico intacto para esta demonstração.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `OPTIMIZE` — compactando arquivos pequenos
# MAGIC
# MAGIC Cada `MERGE`/`WRITE` que fizemos até agora gerou seus próprios arquivos
# MAGIC Parquet pequenos (nossa tabela tem só 47 linhas, mas em produção uma
# MAGIC tabela que recebe muitos merges incrementais pequenos acumula **muitos**
# MAGIC arquivos pequenos — o mesmo "small file problem" que o projeto real
# MAGIC documenta na bronze). `OPTIMIZE` reescreve esses arquivos pequenos em
# MAGIC arquivos maiores (padrão ~1 GB), sem mudar nenhum dado — só a forma como
# MAGIC ele está fisicamente organizado em disco.
# MAGIC
# MAGIC **Por que importa em produção:** menos arquivos = menos overhead de listar
# MAGIC e abrir arquivos a cada leitura = queries mais rápidas. É rotina, não é
# MAGIC único: em produção normalmente roda agendado (ex.: uma vez por dia).

# COMMAND ----------

optimize_result = spark.sql(f"OPTIMIZE {silver_table}")
display(optimize_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `ZORDER BY` — organizando os dados por coluna de filtro
# MAGIC
# MAGIC `OPTIMIZE ... ZORDER BY (coluna)` vai além de só compactar: ele também
# MAGIC **reorganiza** as linhas para que valores parecidos daquela coluna fiquem
# MAGIC nos mesmos arquivos. O Delta guarda estatísticas de min/max por arquivo, e
# MAGIC o otimizador usa isso para **pular arquivos inteiros** (data skipping)
# MAGIC quando a query filtra por essa coluna — sem nem abrir o arquivo.
# MAGIC
# MAGIC Escolhemos `payment_id` porque é a coluna mais provável de aparecer num
# MAGIC `WHERE payment_id = ...` ou num `JOIN` com outra tabela (ex.: `customers`).
# MAGIC
# MAGIC **Por que importa em produção:** numa tabela de milhões de linhas, uma
# MAGIC query que filtra por `payment_id` sem Z-order pode precisar escanear todos
# MAGIC os arquivos; com Z-order, ela pula a maioria.

# COMMAND ----------

zorder_result = spark.sql(f"OPTIMIZE {silver_table} ZORDER BY (payment_id)")
display(zorder_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering — a alternativa mais nova ao `ZORDER`
# MAGIC
# MAGIC `ZORDER` tem uma limitação: cada vez que você roda `OPTIMIZE ZORDER BY`,
# MAGIC ele reconsidera a tabela inteira (ou uma fatia grande dela) para reordenar
# MAGIC os dados — é uma operação pesada e você precisa lembrar de agendá-la.
# MAGIC **Liquid Clustering** (`CLUSTER BY`) é a evolução disso: o Delta mantém o
# MAGIC clustering **incrementalmente**, à medida que os dados chegam, e permite
# MAGIC trocar as colunas de clustering depois sem reescrever tudo. É o que a
# MAGIC Databricks recomenda hoje como padrão para tabelas novas, no lugar de
# MAGIC particionamento fixo ou `ZORDER`.
# MAGIC
# MAGIC Vamos tentar habilitar na nossa tabela — envolvido em `try/except` porque
# MAGIC nem todo workspace/edição do Databricks garante suporte a isso.

# COMMAND ----------

try:
    spark.sql(f"ALTER TABLE {silver_table} CLUSTER BY (payment_id)")
    print("Liquid clustering habilitado na tabela.")

    cluster_optimize_result = spark.sql(f"OPTIMIZE {silver_table}")
    display(cluster_optimize_result)

    display(spark.sql(f"DESCRIBE TABLE EXTENDED {silver_table}").filter(F.col("col_name") == "Clustering Information"))
except Exception as e:
    print("Liquid clustering não pôde ser habilitado neste workspace/tabela:")
    print(str(e).splitlines()[0])
    print("Isso não invalida o restante da demo — ZORDER BY já resolve o mesmo problema.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulando para a entrevista
# MAGIC
# MAGIC | Pergunta que podem fazer | Resposta curta |
# MAGIC |---|---|
# MAGIC | Como o Delta sabe quais arquivos pertencem a cada versão? | O log de transações (`_delta_log/`), uma sequência de arquivos JSON/Parquet que listam quais arquivos Parquet foram adicionados/removidos em cada commit. `DESCRIBE HISTORY` é a visão amigável desse log. |
# MAGIC | Uma transação que falhou (nosso merge sem dedup) aparece no histórico? | Não. Só transações que **commitam** viram uma nova versão. Uma exceção antes do commit não deixa rastro na tabela. |
# MAGIC | Time travel funciona pra sempre? | Não — depende dos arquivos Parquet antigos ainda existirem. `VACUUM` remove arquivos órfãos mais antigos que o período de retenção (padrão 7 dias) e isso limita até onde dá para voltar. |
# MAGIC | Qual a diferença entre `OPTIMIZE` sozinho e `OPTIMIZE ... ZORDER BY`? | `OPTIMIZE` sozinho só compacta arquivos pequenos em maiores. `ZORDER BY` também reorganiza o conteúdo pra colocar valores parecidos juntos, habilitando data skipping em queries que filtram por aquela coluna. |
# MAGIC | Por que Liquid Clustering em vez de `ZORDER`/particionamento? | Clustering incremental (não precisa reprocessar a tabela inteira a cada `OPTIMIZE`) e as colunas de clustering podem mudar sem reescrever os dados — mais flexível para uma tabela que evolui. |
# MAGIC
# MAGIC ## Fim da demo
# MAGIC Os quatro notebooks juntos cobrem o ciclo completo: captura crua (bronze),
# MAGIC limpeza e contrato de tipos (silver), upsert incremental (`MERGE INTO`), e
# MAGIC os recursos do Delta que sustentam tudo isso em produção (histórico, time
# MAGIC travel, otimização de arquivos).
