# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — deduplicação, "estado atual" e tipagem
# MAGIC
# MAGIC **Objetivo deste notebook:** ler a bronze (crua, com duplicatas, fora de
# MAGIC ordem, tudo como texto) e produzir uma tabela silver com:
# MAGIC 1. **Uma linha por `payment_id`** — o estado mais recente daquele pagamento,
# MAGIC    decidido pelo maior `lsn` visto até agora.
# MAGIC 2. **Colunas tipadas** — `payment_id` como `bigint`, `amount` como
# MAGIC    `decimal(14,2)` (mesma precisão do contrato Pydantic do projeto real —
# MAGIC    `contracts/tables.py`), `updated_at` como `timestamp`.
# MAGIC
# MAGIC ### Recorte de escopo (importante para a entrevista)
# MAGIC O projeto real (`payments-cdc-observability`) mantém a silver como um
# MAGIC **change log limpo**, com `is_current` marcado por linha — porque lá a gold
# MAGIC precisa reconstruir SCD Type 2 (histórico completo de mudanças). Neste demo
# MAGIC eu simplifiquei de propósito: a silver aqui guarda **só o estado atual**,
# MAGIC porque é isso que o `03_merge` (próximo notebook) precisa como alvo de um
# MAGIC `MERGE INTO`/upsert. É uma decisão de escopo, não uma limitação — vale
# MAGIC comentar isso se perguntarem "por que sua silver não guarda histórico?".

# COMMAND ----------

catalog = "workspace"
schema = "payments_cdc_demo"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

bronze_table = f"{catalog}.{schema}.bronze_payments_cdc"
silver_table = f"{catalog}.{schema}.silver_payments_current"

bronze_df = spark.table(bronze_table)
bronze_count = bronze_df.count()
print(f"Bronze ({bronze_table}): {bronze_count} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A window function, linha por linha
# MAGIC
# MAGIC ```python
# MAGIC window_spec = (
# MAGIC     Window.partitionBy("payment_id")
# MAGIC     .orderBy(F.col("lsn").desc())
# MAGIC )
# MAGIC ```
# MAGIC - **`Window.partitionBy("payment_id")`** — divide o DataFrame em grupos
# MAGIC   independentes, um por `payment_id`. É parecido com um `GROUP BY`, mas
# MAGIC   **não colapsa as linhas**: cada linha continua existindo, só passa a
# MAGIC   "saber" em qual grupo ela está.
# MAGIC - **`.orderBy(F.col("lsn").desc())`** — dentro de cada grupo, ordena as
# MAGIC   linhas por `lsn` decrescente. Isso é o que resolve a **desordem física**
# MAGIC   dos dados: não importa em que ordem as linhas chegaram no arquivo/bronze,
# MAGIC   a window reordena *logicamente* por `lsn` antes de fazer qualquer conta.
# MAGIC   O evento com o `lsn` mais alto (a escrita mais recente no WAL para
# MAGIC   aquele pagamento) fica em primeiro lugar.
# MAGIC
# MAGIC ```python
# MAGIC ranked_df = bronze_df.withColumn("rn", F.row_number().over(window_spec))
# MAGIC ```
# MAGIC - **`F.row_number().over(window_spec)`** — numera as linhas de cada grupo
# MAGIC   sequencialmente (1, 2, 3, ...), seguindo a ordem definida acima. A linha
# MAGIC   com `rn = 1` é a "vencedora" de cada `payment_id`: o evento mais recente.
# MAGIC   Repare que `row_number` sempre dá números **distintos**, mesmo para
# MAGIC   linhas empatadas (duas cópias idênticas com o mesmo `lsn` por causa de
# MAGIC   duplicata) — uma delas vira `rn=1`, a outra `rn=2`. Como o conteúdo é
# MAGIC   idêntico, não importa qual das duas "ganha".
# MAGIC
# MAGIC ```python
# MAGIC deduped_df = ranked_df.filter(F.col("rn") == 1).drop("rn")
# MAGIC ```
# MAGIC - **`.filter(F.col("rn") == 1)`** — mantém só a linha vencedora de cada
# MAGIC   grupo. Isso resolve **os dois problemas da bronze ao mesmo tempo**:
# MAGIC   - Se um evento antigo foi duplicado, ele nunca vira `rn=1` (a menos que
# MAGIC     seja o próprio evento de maior `lsn`, caso em que a duplicata é
# MAGIC     idêntica e não importa qual cópia sobra).
# MAGIC   - Sobra exatamente **uma linha por `payment_id`**, que é o estado atual.
# MAGIC - **`.drop("rn")`** — remove a coluna auxiliar; ela só existia para o
# MAGIC   filtro, não faz sentido gravá-la na silver.
# MAGIC
# MAGIC **Por que não usar `dropDuplicates()` sozinho?** Ele só removeria linhas
# MAGIC *exatamente iguais* — não saberia decidir qual `status` é o mais recente
# MAGIC entre um `PENDING` e um `CAPTURED` do mesmo pagamento. Precisamos de uma
# MAGIC noção de **ordem** (`lsn`), e é exatamente isso que uma window function
# MAGIC com `orderBy` dá e um `dropDuplicates` não tem como saber.
# MAGIC
# MAGIC **Por que não `groupBy("payment_id").agg(F.max("lsn"))`?** Isso te dá o
# MAGIC maior `lsn` de cada pagamento, mas **perde todas as outras colunas** —
# MAGIC você precisaria de um segundo join contra a tabela original pra recuperar
# MAGIC `status`, `amount`, etc. A window function te dá a linha inteira de graça.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

window_spec = Window.partitionBy("payment_id").orderBy(F.col("lsn").desc())

ranked_df = bronze_df.withColumn("rn", F.row_number().over(window_spec))
deduped_df = ranked_df.filter(F.col("rn") == 1).drop("rn")

distinct_events = bronze_df.dropDuplicates(["payment_id", "lsn"]).count()
deduped_count = deduped_df.count()

print(f"Bronze total:                         {bronze_count} linhas")
print(f"Eventos distintos (payment_id, lsn):   {distinct_events} linhas  (duplicatas exatas removidas)")
print(f"Silver após 'ficar com o mais recente': {deduped_count} linhas  (uma por payment_id)")

display(deduped_df.orderBy("payment_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E se o último evento for um DELETE?
# MAGIC
# MAGIC A window function acima decide **qual é o estado mais recente**, mas não
# MAGIC decide **se esse pagamento ainda deve existir na silver**. Se o `op` do
# MAGIC evento vencedor (maior `lsn`) for `DELETE`, o pagamento foi apagado na
# MAGIC origem — e como esta silver representa **só o estado atual** (sem coluna
# MAGIC `is_deleted`, diferente do change log do projeto real, que mantém a linha
# MAGIC e marca a data da morte), o comportamento correto é **o pagamento
# MAGIC simplesmente não aparecer** na tabela.
# MAGIC
# MAGIC Importante: esse filtro tem que vir **depois** da window function, nunca
# MAGIC antes. Se filtrássemos `op != 'DELETE'` direto na bronze, um `DELETE` que
# MAGIC não fosse o evento de maior `lsn` (ex.: alguém apagou e depois reverteu com
# MAGIC um novo `INSERT`) sumiria da disputa por engano — a regra certa é
# MAGIC "decida primeiro quem é o evento mais recente, decida depois se ele
# MAGIC sobrevive".
# MAGIC
# MAGIC Os dados gerados no `01_bronze` não têm nenhum `DELETE`, então a contagem
# MAGIC abaixo não muda nada agora — mas o `03_merge` vai injetar um `DELETE` de
# MAGIC verdade no lote novo. Ter essa regra aqui **também** (e não só no
# MAGIC `MERGE INTO` do próximo notebook) garante que reprocessar a bronze inteira
# MAGIC do zero e aplicar o merge incrementalmente cheguem ao mesmo resultado.

# COMMAND ----------

before_delete_filter = deduped_df.count()
deduped_df = deduped_df.filter(F.col("op") != "DELETE")
removed_by_delete = before_delete_filter - deduped_df.count()

print(f"Pagamentos cujo evento mais recente é DELETE (removidos da silver): {removed_by_delete}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tipando as colunas
# MAGIC
# MAGIC Até aqui, `payment_id`, `amount` e `updated_at` ainda são `string` — herdados
# MAGIC da bronze. Agora aplicamos o contrato de tipos (o mesmo que
# MAGIC `contracts/tables.py` define para `PaymentRow` no projeto real):
# MAGIC
# MAGIC | coluna | tipo bronze | tipo silver | por quê |
# MAGIC |---|---|---|---|
# MAGIC | `payment_id` | string | `bigint` | é a chave primária; comparações e joins numéricos devem ser numéricos |
# MAGIC | `amount` | string | `decimal(14,2)` | dinheiro nunca é `double` (erro de ponto flutuante); `decimal(14,2)` é a mesma precisão do contrato Pydantic |
# MAGIC | `updated_at` | string | `timestamp` | precisa virar um tipo de data de verdade para o `03_merge` e para consultas de tempo |
# MAGIC | `lsn`, `op`, `status` | — | mantidos | `lsn` já era `bigint` desde a bronze; `op`/`status` continuam texto, são categóricos por natureza |
# MAGIC
# MAGIC ### Validando o cast (em vez de confiar cegamente nele)
# MAGIC Um `.cast()` que falha no Spark **não lança erro — devolve `null`**. Se a
# MAGIC origem mandasse um valor impossível de tipar (ex.: `amount = "abc"`), o
# MAGIC `.cast(DecimalType(14,2))` viraria `null` silenciosamente, e o pipeline
# MAGIC seguiria em frente com um dado errado. Por isso conferimos explicitamente:
# MAGIC nenhum valor não-nulo na bronze pode virar nulo depois do cast. É a mesma
# MAGIC ideia do contrato checado no `Relation message` no projeto real (ADR 0011) —
# MAGIC só que aqui, em vez de checar no schema da origem, checamos no resultado do
# MAGIC cast.

# COMMAND ----------

from pyspark.sql.types import LongType, DecimalType

typed_df = (
    deduped_df
    .withColumn("payment_id", F.col("payment_id").cast(LongType()))
    .withColumn("amount", F.col("amount").cast(DecimalType(14, 2)))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
)

cast_failures = typed_df.select(
    F.sum(F.when(F.col("payment_id").isNull(), 1).otherwise(0)).alias("payment_id_failures"),
    F.sum(F.when(F.col("amount").isNull(), 1).otherwise(0)).alias("amount_failures"),
    F.sum(F.when(F.col("updated_at").isNull(), 1).otherwise(0)).alias("updated_at_failures"),
).first()

print(f"Falhas de cast — payment_id: {cast_failures['payment_id_failures']}, "
      f"amount: {cast_failures['amount_failures']}, "
      f"updated_at: {cast_failures['updated_at_failures']}")

if any(cast_failures[c] > 0 for c in cast_failures.asDict()):
    raise ValueError("Contrato violado: existe valor na bronze que não foi possível tipar. Abortando gravação da silver.")

print("Contrato de tipos OK — nenhum valor virou nulo por causa do cast.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravando a silver

# COMMAND ----------

silver_df = (
    typed_df
    .select("payment_id", "lsn", "op", "amount", "status", "updated_at")
    .withColumn("_silver_processed_at", F.current_timestamp())
)

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Gravado em {silver_table}")
spark.table(silver_table).printSchema()
display(spark.table(silver_table).orderBy("payment_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes / depois

# COMMAND ----------

summary_df = spark.createDataFrame(
    [
        ("bronze (bruto, com duplicatas e fora de ordem)", bronze_count),
        ("silver (1 linha por payment_id, tipada)", spark.table(silver_table).count()),
    ],
    ["etapa", "linhas"],
)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulando para a entrevista
# MAGIC
# MAGIC | Pergunta que podem fazer | Resposta curta |
# MAGIC |---|---|
# MAGIC | Por que usar `row_number` e não `rank`/`dense_rank`? | `rank`/`dense_rank` deixam **empates** (dois `rn=1` se o `lsn` se repetir), e um `filter(rn == 1)` deixaria duas linhas por pagamento. `row_number` garante exatamente uma linha vencedora por grupo, mesmo com empate no valor de ordenação. |
# MAGIC | Por que ordenar por `lsn` e não por `updated_at`? | `updated_at` vem da origem e pode repetir, sofrer clock skew, ou faltar. `lsn` é atribuído pelo próprio WAL e só cresce — é a fonte de verdade sobre "o que aconteceu por último". |
# MAGIC | O que acontece se o cast falhar? | O notebook levanta uma exceção **antes** de gravar a silver, em vez de deixar um `null` silencioso entrar na tabela. |
# MAGIC | Por que a silver aqui é "estado atual" e não "change log completo"? | Decisão de escopo para este demo — o `03_merge` precisa de uma tabela de estado atual como alvo do `MERGE INTO`. O projeto real mantém o change log completo porque a gold dele reconstrói SCD Type 2. |
# MAGIC | `amount` como `decimal(14,2)`, por que não `double`? | `double` é ponto flutuante binário — `0.1 + 0.2` já não bate exato. Dinheiro exige aritmética decimal exata. |
# MAGIC | O que acontece se o último evento de um pagamento for `DELETE`? | A window function ainda escolhe esse evento como vencedor (maior `lsn`), mas ele é filtrado logo em seguida (`op != 'DELETE'`) antes de ir para a silver — o pagamento some da tabela de estado atual. |
# MAGIC
# MAGIC **Próximo passo:** notebook `03_merge` — simular um novo lote de eventos de
# MAGIC CDC e aplicar `MERGE INTO` na silver (upsert incremental).
