# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Merge — aplicando um lote incremental de CDC na silver
# MAGIC
# MAGIC **Objetivo deste notebook:** simular a chegada de um **novo lote** de
# MAGIC eventos de CDC (pagamentos novos + atualizações de pagamentos existentes +
# MAGIC um `DELETE`) e aplicá-lo na `silver_payments_current` com `MERGE INTO`, em
# MAGIC vez de reprocessar a bronze inteira de novo.
# MAGIC
# MAGIC ### Por que `MERGE INTO` e não reescrever a tabela toda?
# MAGIC O `02_silver` faz um **full reload**: lê toda a bronze, recalcula tudo,
# MAGIC sobrescreve a silver inteira (`mode("overwrite")`). Isso é simples, mas não
# MAGIC escala — em produção a bronze cresce sem parar (o próprio projeto real
# MAGIC documenta isso: "bronze grows without bound"), e reprocessar tudo a cada
# MAGIC novo evento fica caro rápido.
# MAGIC
# MAGIC `MERGE INTO` (o "upsert" do Delta Lake) resolve isso: você entrega só o
# MAGIC **lote novo** e o Delta decide, linha por linha, se aquilo é uma
# MAGIC atualização de algo que já existe, uma inserção de algo novo, ou (no nosso
# MAGIC caso) uma remoção — tudo em uma única transação atômica, sem reescrever a
# MAGIC tabela inteira.

# COMMAND ----------

import random
from decimal import Decimal
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DecimalType, TimestampType,
)

random.seed(7)

catalog = "workspace"
schema = "payments_cdc_demo"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

silver_table = f"{catalog}.{schema}.silver_payments_current"

before_count = spark.table(silver_table).count()
max_lsn_before = spark.table(silver_table).agg(F.max("lsn").alias("m")).first()["m"]

print(f"Silver antes do merge: {before_count} linhas")
print(f"Maior lsn visto até agora: {max_lsn_before}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montando o lote novo
# MAGIC
# MAGIC O lote vai ter três tipos de evento, todos com `lsn` maior que qualquer
# MAGIC coisa já na silver (o WAL só cresce):
# MAGIC
# MAGIC 1. **8 pagamentos novos** (`payment_id` 41–48) — devem ser **inseridos**.
# MAGIC 2. **6 pagamentos existentes** recebem um novo evento cada — devem ser
# MAGIC    **atualizados** (novo status `SETTLED`).
# MAGIC 3. **1 pagamento existente (`payment_id = 30`) recebe DOIS eventos no
# MAGIC    mesmo lote**: primeiro um `UPDATE` (`REFUNDED`), depois um `DELETE`
# MAGIC    (`lsn` maior) — de propósito, para provar por que o lote também precisa
# MAGIC    passar pela mesma dedup por `lsn` que a bronze passou no `02_silver`.

# COMMAND ----------

NEW_PAYMENT_IDS = list(range(41, 49))          # 8 pagamentos novos
UPDATE_PAYMENT_IDS = [2, 5, 10, 15, 20, 25]    # 6 pagamentos existentes, 1 evento cada
DOUBLE_EVENT_PAYMENT_ID = 30                   # recebe update + delete no mesmo lote

lsn_counter = max_lsn_before + 10
batch_base_ts = datetime(2024, 1, 16, 9, 0, 0)


def next_lsn():
    global lsn_counter
    lsn_counter += random.randint(1, 5)
    return lsn_counter


batch_events = []

for pid in NEW_PAYMENT_IDS:
    batch_events.append(
        {
            "payment_id": pid,
            "lsn": next_lsn(),
            "op": "INSERT",
            "amount": Decimal(random.randrange(1000, 200000)) / 100,
            "status": "PENDING",
            "updated_at": batch_base_ts + timedelta(minutes=pid),
        }
    )

for pid in UPDATE_PAYMENT_IDS:
    current_row = spark.table(silver_table).filter(F.col("payment_id") == pid).first()
    batch_events.append(
        {
            "payment_id": pid,
            "lsn": next_lsn(),
            "op": "UPDATE",
            "amount": current_row["amount"],
            "status": "SETTLED",
            "updated_at": batch_base_ts + timedelta(minutes=pid, hours=1),
        }
    )

double_row = spark.table(silver_table).filter(F.col("payment_id") == DOUBLE_EVENT_PAYMENT_ID).first()
batch_events.append(
    {
        "payment_id": DOUBLE_EVENT_PAYMENT_ID,
        "lsn": next_lsn(),
        "op": "UPDATE",
        "amount": double_row["amount"],
        "status": "REFUNDED",
        "updated_at": batch_base_ts + timedelta(hours=2),
    }
)
batch_events.append(
    {
        "payment_id": DOUBLE_EVENT_PAYMENT_ID,
        "lsn": next_lsn(),  # maior lsn -> este é o evento que deveria vencer
        "op": "DELETE",
        "amount": double_row["amount"],
        "status": "REFUNDED",
        "updated_at": batch_base_ts + timedelta(hours=2, minutes=30),
    }
)

batch_schema = StructType(
    [
        StructField("payment_id", LongType(), False),
        StructField("lsn", LongType(), False),
        StructField("op", StringType(), False),
        StructField("amount", DecimalType(14, 2), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

batch_raw_df = spark.createDataFrame(batch_events, schema=batch_schema)
print(f"Eventos no lote novo (bruto, antes de deduplicar): {batch_raw_df.count()}")
display(batch_raw_df.orderBy("payment_id", "lsn"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Por que não dá pra fazer `MERGE` direto com o lote bruto
# MAGIC
# MAGIC O `payment_id = 30` aparece **duas vezes** na fonte (`UPDATE` e depois
# MAGIC `DELETE`). O Delta exige que, para cada linha do alvo, **no máximo uma
# MAGIC linha da fonte** dê match na condição `ON` — porque senão ele não sabe
# MAGIC qual das duas regras (`UPDATE` ou `DELETE`) aplicar primeiro. Vamos deixar
# MAGIC isso falhar de propósito, para ver o erro real do Delta.

# COMMAND ----------

batch_raw_df.createOrReplaceTempView("batch_events_raw")

try:
    spark.sql(
        f"""
        MERGE INTO {silver_table} AS target
        USING batch_events_raw AS source
        ON target.payment_id = source.payment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("Merge concluído sem erro (não deveria acontecer com este lote).")
except Exception as e:
    print("Erro esperado, porque payment_id=30 casa com duas linhas da fonte:\n")
    print(str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corrigindo: deduplicar o lote antes do merge
# MAGIC
# MAGIC Mesma window function do `02_silver`, aplicada agora ao **lote**, não à
# MAGIC bronze inteira: `partitionBy("payment_id")`, `orderBy(lsn desc)`,
# MAGIC `row_number() == 1`. Isso garante uma única linha por `payment_id` na
# MAGIC fonte do merge — e, como o `DELETE` tem o maior `lsn` para o
# MAGIC `payment_id = 30`, ele é o que sobrevive, exatamente como deveria.

# COMMAND ----------

batch_window = Window.partitionBy("payment_id").orderBy(F.col("lsn").desc())

batch_df = (
    batch_raw_df.withColumn("rn", F.row_number().over(batch_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"Eventos no lote após dedup (1 por payment_id): {batch_df.count()}")
display(batch_df.orderBy("payment_id"))

batch_df.createOrReplaceTempView("batch_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## O `MERGE INTO`, linha por linha
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO workspace.payments_cdc_demo.silver_payments_current AS target
# MAGIC USING batch_events AS source
# MAGIC ON target.payment_id = source.payment_id
# MAGIC ```
# MAGIC - **`MERGE INTO ... AS target`** — a tabela Delta que vai ser modificada
# MAGIC   *in place*, dentro de uma única transação atômica (ou tudo aplica, ou
# MAGIC   nada aplica).
# MAGIC - **`USING batch_events AS source`** — a tabela/view com o lote novo, já
# MAGIC   deduplicado (uma linha por chave).
# MAGIC - **`ON target.payment_id = source.payment_id`** — a condição de match: é
# MAGIC   o `payment_id` que liga uma linha da fonte a uma linha (no máximo) do
# MAGIC   alvo.
# MAGIC
# MAGIC ```sql
# MAGIC WHEN MATCHED AND source.op = 'DELETE' AND source.lsn > target.lsn THEN
# MAGIC   DELETE
# MAGIC ```
# MAGIC - Se já existe uma linha no alvo para esse `payment_id` **e** o evento que
# MAGIC   chegou é um `DELETE` **e** esse `DELETE` é mais recente que o que já
# MAGIC   está gravado (`source.lsn > target.lsn`), a linha é removida da silver.
# MAGIC   Essa é a **primeira** cláusula `WHEN MATCHED` — o Delta avalia as
# MAGIC   cláusulas na ordem em que aparecem e executa a **primeira** cuja
# MAGIC   condição extra (`AND ...`) seja verdadeira. Por isso o `DELETE` precisa
# MAGIC   vir antes do `UPDATE` genérico.
# MAGIC - **O guard de `lsn` aqui não é opcional.** Sem ele, um `DELETE`
# MAGIC   **atrasado** — que chega fora de ordem com um `lsn` menor que o que já
# MAGIC   está na silver — apagaria uma linha que já foi atualizada por algo mais
# MAGIC   novo depois daquele delete "ter acontecido" na origem. É o mesmo
# MAGIC   raciocínio da cláusula de `UPDATE` logo abaixo, aplicado ao `DELETE`:
# MAGIC   toda escrita no merge — insert, update **ou** delete — só deve valer se
# MAGIC   for mais recente que o estado atual.
# MAGIC
# MAGIC ```sql
# MAGIC WHEN MATCHED AND source.lsn > target.lsn THEN
# MAGIC   UPDATE SET
# MAGIC     target.lsn = source.lsn,
# MAGIC     target.op = source.op,
# MAGIC     target.amount = source.amount,
# MAGIC     target.status = source.status,
# MAGIC     target.updated_at = source.updated_at,
# MAGIC     target._silver_processed_at = current_timestamp()
# MAGIC ```
# MAGIC - Só chega aqui se a cláusula do `DELETE` não disparou. Atualiza os campos
# MAGIC   **coluna a coluna** (em vez de `UPDATE SET *`) porque queremos também
# MAGIC   carimbar `_silver_processed_at`, uma coluna que não existe na fonte.
# MAGIC - **`source.lsn > target.lsn`** é a parte mais importante desta cláusula:
# MAGIC   sem essa guarda, o `MERGE INTO` aplicaria cegamente qualquer evento do
# MAGIC   lote, mesmo um **atrasado** (`lsn` menor que o que já está na silver).
# MAGIC   Com a guarda, um evento antigo que chega fora de ordem é simplesmente
# MAGIC   ignorado — o merge fica tão seguro contra desordem quanto a window
# MAGIC   function do `02_silver` foi contra a bronze.
# MAGIC
# MAGIC ```sql
# MAGIC WHEN NOT MATCHED AND source.op != 'DELETE' THEN
# MAGIC   INSERT (payment_id, lsn, op, amount, status, updated_at, _silver_processed_at)
# MAGIC   VALUES (source.payment_id, source.lsn, source.op, source.amount, source.status, source.updated_at, current_timestamp())
# MAGIC ```
# MAGIC - Se **não existe** linha no alvo para esse `payment_id`, é um pagamento
# MAGIC   novo: insere. A guarda `AND source.op != 'DELETE'` evita inserir uma
# MAGIC   linha "fantasma" se, por algum motivo, chegar um `DELETE` para um
# MAGIC   `payment_id` que nunca existiu (ou já foi apagado) na silver — não faz
# MAGIC   sentido inserir só para representar a ausência de algo.
# MAGIC
# MAGIC Se nenhuma cláusula `WHEN` casar para uma linha da fonte (ex.: um `UPDATE`
# MAGIC atrasado que não passa em `source.lsn > target.lsn`), **nada acontece com
# MAGIC ela** — não é erro, é só ignorada.

# COMMAND ----------

merge_sql = f"""
MERGE INTO {silver_table} AS target
USING batch_events AS source
ON target.payment_id = source.payment_id
WHEN MATCHED AND source.op = 'DELETE' AND source.lsn > target.lsn THEN
  DELETE
WHEN MATCHED AND source.lsn > target.lsn THEN
  UPDATE SET
    target.lsn = source.lsn,
    target.op = source.op,
    target.amount = source.amount,
    target.status = source.status,
    target.updated_at = source.updated_at,
    target._silver_processed_at = current_timestamp()
WHEN NOT MATCHED AND source.op != 'DELETE' THEN
  INSERT (payment_id, lsn, op, amount, status, updated_at, _silver_processed_at)
  VALUES (source.payment_id, source.lsn, source.op, source.amount, source.status, source.updated_at, current_timestamp())
"""

merge_result = spark.sql(merge_sql)
display(merge_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Antes / depois

# COMMAND ----------

after_count = spark.table(silver_table).count()
expected_count = before_count + len(NEW_PAYMENT_IDS) - 1  # +8 novos, -1 deletado

print(f"Silver antes do merge:  {before_count} linhas")
print(f"Silver depois do merge: {after_count} linhas")
print(f"Esperado: {before_count} + {len(NEW_PAYMENT_IDS)} novos - 1 deletado = {expected_count}")
assert after_count == expected_count, "Contagem não bate com o esperado!"

summary_df = spark.createDataFrame(
    [
        ("antes do merge", before_count),
        ("depois do merge", after_count),
    ],
    ["etapa", "linhas"],
)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferindo cada tipo de mudança

# COMMAND ----------

print("### Pagamentos novos (devem existir, status PENDING) ###")
display(
    spark.table(silver_table)
    .filter(F.col("payment_id").isin(NEW_PAYMENT_IDS))
    .orderBy("payment_id")
)

print("### Pagamentos atualizados (devem estar SETTLED) ###")
display(
    spark.table(silver_table)
    .filter(F.col("payment_id").isin(UPDATE_PAYMENT_IDS))
    .orderBy("payment_id")
)

print(f"### payment_id = {DOUBLE_EVENT_PAYMENT_ID} (deve ter sumido — foi deletado) ###")
display(
    spark.table(silver_table).filter(F.col("payment_id") == DOUBLE_EVENT_PAYMENT_ID)
)
deleted_still_present = (
    spark.table(silver_table).filter(F.col("payment_id") == DOUBLE_EVENT_PAYMENT_ID).count()
)
print(f"Linhas encontradas para payment_id={DOUBLE_EVENT_PAYMENT_ID}: {deleted_still_present} (esperado: 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bônus: provando que o guard do `DELETE` funciona
# MAGIC
# MAGIC A correção acima (`AND source.lsn > target.lsn` no `DELETE`) só faz
# MAGIC diferença para um cenário específico: um `DELETE` **atrasado**, com `lsn`
# MAGIC menor que o que já está gravado na silver. Vamos simular exatamente isso
# MAGIC contra um pagamento que **não** foi tocado por nada até agora
# MAGIC (`payment_id = 3`), para provar que a linha sobrevive.

# COMMAND ----------

UNTOUCHED_PAYMENT_ID = 3

untouched_row = spark.table(silver_table).filter(F.col("payment_id") == UNTOUCHED_PAYMENT_ID).first()
print(f"Estado atual de payment_id={UNTOUCHED_PAYMENT_ID}: lsn={untouched_row['lsn']}, status={untouched_row['status']}")

stale_delete_df = spark.createDataFrame(
    [
        {
            "payment_id": UNTOUCHED_PAYMENT_ID,
            "lsn": untouched_row["lsn"] - 1,  # menor que o lsn já gravado -> atrasado
            "op": "DELETE",
            "amount": untouched_row["amount"],
            "status": untouched_row["status"],
            "updated_at": untouched_row["updated_at"],
        }
    ],
    schema=batch_schema,
)
stale_delete_df.createOrReplaceTempView("batch_events")

spark.sql(merge_sql)

after_stale_delete = spark.table(silver_table).filter(F.col("payment_id") == UNTOUCHED_PAYMENT_ID).count()
print(f"Linhas para payment_id={UNTOUCHED_PAYMENT_ID} após o DELETE atrasado: {after_stale_delete} (esperado: 1 — o guard bloqueou o delete)")
assert after_stale_delete == 1, "O DELETE atrasado não deveria ter apagado a linha!"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulando para a entrevista
# MAGIC
# MAGIC | Pergunta que podem fazer | Resposta curta |
# MAGIC |---|---|
# MAGIC | Por que `MERGE INTO` em vez de reescrever a tabela? | É um upsert atômico incremental — só processa o lote novo, não a tabela inteira. Full reload (`02_silver`) não escala conforme a bronze cresce. |
# MAGIC | Por que a ordem das cláusulas `WHEN MATCHED` importa? | O Delta executa a **primeira** cláusula cuja condição bate. Se o `UPDATE` genérico viesse antes do `DELETE`, um evento de delete nunca seria tratado como delete. |
# MAGIC | Por que `source.lsn > target.lsn` na cláusula de update? | Proteção contra evento atrasado/fora de ordem: sem essa guarda, um `UPDATE` velho reaplicado no lote sobrescreveria um estado mais novo já gravado. |
# MAGIC | E por que a mesma guarda também está na cláusula de `DELETE`? | Sem ela, um `DELETE` atrasado (lsn menor que o já gravado) apagaria uma linha que já foi atualizada por algo mais novo — o bônus deste notebook prova isso na prática com `payment_id=3`. |
# MAGIC | Por que o lote precisou ser deduplicado antes do merge? | O Delta não aceita mais de uma linha da fonte casando com a mesma linha do alvo — vimos o erro real ao tentar sem dedup. A mesma window function do `02_silver` resolve, agora aplicada ao lote. |
# MAGIC | Por que `WHEN NOT MATCHED AND source.op != 'DELETE'`? | Evita inserir uma linha só para representar um `DELETE` de um `payment_id` que nunca existiu (ou já foi apagado) na silver. |
# MAGIC | O que `MERGE INTO` retorna? | Um DataFrame com métricas: quantas linhas foram inseridas, atualizadas e deletadas na operação — útil para observabilidade/auditoria do próprio pipeline. |
# MAGIC
# MAGIC **Próximo passo:** notebook `04_delta_features` — `DESCRIBE HISTORY`, time
# MAGIC travel (`VERSION AS OF`) e `OPTIMIZE`, usando justamente as versões que a
# MAGIC silver acumulou nos merges deste notebook.
