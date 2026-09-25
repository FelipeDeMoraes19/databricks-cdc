# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — captura crua de eventos de CDC
# MAGIC
# MAGIC **Objetivo deste notebook:** simular a chegada de eventos de CDC (Change Data
# MAGIC Capture) vindos de uma tabela `payments` no Postgres, e gravá-los **sem
# MAGIC nenhuma limpeza** em uma tabela Delta bronze.
# MAGIC
# MAGIC ### Por que "sem nenhuma limpeza"?
# MAGIC Na camada bronze, a regra de ouro do medallion architecture é: **grave os
# MAGIC dados o mais parecido possível com a origem.** Isso significa:
# MAGIC - **Duplicatas são esperadas.** Um consumidor de replication slot (Debezium,
# MAGIC   um consumidor `pgoutput` próprio, etc.) normalmente garante *at-least-once
# MAGIC   delivery* — ou seja, o mesmo evento pode chegar mais de uma vez se o
# MAGIC   consumidor reiniciar antes de confirmar o offset. É mais barato aceitar
# MAGIC   duplicata na bronze do que arriscar perder um evento.
# MAGIC - **Fora de ordem é esperado.** Múltiplos arquivos/batches podem chegar em
# MAGIC   paralelo, ou um retry pode reenviar um evento antigo depois de um novo.
# MAGIC   Bronze não tenta resolver isso — quem resolve é a silver (próximo
# MAGIC   notebook), usando o `lsn` (Log Sequence Number).
# MAGIC - **Colunas de negócio ficam como texto.** `amount`, `status` e `updated_at`
# MAGIC   são gravados como `string`, exatamente como vieram do WAL (Write-Ahead
# MAGIC   Log) do Postgres antes de qualquer conversão de tipo. Só a `lsn` (uma
# MAGIC   coluna de controle técnico, não de negócio) já nasce numérica, porque é
# MAGIC   ela que vai ordenar os eventos daqui a pouco.
# MAGIC
# MAGIC Essa decisão de design não é um exagero acadêmico: é exatamente o que o
# MAGIC projeto real (`payments-cdc-observability`) faz — ver ADR 0002 (dedup por
# MAGIC `(key, lsn)`, não por `updated_at`) e ADR 0013 (bronze em Parquet, valores
# MAGIC como texto). Se tipássemos direto na bronze e a origem mudasse o tipo de uma
# MAGIC coluna, o job de ingestão quebraria. Guardando como texto, a ingestão nunca
# MAGIC falha por causa de tipo — só a silver, que é onde queremos que essa
# MAGIC validação aconteça.
# MAGIC
# MAGIC ### O que é `lsn`?
# MAGIC No Postgres, o **LSN (Log Sequence Number)** é a posição de um evento dentro
# MAGIC do write-ahead log — um número que só cresce. Cada mudança (INSERT, UPDATE,
# MAGIC DELETE) recebe um LSN maior que a anterior. É a fonte de verdade de "o que
# MAGIC aconteceu por último", muito mais confiável que `updated_at`, porque
# MAGIC `updated_at` pode ter clock skew, pode ser igual em dois eventos, ou pode
# MAGIC nem existir em um DELETE.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Definindo onde os dados vão morar. No Databricks Free Edition, o Unity
# MAGIC Catalog já vem habilitado por padrão, então criamos um **schema** dedicado
# MAGIC para este projeto dentro do catálogo padrão do workspace.

# COMMAND ----------

catalog = "workspace"     # catálogo padrão do Databricks Free Edition
schema = "payments_cdc_demo"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

bronze_table = f"{catalog}.{schema}.bronze_payments_cdc"
print(f"Tabela bronze de destino: {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerando eventos de CDC sintéticos
# MAGIC
# MAGIC A ideia: para cada `payment_id`, simular o ciclo de vida real de um
# MAGIC pagamento —
# MAGIC
# MAGIC `INSERT (PENDING) → UPDATE (AUTHORIZED) → UPDATE (CAPTURED)` e, para uma
# MAGIC fração dos pagamentos, `UPDATE (FAILED)` ou `UPDATE (REFUNDED)`.
# MAGIC
# MAGIC Cada evento recebe um `lsn` **globalmente crescente** (como seria no WAL
# MAGIC real — um único log por banco, compartilhado por todas as tabelas e
# MAGIC transações). Depois de gerar tudo em ordem, nós **embaralhamos** a lista e
# MAGIC **duplicamos** alguns eventos de propósito, para simular exatamente os dois
# MAGIC problemas que a bronze precisa tolerar: entrega fora de ordem e duplicata.

# COMMAND ----------

import random
from datetime import datetime, timedelta
from decimal import Decimal

random.seed(42)  # reprodutível — importante para explicar o resultado na entrevista

NUM_PAYMENTS = 40
BASE_TS = datetime(2024, 1, 15, 8, 0, 0)

STATUS_FLOW_OPTIONS = [
    ["PENDING", "AUTHORIZED", "CAPTURED"],
    ["PENDING", "AUTHORIZED", "FAILED"],
    ["PENDING", "AUTHORIZED", "CAPTURED", "REFUNDED"],
    ["PENDING", "CAPTURED"],  # fluxo mais curto, sem etapa de autorização separada
]

events = []
lsn_counter = 1000  # LSN "real" seria um offset de byte no WAL; aqui é só um contador crescente

for payment_id in range(1, NUM_PAYMENTS + 1):
    flow = random.choice(STATUS_FLOW_OPTIONS)
    amount = Decimal(random.randrange(500, 500000)) / 100  # entre 5.00 e 5000.00
    current_ts = BASE_TS + timedelta(minutes=payment_id * 3)

    for step_index, status in enumerate(flow):
        op = "INSERT" if step_index == 0 else "UPDATE"
        current_ts = current_ts + timedelta(minutes=random.randint(1, 45))
        events.append(
            {
                "payment_id": str(payment_id),
                "lsn": lsn_counter,
                "op": op,
                "amount": str(amount),
                "status": status,
                "updated_at": current_ts.isoformat(),
            }
        )
        lsn_counter += random.randint(1, 5)  # outras tabelas também escrevem no WAL entre esses eventos

print(f"Gerados {len(events)} eventos 'limpos', em ordem, para {NUM_PAYMENTS} pagamentos.")
events[:6]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Injetando duplicatas e desordem
# MAGIC
# MAGIC Agora que temos a sequência "ideal" de eventos, vamos estragá-la de
# MAGIC propósito, do jeito que ela chegaria de verdade em um pipeline distribuído:
# MAGIC
# MAGIC 1. **Duplicatas** — escolhemos ~15% dos eventos e os repetimos (simula
# MAGIC    reprocessamento após reinício do consumidor antes do commit do offset).
# MAGIC 2. **Fora de ordem** — embaralhamos a lista inteira antes de escrever. Na
# MAGIC    bronze isso não é um problema: cada arquivo/partição só precisa registrar
# MAGIC    o evento com seu LSN; a ordenação correta é responsabilidade de quem lê
# MAGIC    depois (a silver).

# COMMAND ----------

duplicated = random.sample(events, k=int(len(events) * 0.15))
all_events = events + duplicated
random.shuffle(all_events)

print(f"Total após duplicatas: {len(all_events)} eventos ({len(duplicated)} duplicados)")
print("Exemplo de evento duplicado:", duplicated[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando o DataFrame com schema explícito
# MAGIC
# MAGIC Definir o schema manualmente (em vez de deixar o Spark inferir) é uma boa
# MAGIC prática em bronze: garante que a tabela sempre tenha o mesmo formato,
# MAGIC mesmo que o lote gerado varie. Repare que `amount`, `status` e `updated_at`
# MAGIC são `StringType` — **de propósito**, como explicado acima.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import functions as F

bronze_schema = StructType(
    [
        StructField("payment_id", StringType(), False),
        StructField("lsn", LongType(), False),
        StructField("op", StringType(), False),
        StructField("amount", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)

bronze_df = spark.createDataFrame(all_events, schema=bronze_schema)

# Metadados de ingestão: quando este evento efetivamente chegou na bronze.
# Note que isso é diferente de `updated_at` (quando o evento aconteceu na origem) —
# essa distinção entre "hora do evento" e "hora da ingestão" é o que permite
# detectar dados atrasados (chaos-late no projeto original).
bronze_df = bronze_df.withColumn("_ingested_at", F.current_timestamp())

display(bronze_df.orderBy("payment_id", "lsn"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferindo que as duplicatas e a desordem estão lá
# MAGIC Antes de gravar, vamos provar visualmente os dois problemas que a silver
# MAGIC vai precisar resolver.

# COMMAND ----------

print("### Duplicatas (mesmo payment_id + lsn aparecendo mais de uma vez) ###")
display(
    bronze_df.groupBy("payment_id", "lsn")
    .count()
    .filter("count > 1")
    .orderBy("payment_id", "lsn")
)

print("### Exemplo de desordem: LSNs de um mesmo payment_id fora de sequência no arquivo ###")
display(
    bronze_df.filter(F.col("payment_id") == "1").select("payment_id", "lsn", "op", "status")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravando como tabela Delta bronze
# MAGIC
# MAGIC `mode("overwrite")` aqui é só para tornar este notebook re-executável
# MAGIC durante os testes/demo. Em um pipeline real de CDC, a bronze normalmente é
# MAGIC **append-only** (cada rodada de ingestão só adiciona os eventos novos que
# MAGIC chegaram, nunca reescreve o que já está lá).

# COMMAND ----------

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Gravado em {bronze_table}")
display(spark.table(bronze_table).orderBy("payment_id", "lsn"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recapitulando para a entrevista
# MAGIC
# MAGIC | Pergunta que podem fazer | Resposta curta |
# MAGIC |---|---|
# MAGIC | Por que bronze guarda `amount`/`status` como string? | Para nunca falhar a ingestão por causa de mudança de tipo na origem; tipagem é responsabilidade da silver, com um contrato explícito. |
# MAGIC | Por que existem duplicatas de propósito? | Porque um consumidor real de CDC é *at-least-once*: se ele reinicia antes de confirmar o que já processou, reprocessa e duplica. É mais seguro duplicar do que perder um evento. |
# MAGIC | Por que os eventos estão embaralhados? | Simula chegada fora de ordem (múltiplos arquivos/partições, retries). A bronze não resolve isso — só registra. |
# MAGIC | O que é `lsn` e por que não usar `updated_at` para ordenar? | LSN é a posição no write-ahead log, estritamente crescente e definida pelo próprio banco. `updated_at` pode repetir, sofrer clock skew, ou não existir em alguns eventos — LSN nunca. |
# MAGIC | Por que `mode("overwrite")` aqui e não `append`? | Só para o notebook ser re-executável na demo. Em produção, a bronze de CDC é append-only. |
# MAGIC
# MAGIC **Próximo passo:** notebook `02_silver` — deduplicar por `(payment_id, lsn)`
# MAGIC com uma window function, tipar as colunas, e gravar a silver.
