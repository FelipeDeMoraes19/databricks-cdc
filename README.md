# databricks-cdc

Pipeline de dados end-to-end no Databricks simulando **Change Data Capture
(CDC)** de uma tabela `payments`, construído em Python/PySpark seguindo a
arquitetura medallion (bronze → silver → gold) e práticas de engenharia de
dados: Unity Catalog, Auto Loader, testes automatizados e orquestração via
Databricks Asset Bundles.

> Projeto de portfólio, desenvolvido e implantado no **Databricks Free
> Edition** (compute serverless).

## Stack

- **Databricks** — Unity Catalog, Volumes, Auto Loader, Delta Lake, Jobs, Asset Bundles
- **Apache Spark / PySpark** — Structured Streaming, `foreachBatch`, `MERGE INTO`
- **Python** — módulos de transformação testáveis em `src/`, pytest, GitHub Actions

## Status

Projeto em construção, por fases:

- [x] Fase 1 — Estrutura do repositório
- [ ] Fase 2 — Ingestão (gerador de eventos CDC + Auto Loader)
- [ ] Fase 3 — Silver (dedup por LSN, `MERGE` com guard, quarentena)
- [ ] Fase 4 — Gold (dimensão SCD Tipo 2 + métricas diárias)
- [ ] Fase 5 — Orquestração (Job encadeado via Databricks Asset Bundle)
- [ ] Fase 6 — Governança (column mask no Unity Catalog)
- [ ] Fase 7 — Qualidade de código (pytest + GitHub Actions)
- [ ] Fase 8 — Documentação final (diagrama, decisões técnicas, resultados)

## Estrutura

```
src/cdc_demo/        código de transformação (bronze/silver/gold), testável fora do Databricks
notebooks/           notebooks finos que orquestram os módulos de src/
notebooks/archive/   primeira versão (batch, exploratória) do projeto, preservada como histórico
tests/               testes pytest das funções de src/
resources/           definições de Job do Databricks Asset Bundle
```

Diagrama de arquitetura, decisões técnicas, instruções de execução e
resultados serão documentados na Fase 8, ao final do projeto.
