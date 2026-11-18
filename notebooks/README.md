# notebooks/ — Análisis, modelado y resultados

Notebooks de Jupyter con el análisis del proyecto. Leen las copias locales de `data/` (no necesitan el clúster, salvo `01_ingesta` si se activa la descarga) y guardan las figuras en `informe/`. **Las rutas son relativas a esta carpeta**, así que se ejecutan desde `notebooks/`:

```bash
cd notebooks
jupyter nbconvert --to notebook --execute <notebook>.ipynb --ExecutePreprocessor.timeout=-1 --output-dir /tmp/repro
```

## Orden de lectura

| Notebook | Fase | Qué hace | Lee | Escribe |
|---|---|---|---|---|
| `01_ingesta` | 2 | Documenta y verifica la ingesta: inventario, SHA-256 y estado de HDFS. Con `EJECUTAR_DESCARGA = True` vuelve a descargar. | `zona_cruda/`, `~/aggtrades_local/` o la evidencia del repo | — |
| `02_calidad_datos` | 3 | Recalcula los controles de `src/quality.py` sobre los datos limpios y lista los huecos. | `data/klines_clean/` | — |
| `03_eda` | 4 | Serie de paridad, desviación por evento, volumen normal frente a estrés y huecos. | `data/klines_clean/` | `informe/fig_serie_paridad.png`, `fig_volumen_comparacion.png`, `fig_huecos_tiempo.png` |
| `04_features` | 5.1 | Construye las 5 variables y fija los umbrales de severidad (p90 y p99). | `data/klines_clean/` | `data/features.parquet`, `informe/fig_umbrales_severidad.png` |
| `05_modelo_baseline` | 5.3 | Regresión logística con validación cruzada por evento. *Cifras retiradas.* | `data/features.parquet` | `informe/fig_matrices_confusion.png` |
| `05_modelo_definitivo` | 5.3 | Comparación de candidatos (LR/RF × balanceo/SMOTE). **Histórico**: sus cifras se retiraron por la fuga de `spread_btc_usdc_usdt`. | `data/features.parquet` | `informe/fig_matrices_confusion_definitivo.png` |
| `06_linea_base` | Pedido 7 | Línea base con datos de mercado: primera alerta, primera hora de estrés y primera hora ≥ 1 % por evento. | `data/features.parquet` | `informe/fig_linea_base.png` |
| `07_validacion_fuga` | Pedido 8 | Auditoría de fuga, predicción de la hora siguiente y validación temporal. **Evaluación vigente.** | `data/features.parquet` | `informe/fig_matrices_validacion_temporal.png` |
| `06_benchmarks` | 2, 5 y 9 | Tablas y gráficos de los benchmarks ya medidos (no vuelve a correr ninguno). | `benchmarks/resultados/` | — |

## Notas

- Los notebooks `03_eda`, `04_features` y `05_modelo_baseline` se generaron en su día con `build_notebook.py`, `build_features_notebook.py` y `build_modelo_notebook.py` (en la raíz). Si se vuelven a ejecutar esos generadores, **sobrescriben** el notebook y se pierden los resultados y los comentarios añadidos después.
- Las ventanas de evento (Terra 2022-05-07 a 05-17, FTX 2022-11-05 a 11-15, SVB 2023-03-08 a 03-18) son las mismas en todos los notebooks.
