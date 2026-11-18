# informe/ — Figuras del informe

Imágenes PNG que se insertan en el informe del proyecto. La mayoría las genera un notebook al ejecutarse; si se vuelve a correr el notebook, la figura se sobrescribe.

| Figura | Qué muestra | Generada por |
|---|---|---|
| `fig_serie_paridad.png` | Precio de paridad (USDCUSDT + proxy BUSDUSDT) con los tres eventos marcados. | `notebooks/03_eda` |
| `fig_volumen_comparacion.png` | Volumen medio normal frente al de las ventanas de evento. | `notebooks/03_eda` |
| `fig_huecos_tiempo.png` | Huecos de tiempo por símbolo. | `notebooks/03_eda` |
| `fig_umbrales_severidad.png` | Distribución de `desviacion_paridad` con los umbrales de alerta y estrés. | `notebooks/04_features` |
| `fig_linea_base.png` | Línea base: primera alerta, primera hora de estrés y primera hora ≥ 1 % por evento. | `notebooks/06_linea_base` |
| `fig_matrices_validacion_temporal.png` | Matrices de confusión de la validación temporal (**vigente**). | `notebooks/07_validacion_fuga` |
| `fig_matrices_confusion.png` | Matrices del modelo baseline (**cifras retiradas**). | `notebooks/05_modelo_baseline` |
| `fig_matrices_confusion_definitivo.png` | Matrices del "modelo definitivo" (**cifras retiradas** por la fuga). | `notebooks/05_modelo_definitivo` |
| `fig_arquitectura.png` | Arquitectura del sistema (copia de `architecture/system_architecture.png`). | — |
| `fig_broadcast_join.png` | Comparación broadcast frente a sort-merge join (Pedido 2). | Hecha fuera del repositorio |
| `fig_escalabilidad_aggtrades_tiempo.png`, `fig_escalabilidad_aggtrades_ram.png` | Escalabilidad de aggTrades, local frente a clúster (Pedido 9). | Hechas fuera del repositorio |

El notebook `notebooks/06_benchmarks.ipynb` dibuja gráficos equivalentes a los de los benchmarks a partir de `benchmarks/resultados/`.
