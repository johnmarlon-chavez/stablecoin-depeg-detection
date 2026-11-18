# tablero/ — Tablero de paridad USDC

`tablero_paridad_usdc.html` es el tablero de riesgo de paridad USDC del proyecto: una página **autocontenida** con los indicadores de la serie horaria (por ejemplo, horas en alerta o estrés, anticipación de la alerta, calidad del dato y alertas fuera de los episodios).

- Los datos van dentro del propio HTML y se generaron a partir de [`data/consumo/`](../data/consumo/README.md), la salida de `src/capa_consumo.py`.
- No consulta HDFS ni ningún servicio. La única referencia externa es la fuente tipográfica de Google Fonts.
- Para verlo, basta con abrir el archivo en el navegador o servir **solo esta carpeta**:

```bash
cd tablero
python3 -m http.server 8080 --bind 127.0.0.1
# http://127.0.0.1:8080/tablero_paridad_usdc.html
```

Las instrucciones para publicarlo temporalmente con ngrok y el SHA-256 del archivo están en la sección 5.12 del [README principal](../README.md). Si se edita el HTML, hay que actualizar ese SHA-256.
