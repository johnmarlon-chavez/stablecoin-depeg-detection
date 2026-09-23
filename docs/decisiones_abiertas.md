# Decisiones abiertas del equipo — Proyecto detección de estrés de paridad USDC

## B.1 — Ausencia total de datos de FTX en pares USDC (RESUELTO)

**Fecha de auditoría:** 20 de setiembre de 2026
**Responsable de la verificación:** Andrés Pagán (Análisis y modelado), confirmado por Marlon Chávez (Ingeniería de datos)

### Hallazgo confirmado

Verificado directamente contra el bucket público de Binance (data.binance.vision):

| Par | Klines mensuales | Klines diarios |
|---|---|---|
| USDCUSDT | Faltan oct-2022 a feb-2023 completos (salta de 2022-09 a 2023-03) | Último archivo: 2022-09-26. Siguiente: 2023-03-11. Vacío total ~5.5 meses |
| BTCUSDC | Mismo hueco exacto: oct-2022 a feb-2023 | Confirmado vacío en noviembre 2022 (0 archivos) |
| BTCUSDT | Sin huecos, cobertura completa desde 2017 | No se requirió verificar: el mensual ya está completo |

Se verificó en las cuatro temporalidades (1h, 4h, 1d, aggTrades) para USDCUSDT y BTCUSDC: todas vacías por igual entre el 27 de septiembre de 2022 y el 10-11 de marzo de 2023. Se verificó también ETHUSDC (otro par cotizado en USDC) en la misma ventana: también vacío, confirmando que no es un archivo faltante puntual.

### Causa confirmada

En septiembre de 2022, Binance anunció la conversión automática de saldos de USDC, USDP y TUSD a BUSD, suspendiendo efectivamente esos pares. La liquidez de stablecoin en Binance se trasladó a BUSD durante ese tramo.

### Decisión adoptada por el equipo

Opción 1 (adoptada): usar BUSDUSDT como proxy de paridad, solo durante la ventana oct-2022 a feb-2023.

BUSDUSDT tiene cobertura mensual completa y continua de octubre 2022 a septiembre 2023, cubriendo exactamente el hueco (incluido noviembre 2022, el mes de FTX).

- El proxy no reemplaza a USDCUSDT/BTCUSDC en los meses donde sí hay datos, solo completa el hueco.
- Cada fila del dataset debe llevar una columna fuente_paridad ('USDCUSDT' o 'BUSDUSDT_proxy') para dejar explícito en el informe dónde se usó el proxy.
- Nota metodológica para el informe: "Se usó BUSDUSDT como proxy porque Binance suspendió los pares USDC en ese periodo por la conversión automática a BUSD anunciada en septiembre de 2022."

Con esta solución, las tres combinaciones de validación cruzada (Terra+SVB→FTX, Terra+FTX→SVB, FTX+SVB→Terra) quedan viables.

### Pendiente de verificar (paso 3 de la Fase 2, sección 6.4)

Confirmar si el archivo mensual USDCUSDT-1h-2023-03.zip trae filas desde el 1 de marzo o solo desde el 11 (fecha en que arranca el diario). Si el mensual también arranca el 11, el día exacto del colapso de SVB (10 de marzo) quedaría sin dato y habría que evaluar usar BUSDUSDT también ahí. Estado: sin verificar todavía.
