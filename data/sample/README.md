# Muestras de datos (data/sample)

Primeras 100 filas **reales**, sin modificar, de los archivos descargados de Binance (data.binance.vision). No hay datos inventados.

- `klines/`: velas de 1 hora, CSV original de Binance (12 columnas: open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_buy_base, taker_buy_quote, ignore).
- `aggtrades/`: operaciones agregadas, CSV original de Binance (agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time en ms, is_buyer_maker, is_best_match).

Los datos completos no estan en el repositorio: se descargan con `scripts/download_klines.py` y `scripts/download_aggtrades.py`.
