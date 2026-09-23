#!/usr/bin/env python3
"""
Descarga el dataset del proyecto "Detección temprana de pérdida de paridad
en stablecoins" (Big Data y Analítica de Datos, UPAO) desde data.binance.vision.

Descarga dos tipos de dato para los 3 símbolos del proyecto:
  1. Klines horarios (1h) de TODO el periodo: enero 2022 -> mes actual.
     (según la sección 1.6 "Alcance": cubre los tres eventos de referencia)
  2. AggTrades diarios SOLO en las ventanas de los 3 eventos históricos
     (según la guía: "aggTrades de nivel minuto en las tres ventanas de evento").

Cada archivo se descarga junto a su .CHECKSUM y se verifica con SHA256
antes de guardarlo (sección 1.7 "Veracidad" de la guía).

Los archivos se guardan en la ZONA CRUDA con el nombre original que publica
Binance (símbolo-tipo_de_dato-periodo), tal como exige la sección 3.2:
    zona_cruda/klines/<SIMBOLO>/<SIMBOLO>-1h-<AAAA-MM>.zip
    zona_cruda/aggTrades/<SIMBOLO>/<SIMBOLO>-aggTrades-<AAAA-MM-DD>.zip

Uso:
    python3 descargar_dataset.py                # descarga todo
    python3 descargar_dataset.py --solo-klines   # solo klines
    python3 descargar_dataset.py --solo-eventos  # solo aggTrades de eventos
"""

import argparse
import hashlib
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuración del proyecto (según sección 1.6 "Alcance" de la guía)
# ---------------------------------------------------------------------------

SIMBOLOS = ["USDCUSDT", "BTCUSDC", "BTCUSDT"]

BASE_URL = "https://data.binance.vision/data/spot"
CARPETA_SALIDA = Path("zona_cruda")

# Klines: enero 2022 hasta el mes actual (mes actual se omite si aún no existe)
KLINES_DESDE = date(2022, 1, 1)
INTERVALO_KLINES = "1h"

# AggTrades: solo en torno a los 3 eventos históricos documentados en 1.3
# (ventana con margen antes/después del pico del evento; ajustable)
EVENTOS = [
    ("evento1_terra_ust_2022-05", date(2022, 5, 7), date(2022, 5, 17)),
    ("evento2_ftx_2022-11", date(2022, 11, 5), date(2022, 11, 15)),
    ("evento3_svb_usdc_2023-03", date(2023, 3, 8), date(2023, 3, 18)),
]

TIMEOUT = 30
PAUSA_ENTRE_DESCARGAS = 0.3  # segundos, para no saturar el servidor


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def meses_entre(inicio: date, fin: date):
    """Genera (año, mes) desde 'inicio' hasta 'fin', inclusive."""
    actual = date(inicio.year, inicio.month, 1)
    while actual <= fin:
        yield actual.year, actual.month
        if actual.month == 12:
            actual = date(actual.year + 1, 1, 1)
        else:
            actual = date(actual.year, actual.month + 1, 1)


def dias_entre(inicio: date, fin: date):
    d = inicio
    while d <= fin:
        yield d
        d += timedelta(days=1)


def sha256_de(ruta: Path) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def descargar_archivo(url: str, destino: Path) -> bool:
    """Descarga 'url' a 'destino' y verifica su .CHECKSUM. Devuelve True si
    quedó guardado y verificado correctamente."""
    if destino.exists():
        print(f"  [ya existe] {destino.name}")
        return True

    resp = requests.get(url, timeout=TIMEOUT)
    if resp.status_code == 404:
        print(f"  [no disponible en la fuente] {url}")
        return False
    resp.raise_for_status()

    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(resp.content)

    # Verificación de integridad con el .CHECKSUM (sección 1.7 "Veracidad")
    checksum_url = url + ".CHECKSUM"
    resp_hash = requests.get(checksum_url, timeout=TIMEOUT)
    if resp_hash.status_code == 200:
        hash_esperado = resp_hash.text.strip().split()[0]
        hash_real = sha256_de(destino)
        if hash_real != hash_esperado:
            print(f"  [ERROR de verificación] {destino.name} — se borra")
            destino.unlink()
            return False
        print(f"  [OK, verificado SHA256] {destino.name}")
    else:
        print(f"  [OK, sin checksum publicado] {destino.name}")

    time.sleep(PAUSA_ENTRE_DESCARGAS)
    return True


# ---------------------------------------------------------------------------
# Descarga de klines (mensual, todo el periodo)
# ---------------------------------------------------------------------------

def descargar_klines():
    hoy = date.today()
    print(f"\n=== Klines horarios ({KLINES_DESDE} a {hoy}) ===")
    ok, fallidos = 0, 0
    for simbolo in SIMBOLOS:
        for anio, mes in meses_entre(KLINES_DESDE, hoy):
            periodo = f"{anio:04d}-{mes:02d}"
            nombre = f"{simbolo}-{INTERVALO_KLINES}-{periodo}.zip"
            url = f"{BASE_URL}/monthly/klines/{simbolo}/{INTERVALO_KLINES}/{nombre}"
            destino = CARPETA_SALIDA / "klines" / simbolo / nombre
            print(f"{simbolo} {periodo} ...")
            if descargar_archivo(url, destino):
                ok += 1
            else:
                fallidos += 1
    print(f"\nKlines: {ok} archivos OK, {fallidos} no disponibles/fallidos.")


# ---------------------------------------------------------------------------
# Descarga de aggTrades (diario, solo ventanas de evento)
# ---------------------------------------------------------------------------

def descargar_agg_trades_eventos():
    print("\n=== AggTrades en ventanas de los 3 eventos históricos ===")
    ok, fallidos = 0, 0
    for nombre_evento, inicio, fin in EVENTOS:
        print(f"\n-- {nombre_evento} ({inicio} a {fin}) --")
        for simbolo in SIMBOLOS:
            for dia in dias_entre(inicio, fin):
                fecha_str = dia.strftime("%Y-%m-%d")
                nombre = f"{simbolo}-aggTrades-{fecha_str}.zip"
                url = f"{BASE_URL}/daily/aggTrades/{simbolo}/{nombre}"
                destino = CARPETA_SALIDA / "aggTrades" / simbolo / nombre
                print(f"{simbolo} {fecha_str} ...")
                if descargar_archivo(url, destino):
                    ok += 1
                else:
                    fallidos += 1
    print(f"\nAggTrades: {ok} archivos OK, {fallidos} no disponibles/fallidos.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--solo-klines", action="store_true",
                         help="Descarga solo los klines horarios")
    parser.add_argument("--solo-eventos", action="store_true",
                         help="Descarga solo los aggTrades de los eventos")
    args = parser.parse_args()

    CARPETA_SALIDA.mkdir(exist_ok=True)

    if args.solo_klines:
        descargar_klines()
    elif args.solo_eventos:
        descargar_agg_trades_eventos()
    else:
        descargar_klines()
        descargar_agg_trades_eventos()

    print(f"\nListo. Archivos guardados en: {CARPETA_SALIDA.resolve()}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
        sys.exit(1)
