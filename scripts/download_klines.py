#!/usr/bin/env python3
"""
Descarga los klines horarios (1h) del proyecto, incorporando la solución
del Anexo B.1 de la guía operativa: USDCUSDT y BTCUSDC no tienen datos
entre 2022-09-27 y 2023-03-10/11 (Binance convirtió USDC/USDP/TUSD a BUSD
en sept. 2022). Se usa BUSDUSDT como proxy en esa ventana y en el día
puntual del 10 de marzo de 2023 (colapso SVB).

Rango fijo del proyecto: 2022-01 a 2023-12 (24 meses), no "hasta hoy".

Uso:
    python3 descargar_klines.py                  # todo
    python3 descargar_klines.py --solo-proxy      # solo BUSDUSDT mensual del hueco FTX
    python3 descargar_klines.py --solo-proxy-svb  # solo BUSDUSDT diario del 10 de marzo
    python3 descargar_klines.py --solo-marzo      # solo el chequeo del archivo de marzo
"""

import argparse
import csv
import hashlib
import io
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path

import requests

BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"
CARPETA_SALIDA = Path("zona_cruda") / "klines"
MISSING_LOG = Path("zona_cruda") / "missing.log"

INTERVALO = "1h"
PARES = ["USDCUSDT", "BTCUSDC", "BTCUSDT"]

# Rango FIJO del proyecto (guía v4): 2022-01 a 2023-12, 24 meses exactos.
# NO usar date.today() como límite superior — eso arrastra años sin ningún
# evento ancla del proyecto y solo ocupa espacio en el clúster sin aportar
# nada al análisis.
DESDE = date(2022, 1, 1)
HASTA = date(2023, 12, 1)

PAR_PROXY = "BUSDUSDT"
MESES_PROXY = ["2022-10", "2022-11", "2022-12", "2023-01", "2023-02"]
DIAS_PROXY = ["2023-03-10"]
BASE_URL_DIARIO = "https://data.binance.vision/data/spot/daily/klines"

TIMEOUT = 30
PAUSA = 0.3


def meses_entre(inicio: date, fin: date):
    actual = date(inicio.year, inicio.month, 1)
    while actual <= fin:
        yield actual.year, actual.month
        actual = date(actual.year + 1, 1, 1) if actual.month == 12 \
            else date(actual.year, actual.month + 1, 1)


def sha256_de(ruta: Path) -> str:
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(8192), b""):
            h.update(bloque)
    return h.hexdigest()


def registrar_faltante(nombre: str):
    MISSING_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(MISSING_LOG, "a") as f:
        f.write(f"{nombre}\n")


def descargar_archivo(url: str, destino: Path) -> bool:
    if destino.exists():
        print(f"  [ya existe] {destino.name}")
        return True

    resp = requests.get(url, timeout=TIMEOUT)
    if resp.status_code == 404:
        print(f"  [no disponible en la fuente] {url}")
        registrar_faltante(destino.name)
        return False
    resp.raise_for_status()

    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(resp.content)

    checksum_url = url + ".CHECKSUM"
    resp_hash = requests.get(checksum_url, timeout=TIMEOUT)
    if resp_hash.status_code == 200:
        hash_esperado = resp_hash.text.strip().split()[0]
        hash_real = sha256_de(destino)
        if hash_real != hash_esperado:
            print(f"  [ERROR de verificación] {destino.name} — se borra")
            destino.unlink()
            registrar_faltante(destino.name + " (checksum invalido)")
            return False
        print(f"  [OK, verificado SHA256] {destino.name}")
    else:
        print(f"  [OK, sin checksum publicado] {destino.name}")

    time.sleep(PAUSA)
    return True


def descargar_klines_par(simbolo: str, desde: date, hasta: date):
    print(f"\n=== Klines {INTERVALO} de {simbolo} ({desde} a {hasta}) ===")
    ok, fallidos = 0, 0
    for anio, mes in meses_entre(desde, hasta):
        periodo = f"{anio:04d}-{mes:02d}"
        nombre = f"{simbolo}-{INTERVALO}-{periodo}.zip"
        url = f"{BASE_URL}/{simbolo}/{INTERVALO}/{nombre}"
        destino = CARPETA_SALIDA / simbolo / nombre
        if descargar_archivo(url, destino):
            ok += 1
        else:
            fallidos += 1
    print(f"{simbolo}: {ok} OK, {fallidos} no disponibles.")
    return ok, fallidos


def descargar_proxy():
    print(f"\n=== Proxy {PAR_PROXY} para el hueco FTX (Anexo B.1) ===")
    ok, fallidos = 0, 0
    for periodo in MESES_PROXY:
        nombre = f"{PAR_PROXY}-{INTERVALO}-{periodo}.zip"
        url = f"{BASE_URL}/{PAR_PROXY}/{INTERVALO}/{nombre}"
        destino = CARPETA_SALIDA / PAR_PROXY / nombre
        if descargar_archivo(url, destino):
            ok += 1
        else:
            fallidos += 1
    print(f"{PAR_PROXY} (proxy): {ok} OK, {fallidos} no disponibles.")


def descargar_proxy_dia_svb():
    print(f"\n=== Proxy {PAR_PROXY} diario para el día puntual de SVB ===")
    ok, fallidos = 0, 0
    for dia in DIAS_PROXY:
        nombre = f"{PAR_PROXY}-{INTERVALO}-{dia}.zip"
        url = f"{BASE_URL_DIARIO}/{PAR_PROXY}/{INTERVALO}/{nombre}"
        destino = CARPETA_SALIDA / PAR_PROXY / "diario" / nombre
        if descargar_archivo(url, destino):
            ok += 1
        else:
            fallidos += 1
    print(f"{PAR_PROXY} (proxy diario SVB): {ok} OK, {fallidos} no disponibles.")


def verificar_marzo_2023():
    print("\n=== Verificación del archivo USDCUSDT-1h-2023-03 (evento SVB) ===")
    nombre = f"USDCUSDT-{INTERVALO}-2023-03.zip"
    url = f"{BASE_URL}/USDCUSDT/{INTERVALO}/{nombre}"
    destino = CARPETA_SALIDA / "USDCUSDT" / nombre

    if not descargar_archivo(url, destino):
        print("No se pudo descargar el archivo de marzo 2023 para verificar.")
        return

    with zipfile.ZipFile(destino) as z:
        nombre_csv = z.namelist()[0]
        with z.open(nombre_csv) as f:
            primera_linea = io.TextIOWrapper(f, encoding="utf-8").readline()

    campos = next(csv.reader([primera_linea]))
    open_time_raw = int(campos[0])
    open_time = datetime.utcfromtimestamp(open_time_raw / 1000)

    print(f"Primera fila del archivo -> open_time: {open_time} UTC")
    if open_time.day == 1:
        print("RESULTADO: el mensual SÍ cubre desde el día 1 de marzo.")
    else:
        print(f"RESULTADO: el mensual arranca el día {open_time.day}, NO desde el 1.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--solo-proxy", action="store_true")
    parser.add_argument("--solo-proxy-svb", action="store_true")
    parser.add_argument("--solo-marzo", action="store_true")
    args = parser.parse_args()

    CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

    if args.solo_proxy:
        descargar_proxy()
    elif args.solo_proxy_svb:
        descargar_proxy_dia_svb()
    elif args.solo_marzo:
        verificar_marzo_2023()
    else:
        for par in PARES:
            descargar_klines_par(par, DESDE, HASTA)
        descargar_proxy()
        descargar_proxy_dia_svb()

    print(f"\nListo. Revisa {MISSING_LOG} para ver qué archivos no existen en la fuente.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
        sys.exit(1)
