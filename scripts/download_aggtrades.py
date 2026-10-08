#!/usr/bin/env python3
"""
Descarga los aggTrades DIARIOS de las 3 ventanas de evento (Pedido 6) desde
data.binance.vision, verifica SHA-256 contra el .CHECKSUM publicado
(re-descarga hasta 3 veces si no coincide) y extrae el CSV.
Solo descarga de archivos; no usa la API REST. No inventa datos: si un
archivo no existe en la fuente se reporta como hueco.

Salida (fuera del repo, por defecto ~/aggtrades_local):
    zip/<SIMBOLO>/<archivo>.zip      (sin renombrar)
    csv/<SIMBOLO>/<archivo>.csv
    descarga_resumen.tsv

Uso:  python3 scripts/download_aggtrades.py [--out RUTA]
"""
import argparse
import hashlib
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path

BASE = "https://data.binance.vision/data/spot/daily/aggTrades"
TIMEOUT = 60
INTENTOS = 3

# (evento, simbolo, desde, hasta)  -- 14 archivos en total
VENTANAS = [
    ("Terra", "USDCUSDT", date(2022, 5, 9), date(2022, 5, 13)),
    ("FTX", "BUSDUSDT", date(2022, 11, 7), date(2022, 11, 11)),
    ("SVB", "BUSDUSDT", date(2023, 3, 10), date(2023, 3, 10)),
    ("SVB", "USDCUSDT", date(2023, 3, 11), date(2023, 3, 13)),
]


def dias(desde, hasta):
    d = desde
    while d <= hasta:
        yield d
        d += timedelta(days=1)


def sha256_de(ruta):
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def abrir(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, timeout=TIMEOUT)


def bajar(url, destino):
    tmp = destino.with_suffix(destino.suffix + ".part")
    with abrir(url) as r, open(tmp, "wb") as f:
        for b in iter(lambda: r.read(1 << 20), b""):
            f.write(b)
    tmp.rename(destino)


def checksum_publicado(url):
    try:
        with abrir(url + ".CHECKSUM") as r:
            return r.read().decode().split()[0].lower()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def contar_y_primera(ruta):
    n = 0
    primera = ""
    with open(ruta, "rb") as f:
        for i, linea in enumerate(f):
            if i == 0:
                primera = linea.decode("utf-8", "replace").strip()
            n += 1
    return n, primera


def procesar(evento, sim, dia, out):
    nombre = f"{sim}-aggTrades-{dia.isoformat()}.zip"
    url = f"{BASE}/{sim}/{nombre}"
    dz = out / "zip" / sim / nombre
    dz.parent.mkdir(parents=True, exist_ok=True)
    fila = dict(evento=evento, simbolo=sim, fecha=dia.isoformat(), archivo=nombre,
                zip_bytes=0, csv_bytes=0, registros=0, checksum="", ts_digitos="")

    try:
        esperado = checksum_publicado(url)
    except Exception as e:
        fila["checksum"] = f"ERROR_CHECKSUM({e})"
        return fila
    if esperado is None:
        print(f"  [sin .CHECKSUM publicado] {nombre}")

    estado = None
    for intento in range(1, INTENTOS + 1):
        try:
            if not dz.exists():
                bajar(url, dz)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                fila["checksum"] = "NO_DISPONIBLE_EN_FUENTE"
                print(f"  [HUECO: 404 en la fuente] {nombre}")
                return fila
            print(f"  [HTTP {e.code}] intento {intento}")
            time.sleep(2)
            continue
        except Exception as e:
            print(f"  [error de red: {e}] intento {intento}")
            if dz.exists():
                dz.unlink()
            time.sleep(2)
            continue
        if esperado is None:
            estado = "SIN_CHECKSUM"
            break
        real = sha256_de(dz)
        if real == esperado:
            estado = "OK"
            break
        print(f"  [CHECKSUM NO COINCIDE] {nombre} intento {intento}: "
              f"esperado {esperado[:12]}.. real {real[:12]}.. -> se borra y se re-descarga")
        dz.unlink()
    if estado is None:
        fila["checksum"] = "FALLA_TRAS_REINTENTOS"
        return fila

    fila["checksum"] = estado
    fila["zip_bytes"] = dz.stat().st_size
    dcsv = out / "csv" / sim
    dcsv.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dz) as zf:
        miembros = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        zf.extract(miembros[0], dcsv)
        ruta_csv = dcsv / miembros[0]
    fila["csv_bytes"] = ruta_csv.stat().st_size
    n, primera = contar_y_primera(ruta_csv)
    campos = primera.split(",")
    if campos and not campos[0].isdigit():
        print(f"  [AVISO] la primera linea parece cabecera: {primera}")
    fila["registros"] = n
    fila["ts_digitos"] = str(len(campos[5])) if len(campos) > 5 else "?"
    print(f"  [{estado}] {nombre} zip={fila['zip_bytes']:,} csv={fila['csv_bytes']:,} "
          f"filas={n:,} ts_digitos={fila['ts_digitos']}")
    return fila


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(Path.home() / "aggtrades_local"))
    out = Path(ap.parse_args().out)
    out.mkdir(parents=True, exist_ok=True)

    filas = []
    for evento, sim, d0, d1 in VENTANAS:
        print(f"\n=== {evento}: {sim} {d0} a {d1} ===")
        for d in dias(d0, d1):
            filas.append(procesar(evento, sim, d, out))

    cols = ["evento", "simbolo", "fecha", "archivo", "zip_bytes", "csv_bytes",
            "registros", "checksum", "ts_digitos"]
    with open(out / "descarga_resumen.tsv", "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in filas:
            f.write("\t".join(str(r[c]) for c in cols) + "\n")

    ok = [r for r in filas if r["checksum"] in ("OK", "SIN_CHECKSUM")]
    print("\n=== RESUMEN ===")
    print(f"archivos esperados: {len(filas)} | descargados: {len(ok)} | "
          f"con problema: {len(filas) - len(ok)}")
    print(f"zip total: {sum(r['zip_bytes'] for r in ok)/1e6:.1f} MB | "
          f"csv total: {sum(r['csv_bytes'] for r in ok)/1e6:.1f} MB | "
          f"registros totales: {sum(r['registros'] for r in ok):,}")
    for r in filas:
        if r not in ok:
            print(f"  PROBLEMA: {r['archivo']} -> {r['checksum']}")
    sys.exit(0 if len(ok) == len(filas) else 1)


if __name__ == "__main__":
    main()
