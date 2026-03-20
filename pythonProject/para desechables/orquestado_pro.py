# Version 2.0

import subprocess
import time
import os
import signal

MOTORES = [
    {
        "nombre": "PRICE_SYNC",
        "script": "PRICE_SYNC_V1.03.py"
    },
    {
        "nombre": "CONTABLE",
        "script": "motor_saldos_v6.6.6.24.py"
    },
    {
        "nombre": "FUNDAMENTALES",
        "script": "fundamentales_engine_v1.5.py"
    },
    {
        "nombre": "MAESTRO",
        "script": "CÓDIGO MAESTRO V2.23.py"
    },
    {
        "nombre": "HISTORICO",
        "script": "motor_financiero_v1.3.0.py"  # ajusta nombre real
    }
]

procesos = {}


def iniciar_motor(motor):
    print(f"🚀 Iniciando {motor['nombre']}")

    p = subprocess.Popen(
        ["python", motor["script"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    return p


def monitor():
    while True:
        for motor in MOTORES:
            nombre = motor["nombre"]

            p = procesos.get(nombre)

            # Si nunca se ha iniciado
            if p is None:
                procesos[nombre] = iniciar_motor(motor)
                continue

            # Si murió → reiniciar
            if p.poll() is not None:
                print(f"⚠️ {nombre} murió. Reiniciando...")
                procesos[nombre] = iniciar_motor(motor)

        time.sleep(10)


def apagar():
    print("\n🛑 Apagando motores...")
    for nombre, p in procesos.items():
        try:
            p.terminate()
            print(f"✔ {nombre} detenido")
        except:
            pass


if __name__ == "__main__":

    print("💎 ORQUESTADOR PRO INICIADO")

    try:
        monitor()
    except KeyboardInterrupt:
        apagar()