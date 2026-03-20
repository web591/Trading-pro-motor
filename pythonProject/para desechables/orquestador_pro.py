# Version 2.1

import subprocess
import time
import signal
import sys

MOTORES = [
    {"nombre": "PRICE_SYNC", "script": "PRICE_SYNC_V1.03.py"},
    {"nombre": "CONTABLE", "script": "motor_saldos_v6.6.6.24.py"},
    {"nombre": "FUNDAMENTALES", "script": "fundamentales_engine_v1.5.py"},
    {"nombre": "MAESTRO", "script": "CÓDIGO MAESTRO V2.23.py"},
    {"nombre": "FINANCIERO", "script": "motor_financiero_v1.3.0.py"}
]

procesos = {}
corriendo = True


# ==========================================================
# INICIAR MOTOR
# ==========================================================
def iniciar_motor(motor):
    print(f"🚀 Iniciando {motor['nombre']}")

    p = subprocess.Popen(
        ["python", motor["script"]],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP  # 🔥 clave Windows
    )

    return p


# ==========================================================
# APAGADO LIMPIO
# ==========================================================
def apagar():
    global corriendo
    corriendo = False

    print("\n🛑 Apagando motores...")

    for nombre, p in procesos.items():
        try:
            print(f"Deteniendo {nombre}...")

            # 🔥 Enviar CTRL+C al proceso
            p.send_signal(signal.CTRL_BREAK_EVENT)

            try:
                p.wait(timeout=5)
            except:
                print(f"Forzando cierre {nombre}")
                p.kill()

            print(f"✔ {nombre} detenido")

        except Exception as e:
            print(f"Error apagando {nombre}: {e}")


# ==========================================================
# MANEJO DE SEÑALES (X, CTRL+C, STOP)
# ==========================================================
def signal_handler(sig, frame):
    print("\n⚠️ Señal de cierre recibida")
    apagar()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)   # CTRL+C
signal.signal(signal.SIGTERM, signal_handler)  # PyCharm / cierre


# ==========================================================
# MONITOR
# ==========================================================
def monitor():
    while corriendo:
        for motor in MOTORES:
            nombre = motor["nombre"]
            p = procesos.get(nombre)

            if p is None:
                procesos[nombre] = iniciar_motor(motor)
                continue

            if p.poll() is not None:
                print(f"⚠️ {nombre} murió. Reiniciando...")
                procesos[nombre] = iniciar_motor(motor)

        time.sleep(10)


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":

    print("💎 ORQUESTADOR PRO INICIADO")

    try:
        monitor()
    except KeyboardInterrupt:
        apagar()