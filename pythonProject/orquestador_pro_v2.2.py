# Version 2.2 - Estabilidad Mejorada
import subprocess
import time
import signal
import sys
import os

MOTORES = [
    {"nombre": "PRICE_SYNC", "script": "PRICE_SYNC_V1_03.py"},
    {"nombre": "CONTABLE", "script": "motor_saldos_v6_6_6_24.py"},
    {"nombre": "FUNDAMENTALES", "script": "fundamentales_engine_v1_5.py"},
    {"nombre": "MAESTRO", "script": "CÓDIGO_MAESTRO_V2_23.py"},
    {"nombre": "FINANCIERO", "script": "motor_financiero_v1_3_0.py"}
]

procesos = {}
corriendo = True

def iniciar_motor(motor):
    # Verificamos si el archivo existe antes de lanzarlo
    if not os.path.exists(motor["script"]):
        print(f"❌ Error: No se encuentra el archivo {motor['script']}")
        return None

    print(f"🚀 Iniciando {motor['nombre']}...")
    
    # IMPORTANTE: Usamos sys.executable para mantener el .venv activo
    p = subprocess.Popen(
        [sys.executable, motor["script"]],
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
    )
    return p

def apagar():
    global corriendo
    corriendo = False
    print("\n🛑 Apagando motores...")
    for nombre, p in procesos.items():
        if p:
            try:
                p.send_signal(signal.CTRL_BREAK_EVENT)
                p.wait(timeout=3)
                print(f"✔ {nombre} detenido.")
            except:
                p.kill()
                print(f"💀 {nombre} forzado a cerrar.")

def signal_handler(sig, frame):
    apagar()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def monitor():
    print("💎 SISTEMA DE MONITOREO ACTIVO")
    while corriendo:
        for motor in MOTORES:
            nombre = motor["nombre"]
            p = procesos.get(nombre)

            # REINICIO AUTOMÁTICO: Si no existe o poll() devuelve un código (está muerto)
            if p is None or p.poll() is not None:
                if p is not None:
                    print(f"⚠️ {nombre} se detuvo inesperadamente. Reiniciando...")
                
                procesos[nombre] = iniciar_motor(motor)

        time.sleep(20)

if __name__ == "__main__":
    print("========================================")
    print("💎 ORQUESTADOR PRO V2.2 - TRADING ENGINE")
    print(f"🐍 Entorno: {sys.executable}")
    print("========================================")
    try:
        monitor()
    except KeyboardInterrupt:
        apagar()