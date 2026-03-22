# Version 2.3 - Estabilidad y Control de Flujo (Staggered Start)
import subprocess
import time
import signal
import sys
import os

MOTORES = [
    {"nombre": "PRICE_SYNC", "script": "pythonProject/PRICE_SYNC_V1_03.py"},
    {"nombre": "CONTABLE", "script": "pythonProject/motor_saldos_v6_6_6_24.py"},
    # {"nombre": "FUNDAMENTALES", "script": "pythonProject/fundamentales_engine_v1_5.py"},
    {"nombre": "MAESTRO", "script": "pythonProject/CÓDIGO_MAESTRO_V2_23.py"},
    {"nombre": "FINANCIERO", "script": "pythonProject/motor_financiero_v1_3_0.py"}
]

procesos = {}
corriendo = True

def iniciar_motor(motor):
    if not os.path.exists(motor["script"]):
        print(f"❌ Error: No se encuentra el archivo {motor['script']}")
        return None

    print(f"🚀 Iniciando {motor['nombre']}...")
    
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

            if p is None or p.poll() is not None:
                if p is not None:
                    print(f"⚠️ {nombre} se detuvo inesperadamente. Reiniciando...")
                
                procesos[nombre] = iniciar_motor(motor)
                
                # --- AJUSTE DE ESTABILIDAD ---
                # Esperamos 5 segundos antes de evaluar el siguiente motor
                # Esto evita saturar Hostinger y las APIs en el arranque inicial
                time.sleep(5) 

        time.sleep(15) # Revisión de salud cada 15 segundos

if __name__ == "__main__":
    print("========================================")
    print("💎 ORQUESTADOR PRO V2.3 - TRADING ENGINE")
    print(f"🐍 Entorno: {sys.executable}")
    print(F"📍 Directorio: {os.getcwd()}")
    print("========================================")
    try:
        monitor()
    except KeyboardInterrupt:
        apagar()
