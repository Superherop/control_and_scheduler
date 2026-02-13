import sys
import os
import time
from datetime import datetime
import json
import importlib
import multiprocessing
import logging

# --- PYTHONPATH fix ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Teszt mód bekapcsolása parancssori argumentummal: python main.py --test
TEST_MODE = "--test" in sys.argv

def print_basedir():
    print(f"[DEBUG] BASE_DIR: {BASE_DIR}")

# Válasszuk ki a konfigurációs fájlt mód szerint
if TEST_MODE:
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "test_config.json")
    MAIL_LIST_PATH = os.path.join(os.path.dirname(__file__), "test_mail_list.json")
else:
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
    MAIL_LIST_PATH = os.path.join(os.path.dirname(__file__), "mail_list.json")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = json.load(f)

with open(MAIL_LIST_PATH, "r", encoding="utf-8") as f:
    mail_list = json.load(f)

print(f"[INFO] Scheduler indul... konfiguráció: {os.path.basename(CONFIG_PATH)}")
print_basedir()

#TODO: Kell definiálni egy listát ,ahol gyűjtöm azokat a riportokat, amik valamilyen oknál fogva leállnak

def run_module_process(module_name):
    """Modul futtatása külön processben részletes hibaloggal."""
    try:
        if TEST_MODE:
            # Létrehozzuk a debug log könyvtárat
            log_dir = os.path.join(os.path.dirname(__file__), "debug_logs")
            os.makedirs(log_dir, exist_ok=True)
            # Modulhoz tartozó fájl elérési útja
            file_path = os.path.join(log_dir, f"{module_name}.txt")
            # Stdout és stderr átirányítása a fájlba
            sys_stdout_orig = sys.stdout
            sys_stderr_orig = sys.stderr
            sys.stdout = open(file_path, "w", encoding="utf-8")
            sys.stderr = sys.stdout

        logging.info(f"[START] Modul betöltése: {module_name}")
        mod = importlib.import_module(f"modules.{module_name}")
        if hasattr(mod, "run"):
            logging.info(f"[RUN] Modul futtatása indul: {module_name}")
            try:
                mod.run()
            except Exception as e:
                logging.error(f"[ERROR] A {module_name} futtatása sikertelen {e} hibából fakadóan")

            logging.info(f"[END] Modul futtatás sikeresen befejeződött: {module_name}")
        else:
            msg = f"{module_name} modulban nincs run() függvény!"
            logging.warning(msg)
            print(f"[WARN] {msg}")
    except Exception as e:
        import traceback
        err_msg = f"[ERROR] {module_name} modul hibával leállt: {e}"
        logging.error(err_msg)
        logging.error(traceback.format_exc())
        print(err_msg)
        print(traceback.format_exc())
    finally:
        if TEST_MODE:
            # Bezárjuk a fájlt és visszaállítjuk az eredeti stdout/stderr-t
            sys.stdout.close()
            sys.stdout = sys_stdout_orig
            sys.stderr = sys_stderr_orig

# Logger setup
LOG_FILE = os.path.join(os.path.dirname(__file__), "scheduler.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    completed_today = {}         # modul -> befejezési idő
    running_processes = {}       # modul -> (Process, start_idő)
    starting_modules = set()     # modulok, amiket most indítottunk, hogy ne duplázzon
    last_day = time.strftime("%Y-%m-%d")

    while True:
        current_date = time.strftime("%Y-%m-%d")
        if current_date != last_day:
            last_day = current_date
            completed_today.clear()
            running_processes.clear()
            starting_modules.clear()
            logging.info("[RESET] Új nap kezdete, státuszok nullázva.")

        current_time = time.strftime("%H:%M")
        current_day = time.strftime("%a")

        print("\n---- Futási állapot ----")
        for module, settings in config["enabled_modules"].items():
            if settings["enabled"]:
                if module in completed_today:
                    status = f"✅ Ma lefutott (befejezés: {completed_today[module]})"
                elif module in running_processes and running_processes[module][0].is_alive():
                    start_time = running_processes[module][1]
                    status = f"🚀 Fut (indult: {start_time})"
                elif current_day in settings["days"] and settings["schedule"] >= current_time:
                    status = "⏳ Ma még futnia kell"
                else:
                    status = "🔹 Ma nem ütemezett"

                print(f"{module:15} {status} (Tervezett: {settings['schedule']} {','.join(settings['days'])})")

        # paralel indítás
        for module, settings in config["enabled_modules"].items():
            if settings["enabled"] and (
                TEST_MODE or (settings["schedule"] == current_time and current_day in settings["days"])
            ):
                if TEST_MODE or current_day in settings["days"]:
                    already_running = module in running_processes and running_processes[module][0].is_alive()
                    already_starting = module in starting_modules
                    already_completed = module in completed_today  # új védelem

                    if not already_running and not already_starting and not already_completed:
                        start_str = time.strftime("%H:%M:%S")
                        logging.info(f"Indítás: {module} (start: {start_str}) [TEST_MODE={TEST_MODE}]")
                        starting_modules.add(module)  # jelöljük, hogy most indul

                        p = multiprocessing.Process(target=run_module_process, args=(module,))
                        p.daemon = True
                        p.start()
                        running_processes[module] = (p, start_str)

        # ellenőrzés: végeztek-e
        finished_modules = [m for m, (proc, _) in running_processes.items() if not proc.is_alive()]
        for m in finished_modules:
            finish_str = time.strftime("%H:%M:%S")
            proc, start_time = running_processes[m]
            exit_code = proc.exitcode

            if exit_code == 0:
                # sikeres futás (normál kilépés)
                completed_today[m] = finish_str
                logging.info(f"Befejezés: {m} (finish: {finish_str})")
                # a processz lezárása
                del running_processes[m]
            else:
                # hiba történt — ne számítsuk be "lefutottnak"
                logging.error(f"HIBA: {m} processz {exit_code} kóddal állt le — újraindítás...")

                # azonnali újraindítás
                #wait time before restart
                time.sleep(60)
                restart_str = time.strftime("%H:%M:%S")
                p = multiprocessing.Process(target=run_module_process, args=(m,))
                p.daemon = True
                p.start()
                running_processes[m] = (p, restart_str)
                starting_modules.add(m)  # jelöljük indulás alatt

            # takarítás az előző processzről
            starting_modules.discard(m)

        time.sleep(60)