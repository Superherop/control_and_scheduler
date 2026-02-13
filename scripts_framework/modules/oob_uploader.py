import sys
import os
import pandas as pd
import time

# --- PYTHONPATH fix: mindig felvesszük a scripts mappa szintjét ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core import utils, database, paths  # tesco_weeks törölve

def check_validity():
    """Ellenőrzi, hogy az adatbázisban a max fiscal_week megegyezik-e az aktuálissal."""
    query = "SELECT max(fiscal_week) AS max_fw FROM tesco_analysts.pmajor1_oob"
    result_df = database.DatabaseManager.run_query(query)
    if result_df is None or result_df.empty:
        return False
    latest_fw_in_db = result_df["max_fw"].iloc[0]
    current_fw = database.get_current_tesco_week()
    return current_fw == latest_fw_in_db


def run(check_need=True, manual_input=None):
    logger = utils.setup_logger("OOBUploader", log_file=os.path.join(paths.LOG_PATH, "oob.log"))
    if not manual_input:
        logger.info("[OOB]: A riport feldolgozás indul.")

        try:
            file_path = utils.list_excel_files(paths.OOB_SOURCE, "Open Order Book")
            logger.info(f"[OOB]: Megvan a mai fájl: {file_path}")
        except FileNotFoundError:
            logger.info("[OOB]: A legfrissebb fájl nem található, várakozás...")
            file_path = utils.wait_until_file_ready(paths.OOB_SOURCE, "Open Order Book")
    else:
        print(f"[OOB]: Working from manual input: {manual_input}")
        logger.info(f"[OOB]: Working from manual input: {manual_input}")
        file_path=manual_input
    try:    
        oob = utils.read_and_process_file(file_path, 'Detail', [
            'ORDER NO', 'LM PO NO', 'Master TPN', 'Cases on pallet', 'NO of pallet',
            'QTY ORDERED', 'QTY OUTSTANDING (Units)', 'VALUE OUTSTANDING (GBP)', 
            'CASES ON ORDER', 'HDC SOH UNITS', 'HDC SOH CASES', 'HDC WKS COVER',
            'raised delivery week vs.actual delivery week_[in weeks]', 'ETA HDC',
            'ETA HDC WEEK', 'CONFIRMED BOOKING week', 'CONFIRMED BOOKING day', 
            'ELC_price_EUR', 'ELC_price_GBP', 'ELC_value_GBP', 'NEW DELIVERY WEEK', 'Hazardous'
        ], 4)
        oob.rename(columns={'QTY OUTSTANDING (Units)': 'QTY OUTSTANDING Units', 
                            'VALUE OUTSTANDING (GBP)': 'VALUE OUTSTANDING GBP',
                            'raised delivery week vs.actual delivery week_[in weeks]': 
                            'raised_delivery_week_vs_actual_delivery_week', 
                            'NEW DELIVERY WEEK': 'new_delivery_week'}, inplace=True)
        time_stamp = utils.creation_date(file_path) #TODO: Zsoltit megkérdezni, hogy baj-e, ha ezt kibaszom innen
        oob['creation time'] = time_stamp
        oob['fiscal_week'] = database.get_current_tesco_week()
        logger.info({'[OOB]: upload is succes'})
        print('[OOB]: is done')
        oob.to_csv(os.path.join(paths.HDP_PATH, "pmajor1_oob.csv"), index=False, sep='|')

        if check_need:
            logger.info("[OOB]: Érvényességi ellenőrzés indul...")
            while not check_validity():
                logger.warning("[OOB]: Report még nem érvényes, újraellenőrzés 10 perc múlva...")
                time.sleep(600)
            database.DatabaseManager.smtp_email_send(
                subject='OOB upload log',
                sender_mail='patrik.major@tesco.com',
                receiver_mail='Zsolt.Kovacs@tesco.com',
                cc_mail='patrik.major@tesco.com',
                html=f'OOB upload sikeresen lefutott. A tábla {time_stamp} dátumbélyeggel készült. A Hadoop-ban: tesco_analysts.pmajor1_oob'
            )

        logger.info("[OOB]: A riport feldolgozás kész.")
        logger.info(f"[OOB]: Az alábbi tábla került felmentésre: {file_path}")
        logger.info(f"[OOB]: A feltöltött tábla {time_stamp} dátumbélyeggel készült")
    except Exception as e:
        import traceback
        err_msg = f"Hiba lépett fel a file feldolgzása közben: {e}"
        logger.error(err_msg)
        logger.error(traceback.format_exc())
        print(err_msg)
        print(traceback.format_exc())
        database.DatabaseManager.smtp_email_send(
                subject='[Failure] : OOB upload log',
                sender_mail='patrik.major@tesco.com',
                receiver_mail='patrik.major@tesco.com',
                cc_mail=None,
                html=f'OOB upload hibára futott: {err_msg}'
            )
        