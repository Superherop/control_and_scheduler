# batch_uploader.py elejére
import sys, os, time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core import utils, database, paths
import pandas as pd

def check_validity():
    """Ellenőrzi, hogy az adatbázisban a max fiscal_week megegyezik-e az aktuálissal."""
    query = "SELECT max(fiscal_week) AS max_fw FROM tesco_analysts.pmajor1_enh_batch"
    result_df = database.DatabaseManager.run_query(query)
    if result_df is None or result_df.empty:
        return False
    latest_fw_in_db = result_df["max_fw"].iloc[0]
    current_fw = database.get_current_tesco_week()
    return current_fw == latest_fw_in_db

def run(check_need=True, manual_input=None):
    logger = utils.setup_logger("BatchUploader", log_file=os.path.join(paths.LOG_PATH, "batch.log"))
    if not manual_input:
        logger.info("[BATCH]: A report feldolgozás indul.")

        # Első próbálkozás: file keresése
        try:
            file_path = utils.list_excel_files(paths.BATCH_DIR, "Enhanced Batch Report")
            logger.info(f"[BATCH]: Megvan a mai fájl: {file_path}")
        except FileNotFoundError:
            logger.info("[BATCH]: A legfrissebb fájl nem mai vagy nem található, várakozás...")
            file_path = utils.wait_until_file_ready(paths.BATCH_DIR, "Enhanced Batch Report")
    else:
        print(f"[BATCH]:  Working from manual input: {manual_input}")
        logger.info(f"[BATCH]:  Working from manual input: {manual_input}")
        file_path=manual_input
    # Feldolgozás csak a tényleges, mai fájlra
    try:
        df = utils.read_and_process_file(
            file_path=file_path,sheet_name= 'Stock On Hand', columns=[
            'LOC', 'Master TPN', 'Single Picking Unit', 'Stock_on_hand', 'Free stock',
            'Total Available Stock ', 'Total NON_Sellable', 'Cost_conv', 'first_received', 
            'Last_received', 'Pick_Dates', 'Tsf_reserved_qty', 'In_transit_qty',
            'Case_supp_pack_size', 'Case_stock_on_hand', 'Case_in_transit_qty'
            ], header=0)

        df['fiscal_week'] = database.get_current_tesco_week()
        df.rename(columns={'total_available_stock_': 'total_available_stock'}, inplace=True)
        df['Master TPN'] = pd.to_numeric(df['Master TPN'], errors='coerce').astype('Int64')
        time_stamp = utils.creation_date(file_path)
        output_path = os.path.join(paths.HDP_PATH, "pmajor1_enh_batch.csv")
        df.to_csv(output_path, index=False, sep="|")

        if check_need:
            logger.info("[BATCH]: Érvényességi ellenőrzés indul...")
            while not check_validity():
                logger.warning("[BATCH]: Report még nem érvényes, újraellenőrzés 10 perc múlva...")
                time.sleep(600)
            database.DatabaseManager.smtp_email_send(
                subject='Batch upload log',
                sender_mail='patrik.major@tesco.com',
                receiver_mail='Zsolt.Kovacs@tesco.com',
                cc_mail='patrik.major@tesco.com',
                html=f'Batch upload sikeresen lefutott. A tábla {time_stamp} dátumbélyeggel kékszült.  A Hadoop-ban: tesco_analysts.pmajor1_enh_batch'
            )
    except Exception as e:
        import traceback
        err_msg = f"Hiba lépett fel a file feldolgzása közben: {e}"
        logger.error(err_msg)
        logger.error(traceback.format_exc())
        print(err_msg)
        print(traceback.format_exc())
        database.DatabaseManager.smtp_email_send(
                subject='[Failure] : Batch uploader log',
                sender_mail='patrik.major@tesco.com',
                receiver_mail='patrik.major@tesco.com',
                cc_mail=None,
                html=f'Batch uploader futott: {err_msg}'
            )