import sys
import os

#from scripts_framework.modules import debug_utils

# --- PYTHONPATH fix: mindig felvesszük a scripts mappa szintjét ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core import utils, database, paths
import pandas as pd
import os

MODULE_NAME = __name__

def run():
    #debug_utils.print_env_info()
    logger = utils.setup_logger("Daily Sales by dep", log_file=os.path.join(paths.LOG_PATH, "daily_sales_by_dep.log"))
    logger.info("Daily Sales by dep modul indul.")

    dummy_query = """   
            WITH fiscal_year AS (
        SELECT fiscal_year FROM tesco_analysts.pmajor1_fiscal_year_for_filter
    ),
    filtered_time AS (
        SELECT 
            dmtm_d_code,
            dmtm_fw_code,
            dmtm_fp_code
        FROM dm.dim_time_d
        WHERE dmtm_d_code > '20171231'
    ),
    rate AS (
        SELECT dmexr_cntr_id, dmexr_rate
        FROM dw.dim_exchange_rates
        WHERE dmexr_dmtm_fy_code = (SELECT fiscal_year FROM fiscal_year)
        AND dmexr_crncy_to = 'GBP'
        AND dmexr_cntr_id < 5
    ),
    filtered_artrep AS (
        SELECT *
        FROM dm.dim_artrep_details
        WHERE dmat_div_code = 0004
        and slad_status = 'A'
    )
    SELECT
        rep.cntr_code as country,
        t.dmtm_d_code AS exact_date,
        t.dmtm_fw_code AS fiscal_week,
        t.dmtm_fp_code AS fiscal_period,
        rep.dmat_dep_des AS department_name,
        -- Kerekítés hozzáadva itt:
        ROUND(COALESCE(SUM(sales.slsms_salex_cs / rate.dmexr_rate),0), 3) AS sales_excl_vat_gbp,
        ROUND(coalesce(SUM(sales.slsms_margin / rate.dmexr_rate),0), 3) AS scan_margin_gbp,
        SUM(sales.slsms_unit) AS sold_unit,
        -- És itt is:
        ROUND(coalesce(SUM(CASE WHEN sales.slsms_margin_cons IS NOT NULL THEN sales.slsms_salex_cs / rate.dmexr_rate ELSE 0 END), 0), 3) AS cons_sales_excl_vat_gbp,
        ROUND(coalesce(SUM(sales.slsms_margin_cons / rate.dmexr_rate), 0), 3) AS cons_scan_margin,
        SUM(CASE WHEN sales.slsms_margin_cons IS NOT NULL THEN sales.slsms_unit ELSE 0 END) AS cons_sold_unit
    FROM filtered_artrep rep
    LEFT JOIN dw.sl_sms sales
        ON sales.slsms_dmat_id = rep.slad_dmat_id
        AND sales.slsms_cntr_id = rep.cntr_id
    JOIN filtered_time t
        ON t.dmtm_d_code = sales.part_col
    LEFT JOIN rate
        ON rate.dmexr_cntr_id = rep.cntr_id
    GROUP BY
        rep.cntr_code,
        t.dmtm_d_code,
        t.dmtm_fw_code,
        t.dmtm_fp_code,
        rep.dmat_dep_des;

        """
    base = None
    MAX_TRIALS = 5
    trials = 0
    while (base is None or (hasattr(base, 'empty') and base.empty)) and trials < MAX_TRIALS: 
        try:
            logger.info(f"[INFO] : A lekérdezés elindult (Kísérlet: {trials + 1}/{MAX_TRIALS})")
            base = database.DatabaseManager.run_query(dummy_query)
            if base is not None and not (hasattr(base, 'empty') and base.empty):
                logger.info("[INFO] : A lekérdezés készen van és sikeresen adatokat hozott.")
                break 
            logger.info("[ERROR] : Nincs adat a táblában")
        except Exception as e:
            import traceback
            err_msg = f"[ERROR] : Hiba lépett fel a csatlakozás során: {e}"
            logger.error(err_msg)
            logger.error(traceback.format_exc())
            print(err_msg)
            print(traceback.format_exc())
        trials += 1
        if trials < MAX_TRIALS:
            import time
            time.sleep(180)
    
    if base is None or (hasattr(base, 'empty') and base.empty):
        final_err_msg = f"[FATAL] : A lekérdezés {MAX_TRIALS} kísérlet után sem sikerült, vagy üres eredményt adott."
        logger.error(final_err_msg)
        database.DatabaseManager.smtp_email_send(subject=f"[ERROR] : Daily Sales by dep",
                                            sender_mail="Patrik.Major@tesco.com",
                                            receiver_mail="Patrik.Major@tesco.com",
                                            cc_mail=None,
                                            html=f"The Daily Sales by dep background table is not ready to use! error: {final_err_msg}",
                                            bcc_mail=None)
        print(final_err_msg)
    else:
        logger.info("[SUCCESS] : A lekérdezés sikeres volt, folytatom az adatok feldolgozásával.")


    
    output_path = os.path.join(paths.DAILY_SALES_BY_DEP,"dail_sales_by_dep.csv")
    base.to_csv(output_path, index=False)
    logger.info(f"daily_sales_by_dep CSV mentve: {output_path}")

    max_day = str(base.exact_date.max())
    day, month, year = max_day[-2:], max_day[4:-2], max_day[:4]
    logger.info(f"Daily Sales by dep is up to date: {day}-{month}-{year}")
    database.DatabaseManager.smtp_email_send(subject=f"Daily Sales by dep is up todate {year}-{month}-{day}",
                                            sender_mail="Patrik.Major@tesco.com",
                                            receiver_mail="Regina.Nagy@tesco.com",
                                            cc_mail="patrik.major@tesco.com",
                                            html="The Daily Sales by dep background table is ready to use!",
                                            bcc_mail=None)
    logger.info("Daily Sales by dep modul kész. Email elküldve.")
