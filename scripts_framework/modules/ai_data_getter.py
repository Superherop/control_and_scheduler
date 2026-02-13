#Getting data for AI
import sys, os
from core import utils, database, paths
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

MODUL_NAME = __name__

from core import database as db
logger = utils.setup_logger('AIdataGetting',log_file=os.path.join(paths.LOG_PATH, 'aidatagetting.log'))
logger.info(f'[{MODUL_NAME}] module started')
'''
Need a follow periods
wtd = Actual week wtih daily data
ptd = Actual period with weekly data
ytd = Actual financial year with period level
'''

def query_builder(period_key):
    """
    Adott period_key alapján SQL lekérdezést épít.
    A period_key lehet: 'wtd_set', 'ptd_set', 'ytd_set'.
    Feltételezi, hogy a db.* függvények elérhetők:
      - db.get_current_tesco_week()
      - db.get_current_tesco_period()
      - db.get_fyear_start_period()
    """

    # A SELECT/GROUP BY oszlopok igazítva a CTE-hez (nap -> t.nap)
    period_sets = {
        'wtd_set': {
            'cols': 't.nap AS day',
            'interval_template': "dmtm_fw_code = '{current_week}'",
            'group': 't.nap'
        },
        'ptd_set': {
            'cols': 't.dmtm_fw_code AS fiscal_week',
            'interval_template': "dmtm_fp_code = '{current_period}'",
            'group': 't.dmtm_fw_code'
        },
        'ytd_set': {
            'cols': 't.dmtm_fp_code AS fiscal_period',
            'interval_template': "dmtm_fp_code BETWEEN '{start_period}' AND '{end_period}'",
            'group': 't.dmtm_fp_code'
        }
    }

    if period_key not in period_sets:
        raise ValueError("Invalid period key. Választható: 'wtd_set', 'ptd_set', 'ytd_set'.")

    # --- Dátum/period változók előállítása a period_key alapján ---
    current_week = None
    current_period = None
    start_period = None
    end_period = None

    match period_key:
        case 'wtd_set':
            current_week = db.get_current_tesco_week()
        case 'ptd_set':
            current_period = db.get_current_tesco_period()
        case 'ytd_set':
            start_period = db.get_fyear_start_period()
            end_period = db.get_current_tesco_period()
        case _:
            raise ValueError(f"Ismeretlen period_key: {period_key}")

    # --- Az intervallum feltétel sablonjának formázása ---
    interval_template = period_sets[period_key]['interval_template']

    if period_key == 'wtd_set':
        if current_week is None:
            raise ValueError("current_week nincs beállítva.")
        interval_sql = interval_template.format(current_week=current_week)

    elif period_key == 'ptd_set':
        if current_period is None:
            raise ValueError("current_period nincs beállítva.")
        interval_sql = interval_template.format(current_period=current_period)

    else:  # 'ytd_set'
        if start_period is None or end_period is None:
            raise ValueError("start_period / end_period nincs beállítva.")
        interval_sql = interval_template.format(start_period=start_period, end_period=end_period)

    # --- SELECT/GROUP BY mezők ---
    cols = period_sets[period_key]['cols']
    group = period_sets[period_key]['group']
    logger.info(f'[{period_key}] set up with {interval_sql} period')
    # --- SQL összeállítása ---
    query = f"""
    SELECT
        rep.cntr_code AS `country`,
        {cols},
        rep.dmat_div_code AS `division`,
        rep.dmat_div_des AS `division_name`,
        rep.dmat_dep_code AS `department`,
        rep.dmat_dep_des AS `department_name`,
        rep.dmat_sec_code AS `section`,
        rep.dmat_sec_des AS `section_name`,
        rep.dmat_grp_code AS `group`,
        rep.dmat_grp_des AS `group_name`,

        rc.rc_cat_name AS rpc_category,

        /* --- Alap metrikák (TY/LY) --- */
        COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ty`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ty`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ty`,

        COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ly`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ly`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ly`,

        /* --- LFL metrikák (TY/LY) --- */
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_lfl`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_lfl`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_lfl`,

        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_lflb`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_lflb`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_lfl_flag = 1
                            THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_lflb`,

        /* --- 2Y LFL metrikák (opcionális) --- */
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_ty_2ylfl`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_ty_2ylfl`,
        COALESCE(SUM(CASE WHEN t.ev = 'ty' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_ty_2ylfl`,

        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_salex_cs / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `sales_excl_vat_gbp_2ylflb`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_margin   / NULLIF(rate.dmexr_rate, 0) ELSE 0 END), 0) AS `scan_margin_gbp_2ylflb`,
        COALESCE(SUM(CASE WHEN t.ev = 'ly' AND sales.slsms_2ylfl_flag = 1
                            THEN sales.slsms_unit_cs                              ELSE 0 END), 0) AS `sold_unit_2ylflb`

    FROM dm.dim_artrep_details rep

    JOIN dw.sl_sms sales
      ON sales.slsms_dmat_id = rep.slad_dmat_id
     AND sales.slsms_cntr_id IN (1, 2, 4)
     AND sales.slsms_cntr_id = rep.cntr_id

    /* Opcionális store join:
    JOIN dm.dim_stores store
      ON sales.slsms_dmst_id = store.dmst_store_id
     AND sales.slsms_cntr_id = store.cntr_id
    */

    JOIN (
        SELECT
            'ty' AS ev,
            dmtm_fw_weeknum AS het,
            dmtm_d_code     AS nap,
            dmtm_fw_code,
            dmtm_fp_code
        FROM dm.dim_time_d
        WHERE {interval_sql}

        UNION
        SELECT
            'ly' AS ev,
            dmtm_fw_weeknum       AS het,
            dmtm_d_code_ly_offset AS nap,
            dmtm_fw_code,
            dmtm_fp_code
        FROM dm.dim_time_d
        WHERE {interval_sql}
    ) t
      ON t.nap = sales.part_col

    JOIN (
        SELECT dmexr_cntr_id, dmexr_rate
        FROM dw.dim_exchange_rates
        WHERE dmexr_dmtm_fy_code = (SELECT fiscal_year FROM tesco_analysts.pmajor1_fiscal_year_for_filter)
          AND dmexr_crncy_to = 'GBP'
          AND dmexr_cntr_id IN (1, 2, 4)
    ) rate
      ON sales.slsms_cntr_id = rate.dmexr_cntr_id

    JOIN (
        SELECT dmrrc_cntr_id, dmrrc_code_id, rc_cat_name
        FROM dw.dim_retail_rc
        JOIN dm.dim_rc_ret_category ON rc_cat_id = dmrrc_rc_cat_id
        GROUP BY dmrrc_cntr_id, dmrrc_code_id, rc_cat_name
    ) rc
      ON sales.slsms_cntr_id = rc.dmrrc_cntr_id
     AND sales.slsms_nrc     = rc.dmrrc_code_id

    WHERE rep.cntr_code IN ('HU','CZ','SK')
    and rep.dmat_div_code = 0004

    GROUP BY
        {group},
        rep.cntr_code,
        rep.dmat_div_code,
        rep.dmat_div_des,
        rep.dmat_dep_code,
        rep.dmat_dep_des,
        rep.dmat_sec_code,
        rep.dmat_sec_des,
        rep.dmat_grp_code,
        rep.dmat_grp_des,
        rc.rc_cat_name;
    """
    return query


def run():
    period_keys = ['wtd_set', 'ptd_set', 'ytd_set']
    for key in period_keys:
        base = None
        MAX_TRIALS = 5
        trials = 0
        final_query = query_builder(key)
        while (base is None or (hasattr(base, 'empty') and base.empty)) and trials < MAX_TRIALS:
            logger.info(f"[INFO] - A lekérdezés elindult (Kísérlet: {trials + 1}/{MAX_TRIALS})")
            try:
                base = db.DatabaseManager.run_query(final_query)
                if base is not None and not (hasattr(base, 'empty') or base.empty):
                    logger.info('[INFO] - A lekérdezés készen van és sikerese adatokat hozott')
                    break
                logger.info('[ERROR] - Nincs adat a táblában')
            except Exception as e:
                import traceback
                err_msg = f"[ERROR : Hiba lépett fel a csatlakozás során: {e}]"
                logger.error(err_msg)
                logger.error(traceback.format_exc())
                print(traceback.format_exc())
            trials += 1
            if trials < MAX_TRIALS:
                import time
                time.sleep(180)

        if base is None or (hasattr(base, 'empty') or base.empty):
            final_err_msg =f"[FATAL] - A lekérdezés {MAX_TRIALS} kísérlet után sem sikerült, vagy üres a riport"
            logger.error(final_err_msg)
            db.DatabaseManager.smtp_email_send(subject=f"[ERROR] : {MODUL_NAME}",
                                sender_mail="Patrik.Major@tesco.com",
                                receiver_mail="Patrik.Major@tesco.com",
                                cc_mail=None,
                                html=f"The {MODUL_NAME} table is not ready to use! error: {final_err_msg}",
                                bcc_mail=None)
        else:
            base.to_csv(fr'C:\Users\pmajor1\OneDrive - Tesco\Business Planning\automatization\AI summarization\Daily summary\output\{key}_data.csv')
            logger.info(f"{MODUL_NAME} up to date")
            database.DatabaseManager.smtp_email_send(subject=f"{MODUL_NAME} is up to date",
                                                    sender_mail="patrik.major@tesco.com",
                                                    receiver_mail="patrik.major@tesco.com",
                                                    cc_mail=None,
                                                    html=f"The {MODUL_NAME} table is ready to use!",
                                                    bcc_mail=None)
            logger.info("{MODUL_NAME} modul kész. Email elküldve.")

