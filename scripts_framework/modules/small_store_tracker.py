import sys
import os
from datetime import datetime
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
    #Logger beállítása
    logger = utils.setup_logger(MODULE_NAME, log_file=os.path.join(paths.LOG_PATH, f"{MODULE_NAME}.log"))
    logger.info(f"{MODULE_NAME} modul indul.")

    #TPN és store lista beolvasása
    tpn_list = pd.read_excel(os.path.join(paths.small_store_tracker_path,"1k_stores.xlsx"),sheet_name="TPN_list")
    tpn_list = utils.conv(tpn_list, "TPN")
    tpns = tpn_list["Final TPN"]
    store_list = pd.read_excel(os.path.join(paths.small_store_tracker_path,"1k_stores.xlsx"),sheet_name="Store_list", usecols=["Áruház száma"])
    stores = store_list["Áruház száma"]
    print(type(tpns))
    print(type(store_list))

    #Query-k és változók beállítása

    dummy_query = """select
    rep.cntr_code as `country`,
    store.dmst_store_code as `store_number`,
    store.dmst_store_des as `store_name`,
    rep.dmat_div_code as `division`,
    rep.dmat_div_des as `division_name`,
    rep.dmat_dep_code as `department`,
    rep.dmat_dep_des as `department_name`,
    rep.dmat_sec_code as `section`,
    rep.dmat_sec_des as `section_name`,
    rep.dmat_grp_code as `group`,
    rep.dmat_grp_des as `group_name`,
    rep.dmat_sgr_code as `subgroup`,
    rep.dmat_sgr_des as `subgroup_name`,
    rep.slad_tpn as `tpn`,
    rep.slad_long_des as `description`,
    sum(case when t.ev = 'ty' then sales.slsms_salex_cs/rate.dmexr_rate else 0 end) as `sales_excl_vat_gbp_ty`,
    sum(case when t.ev = 'ty' then sales.slsms_margin/rate.dmexr_rate else 0 end) as `scan_margin_gbp_ty`,
    sum(case when t.ev = 'ty' then sales.slsms_unit_cs else 0 end) as `sold_unit_ty`,
    sum(case when t.ev = 'ly' then sales.slsms_salex_cs/rate.dmexr_rate else 0 end) as `sales_excl_vat_gbp_ly`,
    sum(case when t.ev = 'ly' then sales.slsms_margin/rate.dmexr_rate else 0 end) as `scan_margin_gbp_ly`,
    sum(case when t.ev = 'ly' then sales.slsms_unit_cs else 0 end) as `sold_unit_ly`

    from dm.dim_artrep_details rep


    join dw.sl_sms sales
    on sales.slsms_dmat_id = rep.slad_dmat_id
    and sales.slsms_cntr_id in (1,2,4)
    and sales.slsms_cntr_id = rep.cntr_id


    join dm.dim_stores store on sales.slsms_dmst_id = store.dmst_store_id and sales.slsms_cntr_id = store.cntr_id


    join (select
            'ty' as ev,
            dmtm_fw_weeknum as het,
            dmtm_d_code as nap,
            dmtm_fw_code
    from dm.dim_time_d
    where dmtm_d_code > '20260228'

    union
    select
        'ly' as ev,
        dmtm_fw_weeknum as het,
        dmtm_d_code_ly_offset as nap,
        dmtm_fw_code
        from dm.dim_time_d
        where dmtm_d_code > '20260228'
        ) t
    on t.nap = sales.part_col

    join (select
    dmexr_cntr_id,
    dmexr_rate
    from
    dw.dim_exchange_rates
    where
    dmexr_dmtm_fy_code = (select fiscal_year from tesco_analysts.pmajor1_fiscal_year_for_filter)
    and dmexr_crncy_to = 'GBP'
    and dmexr_cntr_id in (1,2,4) ) rate
    on sales.slsms_cntr_id = rate.dmexr_cntr_id


    where 
    rep.slad_tpn in ({tpn_list})    
    and store.dmst_store_code in({store_list})

    group by
    rep.cntr_code,
    store.dmst_store_code,
    store.dmst_store_des,
    rep.dmat_div_code,
    rep.dmat_div_des,
    rep.dmat_dep_code,
    rep.dmat_dep_des,
    rep.dmat_sec_code,
    rep.dmat_sec_des,
    rep.dmat_grp_code,
    rep.dmat_grp_des,
    rep.dmat_sgr_code,
    rep.dmat_sgr_des,
    rep.slad_tpn,
    rep.slad_long_des

    """



    #Adat leszedés a hadoop-ról
    queries = []
    base = None
    MAX_TRIALS = 5
    trials = 0

    tpn = ",".join(f"'{x}'" for x in tpns.dropna().unique())
    store = ",".join(f"'{x}'" for x in stores.dropna().unique().astype(int).astype(str))

    q = dummy_query.format(tpn_list=tpn, store_list=store)
    queries.append(q)
    final_query = q

    print(final_query)

    while (base is None or (hasattr(base, 'empty') and base.empty)) and trials < MAX_TRIALS:
        try:
            logger.info(f"[INFO] - A lekérdezés elindult (Kísérlet: {trials + 1}/{MAX_TRIALS})")
            base = database.DatabaseManager.run_query(final_query)
            if base is not None and not (hasattr(base, 'empty') and base.empty):
                logger.info("[INFO] - A lekérdezés készen van és sikeresen adatokat hozott.")
                break
            logger.info("[ERROR] - Nincs adat a táblában")
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


    #kiértékelés és exportálás

    if base is None or (hasattr(base, 'empty') and base.empty):
            final_err_msg = f"[FATAL] - A lekérdezés {MAX_TRIALS} kísérlet után sem sikerült, vagy üres a riport"
            logger.error(final_err_msg)
            """
            database.DatabaseManager.smtp_email_send(subject=f"[ERROR] : {MODULE_NAME} report",
                                        sender_mail="Patrik.Major@tesco.com",
                                        receiver_mail="Patrik.Major@tesco.com",
                                        cc_mail=None,
                                        html=f"The {MODULE_NAME} background table is not ready to use! error: {final_err_msg}",
                                        bcc_mail=None)
            """
    else:
        """
        col_name = ['Phase','Country','Store Nr','Store Name','Store Format','Fiscal Week','Date','Day','Department','TPN','Product Description','Sales','Scan Margin','Scan Margin Cons','Sold Unit','Stock Unit','Stock Value']
        base.columns = col_name
        # hu_prod_desc = base.query('Country == "HU"')[['TPN','Product Description']].drop_duplicates()
        # merged = base.merge(hu_prod_desc, on='TPN', suffixes=('_en',' hu'))
        # merged.drop(['Product Description_en'], axis=1, inplace=True)
        # merged.rename(columns={'Product Description hu':'Product Description'}, inplace=True)
        """
        output_path = os.path.join(paths.small_store_tracker_path,f"{MODULE_NAME}.csv")
        base.to_csv(output_path, index=False)
        logger.info(f"{MODULE_NAME} CSV mentve: {output_path}")

        max_day =datetime.now().strftime("%Y-%m-%d")
        year, month, day = max_day.split("-")
        logger.info(f"{MODULE_NAME} up to date: {day}-{month}-{year}")
        """
        database.DatabaseManager.smtp_email_send(subject=f"{MODULE_NAME} is up todate {year}-{month}-{day}",
                                                sender_mail="patrik.major@tesco.com",
                                                receiver_mail="Krisztina.Kiss-Vida2@tesco.com",
                                                cc_mail="patrik.major@tesco.com",
                                                html="The Black Friday background table is ready to use!",
                                                bcc_mail=None)
        logger.info(f"{MODULE_NAME} modul kész. Email elküldve.")
    """