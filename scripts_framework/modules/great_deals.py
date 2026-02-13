from core import utils, database, paths
import os
import datetime
import pandas as pd

import sys


# --- PYTHONPATH fix: mindig felvesszük a scripts mappa szintjét ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

#test output path:
test_path = paths.TEST_PATH

def run():
    logger = utils.setup_logger("GreatDeals", log_file=os.path.join(paths.LOG_PATH, "great_deals.log"))
    logger.info("Great Deals modul indul.")
    GD_BASIC_TABLE_PATH = os.path.join(paths.GD_PATH,"BasicTable.xlsx")
   
    sheets = pd.ExcelFile(GD_BASIC_TABLE_PATH)

    dummy_query = """SELECT 
        '{phase}' as phase,
        rep.cntr_code AS `country`,
        store.dmst_store_code as store_number,
        store.dmst_store_des as store_name,
        case when store.slsp_region in ('SuperMarkets','Express') then 'SM'
             when store.slsp_region in ('2-4K (CHM)') then 'CHM'
             when store.slsp_region in ('5K+') then 'HM' END as store_format,
       t.dmtm_fw_code AS `fiscal_week`,
       t.dmtm_d_code AS `day`,
       t.DTDW_DAY_DESC_EN AS `day_name`,
       rep.dmat_dep_des as department_name,
       rep.slad_tpn AS `tpn`,
       rep.slad_long_des AS `description`,
       round(sum(sales.slsms_salex_cs/rate.dmexr_rate), 3) AS `sales_excl_vat_gbp`,
       ROUND(SUM(sales.slsms_margin / rate.dmexr_rate),3) AS scan_margin_gbp,
       round(sum(sales.slsms_margin_cons / rate.dmexr_rate), 3) as cons_scan_margin,
       sum(sales.slsms_unit) AS `sold_unit`,
       sum(sales.slsms_stock_unit) AS `stock_unit`,
       round(sum(sales.slsms_stock_val/rate.dmexr_rate),3) AS `stock_value_gbp`
FROM dw.sl_sms sales
JOIN dm.dim_time_d t ON sales.part_col = t.dmtm_d_code
JOIN dm.dim_artrep_details rep ON sales.slsms_dmat_id = rep.slad_dmat_id
AND sales.slsms_cntr_id = rep.cntr_id
JOIN dm.dim_stores store ON sales.slsms_dmst_id = store.dmst_store_id
AND sales.slsms_cntr_id = store.cntr_id

JOIN
  (SELECT dmexr_cntr_id,
          dmexr_rate
   FROM dw.dim_exchange_rates
   WHERE dmexr_dmtm_fy_code LIKE (SELECT fiscal_year FROM tesco_analysts.pmajor1_fiscal_year_for_filter)
     AND dmexr_crncy_to = 'GBP'
     AND dmexr_cntr_id < 5) rate ON sales.slsms_cntr_id = rate.dmexr_cntr_id
WHERE rep.slad_tpn IN ({tpn_list})
  AND store.dmst_store_code IN ({store_list})
  AND dmtm_d_code BETWEEN replace('{start_date}','-','') AND replace(current_date,'-','')
  AND rep.dmat_div_code LIKE '0004'
  AND rep.cntr_code NOT LIKE 'PL'
GROUP BY rep.cntr_code,
         store.dmst_store_code,
         store.dmst_store_des,
         store.slsp_region,
         t.dmtm_fw_code,
         t.dmtm_d_code,
         t.DTDW_DAY_DESC_EN,
         rep.slad_tpn,
         rep.slad_long_des,
         rep.dmat_dep_des"""

  
    queries = []

    for sn in sheets.sheet_names:
        if sn.startswith('phase'):
            t = utils.excel_tpn_integer_konverzio(GD_BASIC_TABLE_PATH, sheet_name=sn, konvertalas=False)
            t1 = t.start_date.dropna().unique()
            date_str = t1[0].strftime('%Y-%m-%d') if len(t1) > 0 else None
            tpn = ",".join(f"'{x}'" for x in t.TPN.dropna().unique())
            store = ",".join(f"'{x}'" for x in t.store_list.dropna().unique().astype(int).astype(str))
            q = dummy_query.format(phase=sn, tpn_list=tpn, store_list=store, start_date=date_str)
            queries.append(q)

    final_query = "\nUNION ALL\n".join(queries)
    base = database.DatabaseManager.run_query(final_query)

    col_name = ['Phase','Country','Store Nr','Store Name','Store Format','Fiscal Week','Date','Day','Department','TPN','Product Description','Sales','Scan Margin','Scan Margin Cons','Sold Unit','Stock Unit','Stock Value']
    base.columns = col_name
    hu_prod_desc = base.query('Country == "HU"')[['TPN','Product Description']].drop_duplicates()
    merged = base.merge(hu_prod_desc, on='TPN', suffixes=('_en',' hu'))
    merged.drop(['Product Description_en'], axis=1, inplace=True)
    merged.rename(columns={'Product Description hu':'Product Description'}, inplace=True)


    merged.to_csv(os.path.join(paths.GD_PATH, "test_output_from_v2.csv"), index=False)


    max_day = str(merged.Date.max())
    day, month, year = max_day[-2:], max_day[4:-2], max_day[:4]
    logger.info(f"Great Deals up to date: {day}-{month}-{year}")
    database.DatabaseManager.smtp_email_send(sender_mail='patrik.major@tesco.com',cc_mail=None,receiver_mail='patrik.major@tesco.com',subject='Great Deals',html=f'The great deals dataset is up to date {day}-{month}-{year}')

#'Krisztina.Kiss-Vida2@tesco.com'