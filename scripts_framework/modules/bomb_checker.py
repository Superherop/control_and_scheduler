from core import utils, database, paths
import os
import pandas as pd

MODULE_NAME = __name__

def run():
    logger = utils.setup_logger("BombChecker", log_file=os.path.join(paths.LOG_PATH, "bomb_checker.log"))
    logger.info("Bomb Checker indul.")

    query = """SELECT 
        Country,
        Promo_Code,
        Promo_Name,
        Promo_Start,
        Promo_End,
        TPN,
        RMS_description,
        RMS_description_en,
        CBM,
        Buyer,
        CD_1P,
        CBM_1P,
        BUY_1P,
        Promo_Offer_Name,
        Tesco_Week,
        Promo_mechanics,
        Normal_retail_price,
        Promo_unit_price,
        Final_price,
        Final_CC_price,
        Bomb_1_price,
        Bomb_2_price,
        Bomb_3_price,
        Bomb_4_price,
        Bomb_5_price,
        Bomb_6_price,
        bomb_1_from,
        bomb_2_from,
        bomb_3_from,
        bomb_4_from,
        bomb_5_from,
        bomb_6_from,
        comment,
        Fiscal_Week,
        Fiscal_Period,
        p1fcd_corrected_promo_price


        FROM com_analysts.view_1p_data
        WHERE div_code = 0004
        AND Promo_start >= '2025-10-27'
        and Promo_end <= '2026-01-10'"""
    logger.info("Elindul a lekérdezés")
    try:
        base = database.DatabaseManager.run_query(query)
    except Exception as e:
        import traceback
        err_msg = f"[ERROR] base tábla létrehozása nem sikerült az alábbi hibából {e}"
        logger.error(err_msg)
        logger.error(traceback.format_exc())
        print(err_msg)
        print(traceback.format_exc())
    
    if (base is None or (hasattr(base, 'empty') and base.empty)):
        logger.error("A base dataframe nem tartalmaz adatot")
        database.DatabaseManager.smtp_email_send(subject=f"Failure in {MODULE_NAME} run!",
                                                 sender_mail='Patrik.Major@tesco.com',
                                                 receiver_mail='Patrik.Major@tesco.com',
                                                 html=f"A {MODULE_NAME} futása közben az alábbi hiba lépett fel:\n {err_msg}")
    else:
        def price_check():
            cols = [
                'Final_CC_price',
                'Bomb_1_price',
                'Bomb_2_price',
                'Bomb_3_price',
                'Bomb_4_price',
                'Bomb_5_price',
                'Bomb_6_price'
            ]
            
            change_counts = []
            
            for idx, row in base.iterrows():
                count = 0
                
                # páronként összehasonlítás (láncszerűen)
                for i in range(len(cols)-1):
                    price_a = row[cols[i]]
                    price_b = row[cols[i+1]]
                    
                    if (
                        pd.notna(price_a) and pd.notna(price_b) and
                        price_a != 0 and price_b != 0 and
                        price_a != price_b
                    ):
                        count += 1
                
                change_counts.append(count)
            
            base['Price_Change_Count'] = change_counts
            return base
    
    
        try:
            data = price_check()
        except Exception as e:
            import traceback
            err_msg = f"[ERROR] base tábla létrehozása nem sikerült az alábbi hibából {e}"
            logger.error(err_msg)
            logger.error(traceback.format_exc())
            print(err_msg)
            print(traceback.format_exc())
        data.to_csv(os.path.join(paths.BOMB_CHECKER_PATH, "bomb_checker.csv"))
        logger.info(f"A {MODULE_NAME} modul lefutott" )

    

