

import pandas as pd
from core import database, paths, utils
import os


base = None


logger = utils.setup_logger("OFDTracker", log_file=os.path.join(paths.LOG_PATH, "ofd_tracker.log"))

class BasicProcessing:
    def __init__(self, data_path):
        self.data_path = data_path
        self.data = pd.read_excel(data_path, usecols='A:L')
        self.start_year = self.data['Year From'].min()
        self.end_year = self.data['Year To'].max()
        self.min_week = self.data['Week From'].min()
        self.max_week = self.data['Week To'].max()
        self.tpn_list = tuple(int(tpn) for tpn in self.data['TPNB'].unique())

class GetData(BasicProcessing):
    def week_normalizer(self):
        print(f"Min week: {self.min_week}, Max week: {self.max_week}, start_year: {self.start_year}, end_year: {self.end_year}")
        if self.min_week < 10:
            self.min_week = '0' + str(self.min_week)
            self.min_week = f"f{self.start_year}w{self.min_week}"
        else:
            self.min_week = f"f{self.start_year}w{str(self.min_week)}"

        if self.max_week < 10:
            self.max_week = '0' + str(self.max_week)
            self.max_week = f"f{self.end_year}w{self.max_week}"
        else:
            self.max_week = f"f{self.end_year}w{str(self.max_week)}"

        return self.min_week, self.max_week

    def get_data(self, base=None):
        self.start_week, self.end_week = self.week_normalizer()

        placeholders = ', '.join(['?'] * len(self.tpn_list))

        query = f"""
            SELECT 
                cntr_code as country,
                fw_code,
                tpnb as TPNB,
                product_desc_en,
                product_description,
                sum(margin_ty) as Margin,
                sum(margin_ty_consignment) as Margin_Consignment,
                sum(sales_ty) as Sales,
                sum(units_ty) as Units
            FROM tesco_analysts.pmajor1_ytd_weekly_tpn_new
            WHERE tpnb IN ({placeholders}) 
            AND fw_code BETWEEN ? AND ?
            GROUP BY cntr_code,fw_code, tpnb, product_desc_en, product_description
        """

        parameters = tuple(self.tpn_list) + (self.start_week, self.end_week)
        #TODO: Do the request while won't true
        # success = False
        # while not success:
        #     try:
        #         result = 
        #     except:
        #         print('Something is not working')
        #     success == True
        trials = 0
        MAX_TRIALS = 5
        while (base is None or (hasattr(base, 'empty') and base.empty)) and trials < MAX_TRIALS:
            try:
                logger.info("Az ofd tracker lekérdezés elindult")
                base =  database.DatabaseManager().run_query(query, parameters)
                break
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
                time.sleep(3) 
        if base is None or (hasattr(base, 'empty') and base.empty):
            logger.error(f"[ERROR] a tábla üresen érkezett vissza")
        else:
            return base