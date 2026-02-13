import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core import utils, paths
from datetime import datetime

import pandas as pd
from core import data_processing as dp

modul_name = __name__

def run():
    logger = utils.setup_logger("OFDTracker", log_file=os.path.join(paths.LOG_PATH, "ofd_tracker.log"))
    logger.info("OFD tracker indul.")

    today = datetime.today().strftime('%Y-%m-%d')
    data_path = r"C:\Users\pmajor1\OneDrive - Tesco\Business Planning\automatization\OFD_tracker\OFD database to dashboard.xlsx"



    # Eredeti feldolgozó logika
    base_data = dp.GetData(data_path)
    result = base_data.get_data()
    data = base_data.data
    data.drop(columns=['CE'], inplace=True)
    m = data.melt(id_vars=['TPNB','Department','OFD name','Season','Year From','Week From','Year To','Week To'], value_vars=['CZ','HU','SK'], var_name='country', value_name='OFD_nr')

    def datacleaning(df):
        df['Year From'] = df['Year From'].astype(str)
        df['Week From'] = df['Week From'].astype(str)
        df['Week To'] = df['Week To'].astype(str)
        df['Year To'] = df['Year To'].astype(str)
        df['fiscal_week_from'] = "f"+ df['Year From'] +"w"+ df['Week From']
        df['fiscal_week_to'] = "f"+ df['Year To'] +"w"+ df['Week To']
        return df

    def aggregate_sales(data, result, metrics):
        """
        Összesíti a megadott metrikák értékeit a result táblából, és hozzáadja a data táblához.
        
        Parameters:
            data (pd.DataFrame): A data tábla, amely tartalmazza a TPNB, fiscal_week_from és fiscal_week_to oszlopokat.
            result (pd.DataFrame): A result tábla, amely tartalmazza a TPNB, fw_code és a megadott metrikák oszlopait.
            metrics (list): A metrikák listája, amelyeket összesíteni kell (pl. ['Sales', 'Margin']).

        Returns:
            pd.DataFrame: A data tábla új oszlopokkal, amelyek tartalmazzák az összesített metrikák értékeit.
        """
        # Inicializáljuk az új oszlopokat a data táblában
        for metric in metrics:
            data[f'{metric}'] = 0

        # Iterálás a data tábla sorain
        for index, row in data.iterrows():
            country = row['country']
            tpn = row['TPNB']
            start = row['fiscal_week_from']
            end = row['fiscal_week_to']
            
            # Szűrés a result táblában
            filtered_sales = result[
                (result['country'] == country) &  # A country oszlop minden értéknek egyezik
                (result['TPNB'] == tpn) &
                (result['fw_code'] >= start) &
                (result['fw_code'] <= end)
            ]
            
            # Összesítés minden metrikára
            for metric in metrics:
                total_value = filtered_sales[metric].sum()
                data.at[index, f'{metric}'] = total_value

        return data

    data_clean = datacleaning(m)

    result['Margin'] = result['Margin'] + result['Margin_Consignment']
    metrics = ['Sales','Margin']
    aggregated_data = aggregate_sales(data_clean, result, metrics)

    names = result[['TPNB','country','product_desc_en','product_description']].drop_duplicates()
    final_data = pd.merge(aggregated_data, names, on=['TPNB','country'], how='left')

    final_data['refresh_date'] = today
    final_data.to_csv(os.path.join(paths.HDP_PATH, "pmajor1_ofd_tracker_data.csv"), index=False)
    logger.info("OFD tracker kész és mentve.")
