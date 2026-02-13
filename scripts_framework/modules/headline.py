
import pandas as pd
import os
from core import utils, paths, database

import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


def run():
    logger = utils.setup_logger("Headline", log_file=os.path.join(paths.LOG_PATH, "headline.log"))
    logger.info("Black Friday modul indul.")
    hl = pd.read_excel(paths.headline_path)
    hl.rename(columns={hl.columns[1]: 'char (max 75 chars)'}, inplace=True)
    hl.to_csv(os.path.join(paths.HDP_PATH, "pmajor1_headline_for_weekly_book.csv"), sep='|')
    logger.info('Headline uploaded')