import pandas as pd
from core import utils, paths
from datetime import datetime
import os
import sys
import pandas as pd
from core import data_processing as dp


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


head_log = pd.read_csv()