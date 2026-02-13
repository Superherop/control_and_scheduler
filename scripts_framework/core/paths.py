import os

def _env_var(name, default=""):
    
    return os.getenv(name) or default

def get_path(key: str) -> str:
   
    onedrive = _env_var("OneDrive", r"C:\Users\pmajor1\OneDrive - Tesco")
    sharepoint = _env_var("REPORT_OUTPUT_SHAREPOINT", fr"{onedrive}\Reporting - Documents")
    sharedrive = _env_var("ShareDrive", r"\\EUPRGVMFS01\GMforCentralEurope")

    paths_map = {
        "OOB_SOURCE": fr"{sharedrive}\Supply Chain\OOB",
        "BATCH_DIR": fr"{sharedrive}\CE Reports\Batch Report",
        "HDP_PATH":  r"\\euprgvmfs01.tesco-europe.com\Hadoop Ingest\User Load\public",
        "LOG_PATH": fr"{onedrive}\Business Planning\automatization\ControlPanel_script_runner\framework_logs",
        "ASSETS_PATH": fr"{onedrive}\Business Planning\automatization\ControlPanel_script_runner\framework_assets",
        "ASSETS_TIME_TABLE": fr"{onedrive}\Business Planning\automatization\ControlPanel_script_runner\framework_assets\time_table.csv",
        "GD_PATH": fr"{sharepoint}\GreatDeals",
        "TEST_PATH": fr"{onedrive}\Business Planning\automatization\ControlPanel_script_runner\framework_logs\test_path",
        "BLACK_FRIDAY_PATH": fr"{sharepoint}\Black Firday tracker",
        "BOMB_CHECKER_PATH": fr"{sharepoint}\1P Checkers",
        "OFD_TRACKER_PATH": fr"{sharepoint}\OFD Tracker",
        "DAILY_SALES_BY_DEP" : fr"{sharepoint}\DailySalesbyDep",
        "headline_path": fr"{onedrive}\Business Planning\automatization\GM Weekly Book\headline_comments.xlsx",
        "ONEDRIVE": onedrive,
        "SHAREPOINT": sharepoint,
        "SHAREDRIVE": sharedrive,    
        }
    return paths_map.get(key)

class _LazyPaths:
    
    def __getattr__(self, name):
        value = get_path(name)
        if value is None:
            raise AttributeError(f"Nincs ilyen path kulcs: {name}")
        return value


paths = _LazyPaths()


OOB_SOURCE = get_path("OOB_SOURCE")
BATCH_DIR = get_path("BATCH_DIR")
HDP_PATH = get_path("HDP_PATH")
LOG_PATH = get_path("LOG_PATH")
ASSETS_PATH = get_path("ASSETS_PATH")
ASSETS_TIME_TABLE = get_path("ASSETS_TIME_TABLE")
GD_PATH = get_path("GD_PATH") #("TEST_PATH") 
TEST_PATH = get_path("TEST_PATH")
BLACK_FRIDAY_PATH = get_path("BLACK_FRIDAY_PATH")
BOMB_CHECKER_PATH = get_path("BOMB_CHECKER_PATH")
OFD_TRACKER_PATH = get_path("OFD_TRACKER_PATH")
DAILY_SALES_BY_DEP = get_path("DAILY_SALES_BY_DEP")
headline_path = get_path("headline_path")
ONEDRIVE = get_path("ONEDRIVE")
SHAREPOINT = get_path("SHAREPOINT")
SHAREDRIVE = get_path("SHAREDRIVE")
