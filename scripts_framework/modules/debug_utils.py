import os
import sys
from core import paths

def print_env_info(to_file=None):
    """
    Kiírja a környezeti információkat, working dir-t, sys.path-t, fontos env változókat és paths.py értékeket.
    Ha 'to_file' útvonalat adunk meg, akkor oda menti a kimenetet.
    """
    output_lines = []
    output_lines.append("\n=== DEBUG ENV INFO START ===")
    output_lines.append(f"CWD: {os.getcwd()}")
    output_lines.append(f"sys.argv: {sys.argv}")
    output_lines.append(f"sys.executable: {sys.executable}")
    output_lines.append(f"sys.path: {sys.path}")
    output_lines.append("Environment variables relevant:")
    for var in ["OneDrive", "REPORT_OUTPUT_SHAREPOINT", "ShareDrive"]:
        output_lines.append(f"  {var} = {os.getenv(var)}")
    output_lines.append("Paths from paths.py:")
    for key in [
        "OOB_SOURCE", "BATCH_DIR", "HDP_PATH", "LOG_PATH",
        "ASSETS_PATH", "ASSETS_TIME_TABLE",
        "GD_PATH", "TEST_PATH", "BLACK_FRIDAY_PATH",
        "BOMB_CHECKER_PATH", "OFD_TRACKER_PATH"
    ]:
        try:
            output_lines.append(f"  {key} = {getattr(paths, key)}")
        except Exception as e:
            output_lines.append(f"  {key} = <ERROR: {e}>")
    output_lines.append("=== DEBUG ENV INFO END ===\n")

    # Kiírás fájlba vagy a standard outputra
    if to_file:
        with open(to_file, "w", encoding="utf-8") as f:
            f.write("\n".join(map(str, output_lines)))
    else:
        print("\n".join(map(str, output_lines)))