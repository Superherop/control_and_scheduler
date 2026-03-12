import os
import logging
import time
import datetime
import pandas as pd
from core import database
import requests
from typing import Union

def analyse_financial_summaries(system_prompt, user_data, model='martain7r/finance-llama-8b:q4_k_m'):
    url = "http://localhost:11434/api/chat" # Fontos: a /api/chat végpontot használjuk!
    
    
    payload = {
        "model": "martain7r/finance-llama-8b:q4_k_m",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_data}
        ],
        "stream": False,
        "options": {
            "format": "json",
            "temperature": 0.15,
            "top_p": 0.9,
            "num_predict": 550,     # JSON-hoz általában elég
            "num_ctx": 4096,
            "seed": 42,
            "repeat_penalty": 1.18,
            "repeat_last_n": 256,
            "presence_penalty": 0.1,
            "frequency_penalty": 0.1,
            "stop": ["```"]         # ha a modell hajlamos kódfence-re
        }
}

    
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        # A válasz szerkezete itt kicsit más: message['content']
        return response.json().get("message", {}).get("content", "Nothing to analyze")
    else:
        return f"Error: {response.status_code} - {response.text}"

def setup_logger(name, log_file=None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # <<< a buborékolást megállítja

    # Multiprocessing öröklött handlerek törlése
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def time_check(creation):
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    return today == creation

def creation_date(path):
    creation_time = os.path.getctime(path)
    creation_date = datetime.datetime.fromtimestamp(creation_time)
    logging.info({'Creation date: ':creation_date})
    return creation_date.strftime("%Y-%m-%d")

def list_excel_files(directory, startswith):
    """
    Visszaadja az első, megfelelő Excel fájlt, és kilép a keresésből.
    Debug módban kiírja path-ot, creation_date-et és összehasonlítást a mai dátummal.
    """
    logger = logging.getLogger("ListExcelFiles")
    today_str = datetime.datetime.today().strftime('%Y-%m-%d')

    found_file = None
    found_creation = None

    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.xls', '.xlsx', '.xlsm', '.xlsb')) \
               and file.startswith(startswith) \
               and not file.startswith('~'):

                full_path = os.path.join(root, file)
                creation_str = datetime.datetime.fromtimestamp(
                    os.path.getctime(full_path)
                ).strftime('%Y-%m-%d')

                # Konzol/Jupyter debug
                # print(f"[DEBUG CHECK] Fájl: {full_path}")
                # print(f"              creation_date = {creation_str}")
                # print(f"              Összehasonlítás today_str = {today_str}")
                # print(f"              {'EGYEZIK' if creation_str == today_str else 'NEM egyezik'}\n")

                # Logger debug
#                logger.debug(f"Vizsgált fájl: {full_path} | Dátum: {creation_str} | Today: {today_str} -> "
#                             f"{'EGYEZIK' if creation_str == today_str else 'NEM egyezik'}")

                # Első találat (ha egyezik) → kilép
                if creation_str == today_str:
                    found_file = full_path
                    #found_creation = creation_str
                    break
        if found_file:
            break  # mappaszintű kilépés, ha megtaláltuk

    if not found_file:
        raise FileNotFoundError(f"Nincs mai {startswith} fájl a {directory} mappában.")

    logger.info({f'Found today {startswith} file': found_file})
    return found_file

def wait_until_file_ready(path, startswith):
    x = 1
    while True:
        try:
            found_file = list_excel_files(path, startswith)
            logging.info({f"Trials": x})
            return found_file
        except FileNotFoundError:
            print(f"{path} is not ready yet, waiting 5 minutes")
            x += 1
            time.sleep(300)



def read_excel(file_path, sheet_name, usecols=None, header=0):
    """Egyszerű Excel beolvasás."""
    return pd.read_excel(file_path, sheet_name=sheet_name, usecols=usecols, header=header)

def read_and_process_file(file_path, sheet_name, columns, header, dtype=None):
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, dtype=dtype)
    df.columns = [col.replace('\n', '_') for col in df.columns]
    df = df[columns]
    df.replace({' ': '', ';': '_'}, regex=True, inplace=True)
    df['file_creation_time'] = creation_date(file_path)
    return df

def excel_tpn_integer_konverzio(fajl_nev, sheet_name: Union[int, str], konvertalas=True) -> pd.DataFrame:
    """
    Fájlból történő beolvasás esetére
    TPN oszlopok beolvasása és opcionális integer konverziója.
    
    Paraméterek:
    - fajl_nev: Excel fájl elérési útja
    - sheet_name: munkalap indexe/neve
    - konvertalas: ha True, integer-ré konvertálja a TPN oszlopokat
    """
    # Fejléc beolvasása
    df_header = pd.read_excel(fajl_nev, sheet_name=sheet_name, nrows=0)
    
    # TPN oszlopok keresése (case-insensitive)
    tpn_oszlopok = [col for col in df_header.columns 
                    if 'tpn' in str(col).lower()]
    
    if not tpn_oszlopok:
        print("Nem találtam TPN-t tartalmazó oszlopot!")
        return pd.read_excel(fajl_nev, sheet_name=sheet_name)
    
    print(f"Találtam {len(tpn_oszlopok)} TPN oszlopot: {tpn_oszlopok}")
    
    # String-ként olvassuk be
    dtype_dict = {col: str for col in tpn_oszlopok}
    df = pd.read_excel(fajl_nev, sheet_name=sheet_name, dtype=dtype_dict)
    
    # Integer konverzió, ha kell
    if konvertalas:
        for col in tpn_oszlopok:
            print(f"\n{col} konvertálása...")
            # Szóközök eltávolítása
            df[col] = df[col].str.strip()
            # Integer konverzió
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            print(f"  Sikeres! Típus: {df[col].dtype}")
    
    return df

def conv(df, col_hint, alias_suffix='_ALIAS', inplace=True):
    """Biztonságos integer konverzió minden olyan oszlopra, aminek a neve tartalmazza a col_hint-et (alapból: 'TPN').
    - col_hint -> a keresett string
    - Case-insensitive (pl. 'tpn', 'TPNB', 'Master TPN', 'LM_TPND')
    - Nem dropna-zik (index-biztos)
    - Nullable int: Int64
    """
    target = df if inplace else df.copy()

    # oszlopok kiválasztása: név tartalmazza a "TPN"-t (case-insensitive)
    tpn_cols = [c for c in target.columns if col_hint.lower() in str(c).lower()]

    for col in tpn_cols:
        alias_col = f"{col}{alias_suffix}"

        target[alias_col] = target[col].astype(str).str.strip()
        target[alias_col] = target[alias_col].replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA})
        target[col] = pd.to_numeric(target[alias_col], errors='coerce').astype('Int64')

        target.drop(columns=[alias_col], inplace=True)

    return target