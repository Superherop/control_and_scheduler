from csv import writer
import os
import pandas as pd
from tkinter import filedialog as fd
import numpy as np
import jaydebeapi
from dotenv import load_dotenv
from core.paths import ASSETS_PATH, ASSETS_TIME_TABLE   # ← ez a fontos rész

class DfModification:
    """ DataFrame-ek módosításának szabályozó osztály. """

    @staticmethod
    def type_modifier(df):
        for col in df.columns:
            if df[col].dtype == 'object' or df[col].dtype == 'float64':
                try:
                    df[col] = pd.to_numeric(df[col].str.replace(',',''), downcast='float')
                except ValueError:
                    print(f"A '{col}' oszlop nem számokat tartalmaz.")
        print(df.select_dtypes(include='number').dtypes)
        return df

class AI_tools:
    OLLAMA_URL = "http://localhost:11434/api/generate"
    

class DatabaseManager:
    """ Adatbázis kapcsolat és lekérdezések kezelése. """
    env_path = os.path.join(os.environ['ONEDRIVECOMMERCIAL'], 'Documents','Python','alapmodulok','.env')
    load_dotenv(env_path)
    DRIVER = "org.apache.kyuubi.jdbc.KyuubiHiveDriver"
    
    URL3 = "jdbc:kyuubi://cep-k8s-user-kyuubi.global.tesco.org:10009/?spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam:::role/role-cep-analyst;#kyuubi.operation.result.format=DEFAULT;kyuubi.operation.incremental.collect=false;kyuubi.engine.share.level=CONNECTION"
    URL2 = "jdbc:kyuubi://cep-k8s-user-kyuubi.global.tesco.org:10009/?spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam:::role/role-cep-rep-supuser;#kyuubi.operation.result.format=DEFAULT;kyuubi.operation.incremental.collect=false;kyuubi.engine.share.level=CONNECTION"
    URL = "jdbc:kyuubi://cep-k8s-ingest-kyuubi.global.tesco.org:10009/;kyuubi.operation.incremental.collect=false;#spark.hadoop.fs.s3a.assumed.role.arn=arn:aws:iam:::role/role-cep-rep-supuser;kyuubi.engine.share.level=CONNECTION;"
    JDBC_DRIVER_PATH = [r"C:\\Users\\pmajor1\\OneDrive - Tesco\\Documents\\Python\\kyuubi-hive-jdbc-shaded-1.9.0-SNAPSHOT\\kyuubi-hive-jdbc-shaded-1.9.3-SNAPSHOT.jar"]
    USERNAME = os.getenv("MY_USERNAME")
    PASSWORD = os.getenv("MY_PASSWORD")

    @staticmethod
    def get_connection():
        return jaydebeapi.connect(DatabaseManager.DRIVER, DatabaseManager.URL2, [DatabaseManager.USERNAME, DatabaseManager.PASSWORD], DatabaseManager.JDBC_DRIVER_PATH)
    @staticmethod
    def run_query(query, *args):
        cursor = None
        conn = None
        """Lekérdezi a query-t és visszaadja DataFrame-ként.
            Ha a lekérdezést szeretnéd paraméterezni, akkor a query-ben ?-t kell használni, és a paramétereket egy tuple-ban kell átadni.
            példa: query = "select * from table where column = ?
                    DatabaseManager.run_query(query, 'value')
                    """

        try:
            conn = DatabaseManager.get_connection()
            cursor = conn.cursor()

            params = tuple(*args)
            
            # Ellenőrzés: megfelelő számú paraméter van-e?
            
            if query.count("?") != len(params):
                raise ValueError(f"A query {query.count('?')} paramétert vár, de {len(params), print(params)} paramétert adtál meg.")

            cursor.execute(query, params)
            result_set = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            return pd.DataFrame(result_set, columns=columns)

        except Exception as e:
            print(f"Hiba történt: {e}")
            return None

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @staticmethod
    def smtp_email_send(subject, sender_mail, receiver_mail, html, cc_mail=None, bcc_mail=None):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # --- címek normalizálása ---
        def normalize(addr):
            if not addr:
                return ""
            if isinstance(addr, (list, tuple)):
                return ",".join(str(a).strip() for a in addr)
            return str(addr).strip()

        sender_mail = normalize(sender_mail)
        receiver_mail = normalize(receiver_mail)
        cc_mail = normalize(cc_mail)
        bcc_mail = normalize(bcc_mail)

        # MIME message
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = sender_mail
        message['To'] = receiver_mail
        if cc_mail:
            message['Cc'] = cc_mail

        # címzett lista
        rcpt = []
        if receiver_mail:
            rcpt.append(receiver_mail)
        if cc_mail:
            rcpt.extend(cc_mail.split(","))
        if bcc_mail:
            rcpt.extend(bcc_mail.split(","))

        html_body = f"<body>{html}</body></html>"
        part = MIMEText(html_body, 'html')
        message.attach(part)

        server = smtplib.SMTP("SMTP.TESCO-EUROPE.COM", 25)
        server.sendmail(sender_mail, rcpt, message.as_string())
        server.quit()

def departments_df():
    import pandas as pd
    return pd.read_csv(os.path.join(ASSETS_PATH,"deps.csv"), index_col=0)

def departments_list():
    import pandas as pd
    deps = pd.read_csv(os.path.join(ASSETS_PATH,"deps.csv"), index_col=0)
    return deps['department_name'].tolist()


def source(df):
    if (df['local_supplier_id'] == 99998) and (df['ticb_supplier_id'] == 4000001) or (df['ticb_supplier_id'] == 4000032): return "IS"
    elif df['ticb_supplier_id'] == -1: return "local"
    else: return "Domestic"

def max_cols():
    import pandas as pd
    pd.options.display.max_columns=None
    
def max_rows():
    import pandas as pd
    pd.options.display.max_rows=None

def write_es(path, df, sheet_name):
    import pandas as pd
    writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
    df.to_excel(writer, sheet_name = sheet_name)
    writer.close()

def read_query_from_text_file(file_name):
	"""Read an SQL query from a text file and make it ready to paste to pandas sql query function
	Args:
	file_name (str): The name of the file to read from
	Returns:
	str: The query string ready to paste to pandas sql query function
	"""
	with open(file_name, 'r') as f:
		query = f.read()
	# Remove all newline characters
		query = query.replace('\n', ' ')
	# Remove all extra spaces
		query = ' '.join(query.split())
	return query

def private_mailing(app_name, message): 
    message = f'Subject: Message from {app_name}\n\nThe result of running is: {message}\n\nThanks\nPatrik'
    def mail_result(message):
        import smtplib
        conn = smtplib.SMTP('smtp.gmail.com', 587)
        conn.starttls()
        conn.login('major.patrik@gmail.com','vyaceudyqyxzeqev')
        conn.sendmail('major.patrik@gmail.com','major.patrik@gmail.com',message)
        conn.quit()

    mail_result(message)

def get_tesco_week():
    import pandas as pd
    from datetime import date, timedelta
    today = date.today() - timedelta(days=7)
    d = int(today.strftime("%Y%m%d"))
    time = pd.read_csv(ASSETS_TIME_TABLE)
    time.query('dmtm_d_code == @d')
    val = time.query('dmtm_d_code == @d')['dmtm_fw_code'].values[0]
    return val

def get_fyear_start_week():
    import pandas as pd
    from datetime import date
    today = date.today()
    d = int(today.strftime("%Y%m%d"))
    time = pd.read_csv(ASSETS_TIME_TABLE)
    time.query('dmtm_d_code == @d')
    val = time.query('dmtm_d_code == @d')['dmtm_fw_code'].values[0]
    year = val[:5]
    fyear = year + 'w01'
    return fyear

def get_fyear_start_period():
    import pandas as pd
    from datetime import date
    today = date.today()
    d = int(today.strftime("%Y%m%d"))
    time = pd.read_csv(ASSETS_TIME_TABLE)
    time.query('dmtm_d_code == @d')
    val = time.query('dmtm_d_code == @d')['dmtm_fw_code'].values[0]
    year = val[:5]
    fyear = year + 'p01'
    return fyear

def time_table():
    import pandas as pd
    return pd.read_csv(ASSETS_TIME_TABLE)

def get_current_tesco_week():
    import pandas as pd
    from datetime import date
    today = date.today()
    d = int(today.strftime("%Y%m%d"))
    time = pd.read_csv(ASSETS_TIME_TABLE)
    time.query('dmtm_d_code == @d')
    val = time.query('dmtm_d_code == @d')['dmtm_fw_code'].values[0]
    return val

def get_current_tesco_period():
    import pandas as pd
    from datetime import date
    today = date.today()
    d = int(today.strftime("%Y%m%d"))
    time = pd.read_csv(ASSETS_TIME_TABLE)
    time.query('dmtm_d_code == @d')
    val = time.query('dmtm_d_code == @d')['dmtm_fp_code'].values[0]
    return val
