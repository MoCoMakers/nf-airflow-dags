import psycopg2
import utils
import traceback

_config = utils.get_config_nf()

DB_HOST = _config['db']['postgres_host']
DB_NAME = _config['db']['postgres_name']
DB_USER = _config['db']['postgres_user']
DB_PASSWORD = _config['db']['postgres_password']

pg_conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
)

def postgres_test():
    try:
        cursor = pg_conn.cursor()
        print("Connection successful!")
    except Exception as e:
        print("Connection failed!")
        traceback.print_exc()     
    finally:
        cursor.close()


#postgres_test()