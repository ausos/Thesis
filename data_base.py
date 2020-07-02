import configparser
import psycopg2
import psycopg2.extras


def get_config():
    config = ConfigParser()
    config.read('config.ini')
    if config.has_section("pgsql"):
        return dict(config["pgsql"])
    return {"user": "myuser", "password": "mypasswd", "host": "localhost", "port": 5432}

conf = get_config()

def connect_db():
    try:
        conn = psycopg2.connect(
               user=conf['user'],
               password=conf['password'],
               host=conf['host'],
               port=conf['port'])
        print("Database opened successfully")
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        raise
    return conn, conn.cursor()


def close_connection(conn, cur):
    if conn:
        conn.commit()
        cur.close()
        conn.close()


