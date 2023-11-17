from sqlalchemy import create_engine
from sqlalchemy import engine as sa_engine
from urllib.parse import quote
from lib.utils.utils import get_parameter


class DB_Connector:
    def __init__(self):
        pass

    @staticmethod
    def get_df_cnx():
        url = sa_engine.URL.create(
            drivername="postgresql+psycopg2",
            username="grafana_ro",
            password=get_parameter('db01.genxs.tradingsystem.grafana_ro.password'),
            host="pgsql.stxfi.org",
            port=5432,
            database="FixedIncomeProd",
        )
        engine = create_engine(url)
        return engine

    @staticmethod
    def get_bankingfile_cnx():
        url = sa_engine.URL.create(
            drivername="postgresql+psycopg2",
            username="grafana_ro",
            password=get_parameter('db01.genxs.tradingsystem.grafana_ro.password'),
            host="so1mh2wqzxv7nls.clklixg3mszi.eu-central-1.rds.amazonaws.com",
            port=5432,
            database="bankingfiles",
        )
        engine = create_engine(url)
        return engine

    def get_sfi_cnx():
        url = sa_engine.URL.create(
            drivername="postgresql+psycopg2",
            username="sfi_report_writer",
            password=get_parameter('db01.genxs.tradingsystem.grafana_ro.password'),
            host="sfi-transactions-db.stxfi.org",
            port=5432,
            database="sfi_transactions",
        )
        engine = create_engine(url)
        return engine

    def get_otdb_cnx():
        url = sa_engine.URL.create(
            drivername="postgresql+psycopg2",
            username=get_parameter('otdb.db_username'),
            password=get_parameter('otdb.db_password'),
            host=get_parameter('otdb.db_host'),
            port=5432,
            database=get_parameter('otdb.db_name'),
        )
        engine = create_engine(url)
        return engine


    @staticmethod
    def get_gx_cnx():
        user = get_parameter('genxs.ro.user')
        pwd = get_parameter('genxs.ro.password')
        host = get_parameter('genxs.ro.host')
        #host = '172.16.148.82'
        engine = create_engine(f'mysql+pymysql://{user}:{quote(pwd)}@{host}/tradingsystem', echo=False)
        return engine
