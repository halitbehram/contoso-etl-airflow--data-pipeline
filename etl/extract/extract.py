from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging

def extract_selected_tables() -> dict:

    logger = logging.getLogger(__name__)

    server = 'DESKTOP-R41JDGP\\SQLEXPRESS'
    database = 'ContosoRetailDW'
    driver = 'ODBC Driver 17 for SQL Server'

    try:
        connection_string = (
            f"mssql+pyodbc://@{server}/{database}"
            "?trusted_connection=yes"
            f"&driver={driver}"
        )
        engine = create_engine(connection_string)
        connection = engine.connect()
        logger.info("Veritabanına başarıyla bağlanıldı.")
    except SQLAlchemyError as e:
        logger.error(f"Tekrardan bağlantı kurmayı deneyiniz:", {e})
        return {}
    except Exception as e:
        logger.error(f"Bağlantı hatası: {e}")
        return {}

    tables = ["DimCustomer", "FactOnlineSales", "DimProduct", "DimDate", "DimStore"]
    dataframes = {}

    for table in tables:
        try:
            logger.info(f"{table} tablosu işleniyor")
            df = pd.read_sql(f"SELECT TOP 100000 * FROM {table}", engine)
            dataframes[table] = df
            logger.info(f"{table} tablosu yüklendi: {df.shape}")
        except Exception as e:
            logger.error(f"{table} tablosu yüklenemedi hata : {e}")
    
    return dataframes

