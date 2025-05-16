import pandas as pd
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

def save_excel(df: pd.DataFrame, path: str, sheet_name: str):
    os.makedirs(os.path.dirname(path), exist_ok= True)
    df.to_excel(path, sheet_name= sheet_name, index= False)
    logger.info(f"Excell Başarıyla Kaydedildi: {path}")

def save_multiple_excel(dataframes: dict[str, pd.DataFrame], path: str):
    os.makedirs(os.path.dirname(path), exist_ok= True)

    with pd.ExcelWriter(path, engine= "openpyxl") as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name = sheet_name, index= False)
            logger.info(f"{sheet_name} sayfası yazıdırıldı => {path}")
            
    logger.info(f"Tüm sayfalar yazıldı => {path}")

def save_csv(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok= True)
    df.to_csv(path, index= False)
    logger.info(f"CSV Başarıyla Kaydedildi = {path}")

def save_multiple_csv(dataframes: dict[str, pd.DataFrame], path: str):
    os.makedirs(path, exist_ok= True)

    for name, df in dataframes.items():
        filename = f"{path}/{name}.csv"
        df.to_csv(filename, index= False)
        logger.info(f"{name} CSV Dosyası Oluşturuldu => {filename}")
    
    logger.info("Tüm CSV Dosyaları Oluşturuldu")

def save_sqlite(dataframes: dict[str, pd.DataFrame], path: str):
    os.makedirs(os.path.dirname(path), exist_ok= True)
    conn = sqlite3.connect(path)

    for name, df in dataframes.items():
        df.to_sql(name, con= conn, if_exists= "replace", index= False)
        logger.info(f"{name} tablosu veritabanına kaydedildi =>{path}")

    logger.info("Bütün tablolar kaydedildi")
    conn.close()    


    