import pandas as pd
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
import logging

# Настройка логирования
logging.basicConfig(filename='logs/etl_log.txt', level=logging.INFO)

if __name__ == "__main__":
    logging.info("ETL процесс начался")

    try:
        df = extract_data("data_sources/generated_data.csv")
        logging.info("Данные извлечены")

        transformed_dfs = transform_data(df)
        logging.info("Данные нормализованы")

        load_data(transformed_dfs)
        logging.info("Данные загружены в БД")

    except Exception as e:
        logging.error(f"Ошибка в ETL: {e}")
        print(f"Произошла ошибка: {e}")