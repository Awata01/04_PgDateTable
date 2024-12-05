import sys
from datetime import datetime

import holidays

sys.path.append(r"C:\Users\awata\Awata01\Programming\00_Common\src")
from common_functions import output_csv_file
from custom_logging import CustomLogging
from postgreSQL_manager import PostgreSQLManager, TableInfo


def Main()  -> None:
    """任意の年の日本の祝日を取得し、postgres.Date.jp_holidaysテーブルに挿入する
    """
    logger = CustomLogging("get_jp_holidays")
    logger.process_start()
    try:
        # 投入先テーブル
        tbl = TableInfo(database="postgres", schema="Date", table="jp_holidays")

        # SQL接続
        pg = PostgreSQLManager(
            host="localhost",
            database="postgres",
            user="postgres",
            password="PostgresAdmin"
        )
        pg.connect()

        # 引数が指定されていなければ、1年後の年を取得
        trgYear = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year + 1

        # 挿入年度のレコードを削除
        query = f"""
            Delete From "{tbl.schema}".{tbl.table}
            Where EXTRACT(YEAR From date) = %(trgYear)s
        """
        pg.execute_query(query, {"trgYear": trgYear})

        file_path = f"C:/Users/awata/Awata01/Programming/04_PostgreSQL/data/jp_holidays_{trgYear}.csv"
        logger.info(f"{trgYear}年の祝日ファイルを生成しました:{file_path}")

        # 日本の祝日を取得
        jp_holidays = holidays.Japan(years=trgYear)
        holidays_list = [{"date": date, "name": name} for date, name in jp_holidays.items()]

        # csvファイルに出力
        output_csv_file(file_path, holidays_list)

        # ファイルをpostgresに挿入
        pg.copy_insert(file_path, tbl.table, schema_name=tbl.schema, header_flag=True)

    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")

    finally:
        pg.close()
        logger.process_end()

if __name__ == "__main__":
    Main()