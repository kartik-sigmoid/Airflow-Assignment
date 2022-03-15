import pandas
import psycopg2


def table_creation():
    df = pandas.read_csv("Weather_Data.csv")
    print(df)

    create_table = """CREATE TABLE weather(
        STATE VARCHAR(30),
        DESCRIPTION varchar(30),
        TEMPERATURE decimal,
        FEELS_LIKE_TEMPERATURE decimal,
        MIN_TEMP decimal,
        MAX_TEMP decimal,
        HUMIDITY numeric,
        CLOUDS numeric)"""

    try:
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()

        cursor.execute(create_table)

        print("Table Created successfully")

        conn.commit()

    except:
        print("Error in connection")
    finally:
        conn.close()
        print("No issues")
