import pandas
import psycopg2


def populate_table():
    df = pandas.read_csv("Weather_Data.csv")
    print(df)

    try:
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()

        insert_query = "Insert into weather (STATE, DESCRIPTION, TEMPERATURE, FEELS_LIKE_TEMPERATURE,MIN_TEMP, " \
                       "MAX_TEMP,HUMIDITY,CLOUDS) values (%s,%s,%s,%s,%s,%s,%s,%s)"

        for index, row in df.iterrows():
            print("added..")
            cursor.execute(insert_query, (
                row['State'], row['Description'], row['Temperature'], row['Feels_Like_Temperature']
                , row['Min_Temperature'], row['Max_Temperature'], row['Humidity'], row['Clouds']))

        conn.commit()

        print("values are Inserted successfully")

    except:
        print("Error in connection")
    finally:
        conn.close()
        print("No issues")