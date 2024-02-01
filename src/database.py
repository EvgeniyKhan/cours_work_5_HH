import psycopg2
import os

from src.utils import get_employer, get_vacancies

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def create_table():
    """Создаем БД и таблицы"""
    conn_params = {
        'host': DB_HOST,
        'database': DB_NAME,
        'user': DB_USER,
        'password': DB_PASSWORD

    }

    try:
        conn = psycopg2.connect(**conn_params, autocommit=True)
        cur = conn.cursor()

        cur.execute("DROP DATABASE IF EXISTS hh")
        cur.execute("CREATE DATABASE hh")

    except psycopg2.Error as e:
        print(f"Ошибка при создании базы данных: {e}")
    finally:
        if conn:
            conn.close()

    try:
        conn = psycopg2.connect(host="localhost", database="hh",
                              user="postgres", password="Igorevi4")

        with conn.cursor() as cur:
            cur.execute("""
                    CREATE TABLE employers (
                    employer_id INTEGER PRIMARY KEY,
                    company_name varchar(260),
                    open_vacancies INTEGER
                    )""")
        cur.execute("""
                            CREATE TABLE vacancies (
                            vacancy_id SERIAL PRIMARY KEY,
                            vacancies_name varchar(260),
                            payment INTEGER,
                            requirement TEXT,
                            vacancies_url TEXT,
                            employer_id INTEGER REFERENCES employers(employer_id)
                            )""")
    except psycopg2.Error as e:
        print(f"Ошибка при создании таблиц: {e}")
    finally:
        if conn:
            conn.close()



def add_to_table(employers_list):
    """
    Заполняем базу данных
    :param employers_list:
    :return:
    """
    conn_params = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD
    }
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE employers, vacancies RESTART IDENTITY")
                for employer in employers_list:
                    employer_list = get_employer(employer)
                    cur.execute("INSERT INTO employers (employer_id, company_name, open_vacancies)"
                                "VALUES (%s, %s, %s) RETURNING employer_id",
                                (employer_list["employer_id"], employer_list["company_name"],
                                 employer_list["open_vacancies"]))
                for employer in employers_list:
                    vacancy_list = get_vacancies(employer)
                    for v in vacancy_list:
                        cur.execute("INSERT INTO vacancies (vacancy_id, vacancies_name,"
                                    "payment, requirement, vacancies_url, employer_id)"
                                    "VALUES (%s, %s, %s, %s, %s, %s)",
                                    (v["vacancy_id"], v["vacancies_name"], v["payment"],
                                     v["requirement"], v["vacancies_url"], v["employer_id"]))
    except psycopg2.Error as error:
        print(f"Ошибка при заполнении таблиц: {error}")
