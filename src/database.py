import psycopg2
import os

from src.utils import get_employer, get_vacancies

DB_HOST = os.getenv('BD_HOST')
DB_NAME = os.getenv('BD_NAME')
DB_USER = os.getenv('BD_USER')
DB_PASSWORD = os.getenv('BD_PASSWORD')


def create_tables():
    """
    Создание БД и таблиц
    :return:
    """
    global conn
    conn_params = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD
    }

    try:
        conn = psycopg2.connect(**conn_params, autocommit=True)
        cur = conn.cursor()

        # Drop and create the database 'hh'
        cur.execute("DROP DATABASE IF EXISTS hh")
        cur.execute("CREATE DATABASE hh")

    except psycopg2.Error as error:
        print(f"Ошибка при создании БД: {error}")
    finally:
        if conn:
            conn.close()

    try:
        # Connect to the 'hh' database
        conn = psycopg2.connect(host="localhost", database="hh",
                                user="postgres", password="Igorevi4")
        with conn.cursor() as cur:
            # Create 'employers' table
            cur.execute("""
            CREATE TABLE employers (
                employer_id SERIAL PRIMARY KEY,
                company_name VARCHAR(200),
                open_vacancies INTEGER
            )
            """)

            # Create 'vacancies' table
            cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                vacancies_name VARCHAR(200),
                payment INTEGER,
                requirements TEXT,
                vacancies_url TEXT,
                employer_id INTEGER REFERENCES employers(employer_id)
            )
            """)
    except psycopg2.Error as error:
        print(f"Ошибка при создании таблиц: {error}")
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
                    cur.execute("INSERT INFO employers (employer_id, company_name, open_vacancies)"
                                "VALUES (%s, %s, %s) RETURNING employer_id",
                                (employer_list["employer_id"], employer_list["company_name"],
                                 employer_list["open_vacancies"]))
                for employer in employers_list:
                    vacancy_list = get_vacancies(employer)
                    for v in vacancy_list:
                        cur.execute("INSERT INFO vacancies (vacancy_id, vacancies_name,"
                                    "payment, requirements, vacancies_url, employer_id)"
                                    "VALUES (%s, %s, %s, %s, %s, %s)",
                                    (v["vacancy_id"], v["vacancies_name"], v["payment"],
                                     v["requirements"], v["vacancies_url"], v["employer_id"]))
    except psycopg2.Error as error:
        print(f"Ошибка при заполнении таблиц: {error}")
