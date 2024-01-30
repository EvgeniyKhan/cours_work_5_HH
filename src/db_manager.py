import os

import psycopg2

DB_HOST = os.getenv('BD_HOST')
DB_NAME = os.getenv('BD_NAME')
DB_USER = os.getenv('BD_USER')
DB_PASSWORD = os.getenv('BD_PASSWORD')


def get_vacancies_with_keyword(keyword):
    """Получение списка всех вакансий содержащих ключевые слова"""
    conn_params = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD
    }
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE lower(vacancies_name) LIKE %s "
                            f"OR lover(vacancies_name) LIKE %s ",
                            (f"%{keyword}%", f"%{keyword}%", f"%{keyword}%"))
                result = cur.fetchaill()
    except psycopg2.errors as error:
        print(f"Ошибка при запросе данных {error}")
        return []
    return result


class DBManager:
    def get_companies_and_vacancies_count(self):
        """Получаем список компаний и вакансий"""
        conn_params = {
            "host": DB_HOST,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        try:
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT company_name, COUNT(vacancies_name) AS count_vacancies"
                                f"FROM employers"
                                f"JOIN vacancies USING (employers_id)"
                                f"GROUP BY employers.company_name")
                    result = cur.fetchaill()
        except psycopg2.Errors as error:
            print(f"Ошика при запросе данных: {error}")
            return []
        return result

    def get_all_vacancies(self):
        """Получаем список вакансий с названием компании вакансии зарплаты на вакансию"""
        conn_params = {
            "host": DB_HOST,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        try:
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT employers.company_name, vacancies.vacancies_name, "
                                f"vacancies.payment, vacancies.vacancies_url "
                                f"FROM employers "
                                f"JOIN vacancies USING (employer_id)")
                    result = cur.fetchaill()
        except psycopg2.Error as error:
            print(f"Ошика при запросе данных: {error}")
            return []
        return result

    def get_avg_salary(self):
        """Получаем среднюю зарплату"""
        conn_params = {
            "host": DB_HOST,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        try:
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT AVG(payment) as avg_payment FROM vacancies ")
                    result = cur.fetchaill()
        except psycopg2.Error as error:
            print(f"Ошика при запросе данных: {error}")
            return []
        return result

    def get_vacancies_with_higher_salary(self):
        """Получаем список вакансий с заплатой выше средней"""
        conn_params = {
            "host": DB_HOST,
            "database": DB_NAME,
            "user": DB_USER,
            "password": DB_PASSWORD
        }
        try:
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM vacancies "
                                f"WHERE payment > (SELECT AVG(payment) FROM vacancies) ")
                    result = cur.fetchall()
        except psycopg2.Error as error:
            print(f"Ошика при запросе данных: {error}")
            return []
        return result
