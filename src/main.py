import os

from src.database import add_to_table, create_tables
from src.db_manager import DBManager, get_vacancies_with_keyword


def main():
    EMPLOYERS_LIST = os.getenv("EMPLOYERS_LIST", "")
    employers_list = [int(employer_id) for employer_id in EMPLOYERS_LIST.split(",") if employer_id]
    dbmanager = DBManager()
    create_tables()
    add_to_table(employers_list)

    while True:

        task = input(
            "Напишите 1 для получения списка всех компаний и количество вакансий у них\n"
            "Напишите 2 для полусения списка всех вакансий с названиями компании, вакансии, зарплаты, ссылки\n"
            "Напишите 3 для получения средней зарплаты по выбранным вакансиям\n"
            "Напишите 4 для получения списка всех вакансий, где зарплата выше средней\n"
            "Напишите 5для получения списка всех вакансий,содержащих ключевые слова\n"
            "Напишите закончить, дабы завершить работу\n"
        )

        if task == "закончить":
            break
        elif task == '1':
            print(dbmanager.get_companies_and_vacancies_count())
            print()
        elif task == '2':
            print(dbmanager.get_all_vacancies())
            print()
        elif task == '3':
            print(dbmanager.get_avg_salary())
            print()
        elif task == '4':
            print(dbmanager.get_vacancies_with_higher_salary())
            print()
        elif task == '5':
            keyword = input('Введите ключевое слово: ')
            print(get_vacancies_with_keyword(keyword))
            print()
        else:
            print('Неверно')


if __name__ == '__main__':
    main()
