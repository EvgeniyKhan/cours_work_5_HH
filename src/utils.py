import requests


def get_vacancies(employer_id):
    """
    Получение данных о вакансиях через API
    :param employer_id:
    :return:
    """
    params = {
        "area": 1,
        "page": 0,
        "per_page": 10
    }
    url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
    try:
        data_vacancies = requests.get(url, params=params).json()
    except requests.RequestException as error:
        print(f"Ошибка при запросе данных: {error}")
        return []

    vacancies = []
    for item in data_vacancies.get("items", []):
        hh_vacancies = {
            "vacancy_id": int(item.get("id", 0)),
            "vacancies_name": item.get("name", ""),
            "payment": item.get("salary", {}).get("from", None) if item.get("salary") else None,
            "requirement": item.get("snippet", {}).get("requirement", ""),
            "vacancies_url": item.get("alternate_url"),
            "employer_id": employer_id
        }
        if hh_vacancies["payment"] is not None:
            vacancies.append(hh_vacancies)
    return vacancies


def get_employer(employer_id):
    """
    Получение данных о работодателях через API
    :param employer_id:
    :return:
    """
    url = f"https://api.hh.ru/employers{employer_id}"
    try:
        data_vacancies = requests.get(url).json()
    except requests.RequestException as error:
        print(f"Ошибка при запросе данных: {error}")
        return {}

    hh_company = {
        "employer_id": int(employer_id),
        "company_name": data_vacancies.get("name", ""),
        "open_vacancies": data_vacancies.get("open_vacancies", 0)
    }
    return hh_company
