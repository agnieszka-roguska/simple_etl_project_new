import csv
import os
import requests
import sqlite3
from dotenv import load_dotenv

load_dotenv()


def get_country(lng: str, lat: str) -> str:
    API_KEY = os.getenv("API_KEY")
    endpoint = (
        f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lng}&key={API_KEY}"
    )
    response = requests.get(endpoint)
    data = response.json()
    try:
        return data["results"][0]["components"]["country"]
    except Exception:
        return "Unknown"


def process_users_data(users_data: list[dict]) -> list:
    for index, user in enumerate(users_data):
        user_filtered = {
            k: v
            for k, v in user.items()
            if k in ["id", "firstName", "lastName", "age", "gender", "email"]
        }
        user_coords = user["address"]["coordinates"]
        lat = user_coords["lat"]
        lng = user_coords["lng"]
        user_filtered["lat"] = lat
        user_filtered["lng"] = lng
        user_filtered["country"] = get_country(lng, lat)
        users_data[index] = user_filtered
    return users_data


def fetch_users_in_batches(base_url: str, limit: int) -> list[dict]:
    all_users = list()
    skip = 0
    while True:
        url = base_url + f"?limit={limit}&skip={skip}"
        response = requests.get(url)
        data = response.json()
        users = data.get("users")
        users_processed = process_users_data(users)
        all_users += users_processed
        total = data.get("total", 0)

        if skip + limit > total:
            break
        skip += limit

    return all_users


def get_category_from_thumbnail(thumbnail: str) -> str:
    prefix_to_remove = "https://cdn.dummyjson.com/products/images/"
    category = thumbnail[len(prefix_to_remove) :].split("/")[0]
    return category


def get_cart_data() -> list[dict]:
    url = "https://dummyjson.com/carts"
    response = requests.get(url)
    data = response.json()
    return data.get("carts")


def find_fav_cart_category_for_users(
    users: list[dict], carts: list[dict]
) -> list[dict]:
    user_map = {user["id"]: user for user in users}
    for cart in carts:
        category_counts = {}
        for product in cart.get("products", []):
            category = get_category_from_thumbnail(product.get("thumbnail", ""))
            quantity = product.get("quantity", 0)
            category_counts[category] = category_counts.get(category, 0) + quantity
        fav_category_str = "Unknown"
        if category_counts:
            max_quantity = max(category_counts.values())
            fav_categories = [
                category
                for category, quantity in category_counts.items()
                if quantity == max_quantity
            ]

            fav_category_str = "; ".join(fav_categories)

        user = user_map.get(cart.get("userId"))
        if user:
            user["fav_category_in_cart"] = fav_category_str

    for user in users:
        user["fav_category_in_cart"] = user.get("fav_category_in_cart")

    return users


def get_results_directory():
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    results_folder = os.path.join(project_root, "results")

    return results_folder


def save_as_csv(data: list[dict]) -> None:
    results_folder = get_results_directory()
    # Make sure the folder exists
    os.makedirs(results_folder, exist_ok=True)
    # Define the full path to the file you want to save
    csv_file_path = os.path.join(results_folder, "users_data.csv")
    with open(csv_file_path, mode="a") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def save_to_db(users_data: list[dict]) -> None:
    results_folder = get_results_directory()
    db_file_path = os.path.join(results_folder, "results.db")

    with open(".\sql\create_table.sql", "r") as sql_file:
        create_table_script = sql_file.read()

    with open(".\sql\insert_user_data.sql", "r") as sql_file:
        insert_user_data_script = sql_file.read()

    connection = sqlite3.connect(db_file_path)
    cur = connection.cursor()  # to execute commands on the database
    cur.execute(create_table_script)

    for user in users_data:
        cur.execute(
            insert_user_data_script,
            (
                user["firstName"],
                user["lastName"],
                user["age"],
                user["gender"],
                user["email"],
                user["lat"],
                user["lng"],
                user["country"],
                user["fav_category_in_cart"],
            ),
        )

    connection.commit()
    connection.close()
