import functions

def main():
    url = "https://dummyjson.com/users"
    users = functions.fetch_users_in_batches(url, 100)
    data = functions.get_cart_data()
    users = functions.find_fav_cart_category_for_users(users, data)

    functions.save_as_csv(users)
    functions.save_to_db(users)

if __name__ == "__main__":
    main()