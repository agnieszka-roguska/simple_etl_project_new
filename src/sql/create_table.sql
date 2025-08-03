
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name TEXT,
    last_name TEXT,
    age TEXT,
    gender TEXT,
    email TEXT,
    lat REAL,
    lng REAL,
    country TEXT,
    fav_category_in_cart TEXT
);
