import pandas as pd
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=NetflixDW;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()

df = pd.read_csv(r"D:\Netflix_project\netflix_titles.csv")
df = df.where(pd.notnull(df), None)  
types = df['type'].dropna().unique()
for i, t in enumerate(types, 1):
    cursor.execute("INSERT INTO Dim_Type VALUES (?, ?)", i, t)

type_map = {t: i for i, t in enumerate(types, 1)}

regions = {
    'Asia': 1, 'Europe': 2, 'Americas': 3,
    'Africa': 4, 'Oceania': 5, 'Unknown': 6
}
for name, key in regions.items():
    cursor.execute("INSERT INTO Dim_Region VALUES (?, ?)", key, name)

subregions = {
    'South Asia': (1, 1), 'East Asia': (2, 1), 'Southeast Asia': (3, 1),
    'Western Europe': (4, 2), 'Eastern Europe': (5, 2),
    'North America': (6, 3), 'South America': (7, 3), 'Latin America': (8, 3),
    'West Africa': (9, 4), 'East Africa': (10, 4),
    'Australia & NZ': (11, 5), 'Unknown': (12, 6)
}
for name, (key, reg) in subregions.items():
    cursor.execute("INSERT INTO Dim_Subregion VALUES (?, ?, ?)", key, reg, name)

country_subregion = {
    'India': 1, 'Pakistan': 1, 'Bangladesh': 1,
    'Japan': 2, 'South Korea': 2, 'China': 2,
    'Thailand': 3, 'Philippines': 3, 'Indonesia': 3, 'Malaysia': 3,
    'United Kingdom': 4, 'France': 4, 'Germany': 4, 'Spain': 4, 'Italy': 4,
    'Poland': 5, 'Romania': 5,
    'United States': 6, 'Canada': 6,
    'Brazil': 7, 'Argentina': 7, 'Colombia': 8, 'Mexico': 8,
    'Nigeria': 9, 'Ghana': 9, 'Egypt': 10, 'Kenya': 10,
    'Australia': 11, 'New Zealand': 11,
}

countries = df['country'].dropna().str.split(',').explode().str.strip().unique()
country_map = {}
for i, c in enumerate(countries, 1):
    sub = country_subregion.get(c, 12)
    cursor.execute("INSERT INTO Dim_Country VALUES (?, ?, ?)", i, sub, c)
    country_map[c] = i

audiences = {'Kids': 1, 'Teens': 2, 'Adults': 3, 'Unknown': 4}
for name, key in audiences.items():
    cursor.execute("INSERT INTO Dim_Audience VALUES (?, ?)", key, name)

rating_audience = {
    'G': 1, 'TV-Y': 1, 'TV-Y7': 1, 'TV-Y7-FV': 1,
    'PG': 2, 'TV-PG': 2, 'TV-G': 2,
    'PG-13': 3, 'TV-14': 3, 'TV-MA': 3, 'R': 3, 'NC-17': 3, 'NR': 4, 'UR': 4
}

ratings = df['rating'].dropna().unique()
rating_map = {}
for i, r in enumerate(ratings, 1):
    aud = rating_audience.get(r, 4)
    cursor.execute("INSERT INTO Dim_Rating VALUES (?, ?, ?)", i, aud, r)
    rating_map[r] = i

categories = ['Drama', 'Comedy', 'Action', 'Documentary', 'Thriller',
              'Romance', 'Horror', 'Animation', 'Family', 'Other']
cat_map = {c: i for i, c in enumerate(categories, 1)}
for name, key in cat_map.items():
    cursor.execute("INSERT INTO Dim_Category VALUES (?, ?)", key, name)

def get_category(genre_str):
    for cat in categories[:-1]:
        if cat.lower() in genre_str.lower():
            return cat
    return 'Other'

genres = df['listed_in'].dropna().str.split(',').explode().str.strip().unique()
genre_map = {}
for i, g in enumerate(genres, 1):
    cat = get_category(g)
    cursor.execute("INSERT INTO Dim_Genre VALUES (?, ?, ?)", i, cat_map[cat], g)
    genre_map[g] = i

cursor.execute("INSERT INTO Dim_Nationality VALUES (?, ?, ?)", 1, 'Unknown', 6)
nationality_key = 1

directors = df['director'].dropna().unique()
director_map = {}
for i, d in enumerate(directors, 1):
    cursor.execute("INSERT INTO Dim_Director VALUES (?, ?, ?)", i, 1, d)
    director_map[d] = i

df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
valid_dates = df['date_added'].dropna().unique()

years, quarters, months, dates = set(), set(), set(), set()
for d in valid_dates:
    years.add(d.year)
    quarters.add((d.year, d.quarter))
    months.add((d.year, d.quarter, d.month))
    dates.add((d.year, d.quarter, d.month, d.day, d.date()))

year_map, quarter_map, month_map, date_map = {}, {}, {}, {}

for i, y in enumerate(sorted(years), 1):
    cursor.execute("INSERT INTO Dim_Year VALUES (?, ?)", i, y)
    year_map[y] = i

quarter_names = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
for i, (y, q) in enumerate(sorted(quarters), 1):
    cursor.execute("INSERT INTO Dim_Quarter VALUES (?, ?, ?, ?)",
                   i, year_map[y], quarter_names[q], q)
    quarter_map[(y, q)] = i

month_names = {1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',
               7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}
for i, (y, q, m) in enumerate(sorted(months), 1):
    cursor.execute("INSERT INTO Dim_Month VALUES (?, ?, ?, ?)",
                   i, quarter_map[(y, q)], month_names[m], m)
    month_map[(y, q, m)] = i

for i, (y, q, m, day, full) in enumerate(sorted(dates), 1):
    cursor.execute("INSERT INTO Dim_Date VALUES (?, ?, ?, ?)",
                   i, month_map[(y, q, m)], full, day)
    date_map[full] = i


for _, row in df.iterrows():
    try:
       
        date_key = date_map.get(row['date_added'].date()) if pd.notnull(row.get('date_added')) else None

       
        country = row['country'].split(',')[0].strip() if row['country'] else None
        country_key = country_map.get(country)

        genre = row['listed_in'].split(',')[0].strip() if row['listed_in'] else None
        genre_key = genre_map.get(genre)

        rating_key = rating_map.get(row['rating'])

        director_key = director_map.get(row['director'])
        type_key = type_map.get(row['type'])

        duration = None
        if row['duration']:
            parts = str(row['duration']).split()
            if parts[0].isdigit():
                duration = int(parts[0])

        cursor.execute("""
            INSERT INTO Fact_Content 
            (date_key, country_key, genre_key, rating_key, director_key, type_key,
             title, release_year, duration_minutes, title_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, date_key, country_key, genre_key, rating_key,
             director_key, type_key, row['title'], row['release_year'], duration)

    except Exception as e:
        print(f"Skipping row due to error: {e}")
        continue

conn.commit()
conn.close()

