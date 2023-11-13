import json

file = open("fr.json")
france_cities = json.load(file)
idf_major_cities = []

PARIS_AREA_NAME = "Île-de-France"
LARGE_POPULATION_NUMBER = 50000

for index, city in enumerate(france_cities):
    if (
        city["admin_name"] == PARIS_AREA_NAME
        and int(city["population"]) >= LARGE_POPULATION_NUMBER
    ):
        major_idf_city = {
            "City_index": index,
            "Latitude": city["lat"],
            "Longitude": city["lng"],
            "City": city["city"],
        }
        idf_major_cities.append(major_idf_city)

print(f"len of major cities in idf: {len(idf_major_cities)}")

with open("idf_major_cities.json", "w") as f:
    json.dump(idf_major_cities, f, ensure_ascii=False)
