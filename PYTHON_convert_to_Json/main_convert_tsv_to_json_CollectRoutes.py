import csv
import json

# Chemin du fichier TSV
routes_file = '../mini-dataset/_mini_routes.tsv'

# Liste pour stocker les données des routes
collect_routes_data = []

# Extraction des données depuis le fichier routes TSV
with open(routes_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        route_data = {
            "route_id": row["route_id"],
            "route_short_name": row["route_short_name"],
            "route_long_name": row["route_long_name"]
        }
        collect_routes_data.append(route_data)


# Conversion en JSON
json_collect_routes = json.dumps(collect_routes_data, indent=4)

# Affichage du JSON résultant
#print(json_collect_routes)

with open('../JSONV2/CollectRoutes.json', 'w', encoding='utf-8') as routes_file:
    routes_file.write(json_collect_routes)
