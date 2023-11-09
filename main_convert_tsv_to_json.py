import csv
import json

# Définir la structure des collections (vides pour l'instant)
arrets_lignes = []
routes = []
stop_times = []
stops = []
trips = []

# Fonction pour lire un fichier TSV et convertir en JSON
def convert_tsv_to_json(tsv_filename, json_list):
    with open(tsv_filename, mode='r', newline='') as tsv_file:
        tsv_reader = csv.DictReader(tsv_file, delimiter='\t')
        for row in tsv_reader:
            json_list.append(row)

# Lire et convertir les fichiers TSV en JSON
convert_tsv_to_json('./new-dataset/_small_arrets_lignes.tsv', arrets_lignes)
convert_tsv_to_json('./new-dataset/_small_routes.tsv', routes)
convert_tsv_to_json('./new-dataset/_small_stop_times.tsv', stop_times)
convert_tsv_to_json('./new-dataset/_small_stops.tsv', stops)
convert_tsv_to_json('./new-dataset/_small_trips.tsv', trips)

# Enregistrer les données JSON dans des fichiers
with open('./json/arrets_lignes.json', 'w') as arrets_lignes_json_file:
    json.dump(arrets_lignes, arrets_lignes_json_file, indent=4)

with open('./json/routes.json', 'w') as routes_json_file:
    json.dump(routes, routes_json_file, indent=4)

with open('./json/stop_times.json', 'w') as stop_times_json_file:
    json.dump(stop_times, stop_times_json_file, indent=4)

with open('./json/stops.json', 'w') as stops_json_file:
    json.dump(stops, stops_json_file, indent=4)

with open('./json/trips.json', 'w') as trips_json_file:
    json.dump(trips, trips_json_file, indent=4)
