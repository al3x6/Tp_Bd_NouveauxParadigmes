import csv
import json

# Chemins des fichiers TSV
arrets_lignes_file = '../mini-dataset/_mini_arrets_lignes.tsv'
trips_file = '../mini-dataset/_mini_trips.tsv'
stops_file = '../mini-dataset/_mini_stops.tsv'
stop_times_file = '../mini-dataset/_mini_stop_times.tsv'

# Dictionnaire pour stocker temporairement les données par routeId
route_data_dict = {}

# Extraction des données depuis le fichier stops times
# with open(stop_times_file, 'r', encoding='utf-8') as file:
#     reader = csv.DictReader(file, delimiter='\t')
#     for row in reader:
#         stope_time_data = {
#             "arrivalTime": row["arrival_time"],
#             "dapartureTime": row["departure_time"],
#             "tripId": row["trip_id"]
#         }

# Extraction des données depuis le fichier stops TSV
with open(stops_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        stop_data = {
            "stopId": row["stop_id"],
            "stopName": row["stop_name"],
            "stopLon": row.get("stop_lon", ""),
            "stopLat": row.get("stop_lat", ""),
            "operatorName": row.get("OperatorName", ""),
            "nomCommune": row.get("Nom_commune", ""),
            "codeInsee": row.get("Code_insee", ""),
            "stopSequence": "",
            "listArrivalsTime": []
        }
        #route_data_dict["listArrivalsTime"].append(stope_time_data)
        print(stop_data)


#Extraction des données depuis le fichier stops TSV
with open(trips_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        route_id = row["route_id"]
        if route_id not in route_data_dict:
            route_data_dict[route_id] = {
                "routeId": route_id,
                "tripHeadsign": row["trip_headsign"],
                "idArretDepart": row["trip_id"],
                "listeArrets": []
            }
        route_data_dict[route_id]["listeArrets"].append(stop_data)
#print(route_data_dict[route_id])

# Construction du JSON final
final_json = json.dumps(list(route_data_dict.values()), indent=4)

# Écriture des données JSON dans un fichier
with open('../JSONV2/CollectTrips.json', 'w', encoding='utf-8') as trips_file:
    trips_file.write(final_json)