import csv
import json

# Chemins des fichiers TSV
arrets_lignes_file = '../mini-dataset/_mini_arrets_lignes.tsv'
trips_file = '../mini-dataset/_mini_trips.tsv'
stops_file = '../mini-dataset/_mini_stops.tsv'
stop_times_file = '../mini-dataset/_mini_stop_times.tsv'

# Dictionnaire pour stocker temporairement les données par routeId
route_data_dict = {}

# Dictionnaire temporaire pour stocker les données des arrêts par stopId
stops_data_dict = {}

# Extraction des données depuis le fichier stops times
with open(stop_times_file, 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file, delimiter='\t')
    for row in reader:
        stope_time_data = {
            "arrivalTime": row["arrival_time"],
            "dapartureTime": row["departure_time"],
            "tripId": row["trip_id"]
        }
#print(stope_time_data)

# Extraction des données depuis le fichier stops TSV
# with open(stops_file, 'r', encoding='utf-8') as file:
#     reader = csv.DictReader(file, delimiter='\t')
#     for row in reader:
#         stop_id = row["stop_id"]
#         if stop_id not in stops_data_dict:
#             stops_data_dict[stop_id] = {
#                 "stopId": stop_id,
#                 "stopName": row["stop_name"],
#                 "stopLon": row.get("stop_lon", ""),
#                 "stopLat": row.get("stop_lat", ""),
#                 "operatorName": row.get("OperatorName", ""),
#                 "nomCommune": row.get("Nom_commune", ""),
#                 "codeInsee": row.get("Code_insee", ""),
#                 "stopSequence": "",
#                 "listArrivalsTime": []  # Associer les données temporelles aux arrêts
#             }
#         stops_data_dict[stop_id]["listArrivalsTime"].append(stope_time_data)
#         print(stops_data_dict[stop_id])
#         data_stop=stops_data_dict[stop_id]
        #print(data_stop)   #ici le data_stop il me sort toutes les valeurs/ toutes les chaines json

# Extraction des données depuis le fichier trips TSV
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


        # Extraction des données depuis le fichier stops TSV
        with open(stops_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for row in reader:
                stop_id = row["stop_id"]
                if stop_id not in stops_data_dict:
                    stops_data_dict[stop_id] = {
                        "stopId": stop_id,
                        "stopName": row["stop_name"],
                        "stopLon": row.get("stop_lon", ""),
                        "stopLat": row.get("stop_lat", ""),
                        "operatorName": row.get("OperatorName", ""),
                        "nomCommune": row.get("Nom_commune", ""),
                        "codeInsee": row.get("Code_insee", ""),
                        "stopSequence": "",
                        "listArrivalsTime": []  # Associer les données temporelles aux arrêts
                    }
                stops_data_dict[stop_id]["listArrivalsTime"].append(stope_time_data)
                # print(stops_data_dict[stop_id])
                data_stop = stops_data_dict[stop_id]
                #print(data_stop)  # ici le data_stop il me sort toutes les valeurs/ toutes les chaines json

                route_data_dict[route_id]["listeArrets"].append(data_stop)
        #print(data_stop)   #ici le data_stop me sort toujours la même valeur

# Construction du JSON final
final_json = json.dumps(list(route_data_dict.values()), indent=4)

# Écriture des données JSON dans un fichier
with open('CollectTrips.json', 'w', encoding='utf-8') as output_file:
    output_file.write(final_json)
