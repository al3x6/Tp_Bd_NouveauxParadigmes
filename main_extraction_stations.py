
from pave.io import scan_file
from pave.io import executeEndStore
from pave.io import clean_directory

from pave.op import projection
from pave.op import selection_att_in_set
from pave.op import distinct
from pave.op import get_column_as_set_of_values
from pave.op import left_semi_join

from pave.formatting import add_prefix

from pathlib import Path

################################

#   MAIN

################################
def extract_arrets_lignes(new_file):
    print("Start extractings stations")

    t1 = scan_file("./data/arrets-lignes.csv", field_delimiter=";")
 
    t2 = projection(t1, ["\ufeffroute_id","stop_id","stop_name","stop_lon","stop_lat","OperatorName", "Nom_commune", "Code_insee"])

    t3 = selection_att_in_set(t2,  "OperatorName", {"RATP", "SNCF"})

    executeEndStore(t3, new_file)


def extract_simply_stops(new_file):
    print("Start extractings stops")

    t1 = scan_file("./data/IDFM-gtfs/stops.txt", field_delimiter=",")
 
    t2 = projection(t1, ["stop_id","stop_name","parent_station"])

    executeEndStore(t2, new_file)
    
def extract_simply_routes(new_file):
    print("Start extractings routes")

    t1 = scan_file("./data/IDFM-gtfs/routes.txt", field_delimiter=",")
 
    t2 = projection(t1, ["route_id","route_short_name","route_long_name"])

    executeEndStore(t2, new_file)
 

def extract_simply_trips(new_file):
    print("Start extractings trips")

    t1 = scan_file("./data/IDFM-gtfs/trips.txt", field_delimiter=",")
 
    t2 = projection(t1, ["route_id","trip_id","trip_headsign"])

    executeEndStore(t2, new_file)
 

def extract_simply_stop_times(new_file):
    print("Start extractings trips")

    t1 = scan_file("./data/IDFM-gtfs/stop_times.txt", field_delimiter=",")
 
    t2 = projection(t1, ["trip_id","stop_id","stop_sequence","arrival_time","departure_time"])

    executeEndStore(t2, new_file)

def extract_lignes_trains_et_metro(new_file):
    print("Start extractings lines")

    t1 = scan_file("./data/emplacement-des-gares-idf.csv", field_delimiter=";")
 
    t2 = projection(t1, ["idrefligc","res_com", "indice_lig",	"mode_"])

    t3 = distinct(t2)

    t4 = add_prefix(t3, "idrefligc", "IDFM:")

    executeEndStore(t4, new_file)


def filtre_arrets_lignes(new_file):
    print("Start filtering stations (keep only metro and train stations)")

    t1 = scan_file("./new-dataset/_project_arrets_lignes.tsv", field_delimiter="\t")
    t2 = scan_file("./new-dataset/_project_emplacement-des-gares-idf.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "idrefligc")
    t3 = left_semi_join(t1, "\ufeffroute_id", s1)
    executeEndStore(t3, new_file)


def filtre_routes(new_file):
    print("Start filtering routes (keep only railway lines)")

    t1 = scan_file("./new-dataset/_project_routes.tsv", field_delimiter="\t")

    t2 = scan_file("./new-dataset/_project_emplacement-des-gares-idf.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "idrefligc")

    t3 = left_semi_join(t1, "route_id", s1)
    
    executeEndStore(t3, new_file)



def filtre_trips(new_file):
    print("Start filtering trips (type de train)")
    t1 = scan_file("./new-dataset/_project_trips.tsv", field_delimiter="\t")
    t2 = scan_file("./new-dataset/_small_routes.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "route_id")
    t3 = left_semi_join(t1, "route_id", s1)
    executeEndStore(t3, new_file)



def filtre_stop_times(new_file):
    print("Start filtering trips (type de train)")

    t1 = scan_file("./new-dataset/_project_stop_times.tsv", field_delimiter="\t")

    t2 = scan_file("./new-dataset/_small_trips.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "trip_id")

    t3 = left_semi_join(t1, "trip_id", s1)
    
    executeEndStore(t3, new_file)

def filtre_stops(new_file):
    print("Start filtering stops")

    t1 = scan_file("./new-dataset/_project_stops.tsv", field_delimiter="\t")

    t2 = scan_file("./new-dataset/_small_stop_times.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "stop_id")

    t3 = left_semi_join(t1, "stop_id", s1)

    executeEndStore(t3, new_file)


def create_small_db():
    clean_directory("./new-dataset")

    extract_arrets_lignes("./new-dataset/_project_arrets_lignes.tsv")

    extract_simply_stops("./new-dataset/_project_stops.tsv")

    extract_simply_routes("./new-dataset/_project_routes.tsv")

    extract_simply_trips("./new-dataset/_project_trips.tsv")

    extract_simply_stop_times("./new-dataset/_project_stop_times.tsv")

    extract_lignes_trains_et_metro("./new-dataset/_project_emplacement-des-gares-idf.tsv")

    filtre_arrets_lignes("./new-dataset/_small_arrets_lignes.tsv")

    filtre_routes("./new-dataset/_small_routes.tsv")

    filtre_trips("./new-dataset/_small_trips.tsv")

    filtre_stop_times("./new-dataset/_small_stop_times.tsv")

    filtre_stops("./new-dataset/_small_stops.tsv")

    print(f'Deleting files _project_* from {dir} ')

    dirpath = Path("./new-dataset")

    [f.unlink() for f in dirpath.glob("_project_*") if f.is_file()]


def create_mini_db():
    clean_directory("./mini-dataset")

    print("Processing routes")
    t1 = scan_file("./new-dataset/_small_routes.tsv", field_delimiter="\t")
    t2 = selection_att_in_set(t1,  "route_short_name", {"1", "4", "6"})
    executeEndStore(t2, "./mini-dataset/_mini_routes.tsv")

    print("Processing trips 1.")
    t1 = scan_file("./new-dataset/_small_trips.tsv", field_delimiter="\t")
    t2 = scan_file("./mini-dataset/_mini_routes.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "route_id")
    t3 = left_semi_join(t1, "route_id", s1) #still very big file as there as every train has another id
    executeEndStore(t3, "./mini-dataset/_tmp_trips.tsv")


    print("Processing trips 2.")



    print("Processing mini stop times")
    t1 = scan_file("./new-dataset/_small_stop_times.tsv", field_delimiter="\t")
    t2 = scan_file("./mini-dataset/_mini_trips.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "trip_id")
    t3 = left_semi_join(t1, "trip_id", s1)
    executeEndStore(t3, "./mini-dataset/_mini_stop_times.tsv")

    print("Processing stops")
    t1 = scan_file("./new-dataset/_small_stops.tsv", field_delimiter="\t")
    t2 = scan_file("./mini-dataset/_mini_stop_times.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "stop_id")
    t3 = left_semi_join(t1, "stop_id", s1)
    executeEndStore(t3, "./mini-dataset/_mini_stops.tsv")


    print("Processing arrets lignes")
    t1 = scan_file("./new-dataset/_small_arrets_lignes.tsv", field_delimiter="\t")
    t2 = scan_file("./mini-dataset/_mini_stops.tsv", field_delimiter="\t")
    s1 = get_column_as_set_of_values(t2, "stop_id")
    t3 = left_semi_join(t1, "stop_id", s1)
    executeEndStore(t3, "./mini-dataset/_mini_arrets_lignes.tsv")
 



#create_small_db()

create_mini_db()
