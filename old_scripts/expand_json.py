# importing csv module


import csv

import time

import itertools

import json

from pathlib import Path

import shutil


def multigen(gen_func):
    class _multigen(object):

        def __init__(self, *args, **kwargs):
            self.__args = args

            self.__kwargs = kwargs

        def __iter__(self):
            return gen_func(*self.__args, **self.__kwargs)

    return _multigen


################################

#   SCANS

################################

@multigen
def scan_file(file_name, field_delimiter=","):
    # reading csv file

    with open(file_name, 'r') as csvfile:
        print(f'We opened file: {file_name}')

        # creating a csv reader object

        csvreader = csv.reader(csvfile, delimiter=field_delimiter)

        # the first row is the header

        fields = next(csvreader)

        yield fields

        # extracting each data row one by one

        for ligne in csvreader:
            # if csvreader.line_num % 10 == 0:

            #  print(ligne)

            yield ligne

        # get total number of rows

        # print("Total no. of rows: %d"%(csvreader.line_num))


@multigen
def scan_files(fList, field_delimiter=","):
    isFirst = True

    for file_name in fList:

        t = iter(scan_file(file_name, field_delimiter))

        header = next(t)

        if isFirst == True:
            yield header

            isFirst = False

        for row in t:
            yield row


################################

#   WRITERS

################################

def executeEndStore(stream, outputFile):
    counter = 0

    with open(outputFile, 'w', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')

        for resultat in stream:
            writer.writerow(resultat)

            counter += 1

    print(f'printed rows: {counter}')


################################

#  DATA STRUCTURES

################################

def get_column_as_set_of_values(table, attribute):
    it = iter(table)

    # get header

    header = next(it)

    # get indice for attribute

    column = header.index(attribute)

    # initiate the set

    values = set()

    for row in it:
        values.add(row[column])

    return values


################################

#   SELECTIONS

################################

@multigen
def selection_att_equals_value(table, attribute, valeur):
    it = iter(table)

    # pass the header along

    header = next(it)

    yield header

    # get the index of the column

    colonne = header.index(attribute)

    # filter

    for row in it:

        if row[colonne] == valeur:
            yield row


@multigen
def selection_att_not_equals_value(table, attribute, valeur):
    it = iter(table)

    # pass the header along

    header = next(it)

    yield header

    # get the index of the column

    colonne = header.index(attribute)

    # filter

    for row in it:

        if row[colonne] != valeur:
            yield row


@multigen
def selection_att_in_set(table, attribute, values):
    it = iter(table)

    # pass the header along

    header = next(it)

    yield header

    # get the index of the column

    colonne = header.index(attribute)

    for row in it:

        if row[colonne] in values:
            yield row


################################

#   PROJECTION

################################

@multigen
def projection(table, attributes):
    # extract first the header and pass it along

    it = iter(table)

    header = next(it)

    # get the indices of the columns of interest

    colonnes = []

    for att in attributes:
        colonnes.append(header.index(att))

    # form the new header and pass it along

    new_line = []

    for indice in colonnes:
        new_line.append(header[indice])

    yield new_line

    for row in it:

        new_line = []

        for indice in colonnes:
            new_line.append(row[indice])

        yield new_line


################################

#   JOINS

################################

def combineRows(row_left, row_right, positions_omitted_right):
    new_line = []

    for item in row_left:
        new_line.append(item)

    for index, item in enumerate(row_right):

        if index not in positions_omitted_right:
            new_line.append(item)

    return new_line


@multigen
def equi_join(table_left, table_right, list_of_pairs_of_attrs):
    # extract the header of the first table

    it_left = iter(table_left)

    header_left = next(it_left)

    was_new_header_generated = False

    # process the join

    for row_L in it_left:

        it_right = iter(table_right)

        header_right = next(it_right)

        # pass along the header if this hasn't been done yet

        if was_new_header_generated == False:

            # transform attributes in indices

            list_pairs = []

            for p in list_of_pairs_of_attrs:
                list_pairs.append([header_left.index(p[0]), header_right.index(p[1])])

            # prepare the list of positions from the right row

            # that are omited when merging two rows

            positions_omitted_right = []

            for item in list_pairs:
                positions_omitted_right.append(item[1])

            # create the new header

            new_header = combineRows(header_left, header_right, positions_omitted_right)

            yield new_header

            was_new_header_generated = True

        for row_R in it_right:

            can_combine = True

            for p in list_pairs:

                if row_L[p[0]] != row_R[p[1]]:
                    can_combine = False

                    break

            if can_combine == True:
                new_line = combineRows(row_L, row_R, positions_omitted_right)

                yield new_line


# for reasons of code re-usability and efficiency,

# we assume that the necessary set of values from the right table has been computed

# and is provided directly as input to this operator

# as a limitation, for now the semi-join is only over one attribute
def left_semi_join(left_table, left_attribute, value_set_in_right_table):
    it_left = iter(left_table)

    # header

    header_left = next(it_left)

    yield header_left

    # convert attribute in column indice

    left_column = header_left.index(left_attribute)

    # process the left table:

    for row in it_left:

        if row[left_column] in value_set_in_right_table:
            yield row

################################

#   EXPAND

################################

@multigen
def expand(table, attr_to_expand):
    # extract first the header and pass it along

    it = iter(table)

    header = next(it)

    # get the indices of the columns of interest

    indice = header.index(attr_to_expand)
    header += ["index", "sub-index"]

    #header does not change
    yield header

    counter_elem = 0
    for row in it:

        cell = row[indice]

        # Replace the value at index `indice` before cloning
        row[indice] = None  # Or another replacement value of your choice
         
        counter_elem += 1 

        # Extract the coordinates list
        data_dict = json.loads(cell)
        coordinates = data_dict['coordinates']

        subindex = 0
        # Process each point
        for point in coordinates:
            # Clone the modified row
            clone_row = row[:]
            clone_row[indice] = point
            subindex += 1 
            clone_row += [str(counter_elem), str(subindex)]
            yield clone_row

        
@multigen
def extract(table, attr_to_expand):
    # extract first the header and pass it along

    it = iter(table)

    header = next(it)

    # get the indices of the columns of interest

    indice = header.index(attr_to_expand)
   

    #header does not change
    yield header

    counter_elem = 0
    for row in it:

        cell = row[indice]

        # Replace the value at index `indice` before cloning
        row[indice] = None  # Or another replacement value of your choice
         
        # Extract the coordinates list
        data_dict = json.loads(cell)
        coordinates = data_dict["coordinates"]

        row[indice] = coordinates

        yield row


#############################################

#   SIMULATE DATA BLOCKS BY FILES

#############################################

def clean_directory(dir):
    print(f'Deleting directory {dir} ')

    dirpath = Path(dir)

    if dirpath.exists() and dirpath.is_dir():

        # remove all the files inside the directory

        [f.unlink() for f in dirpath.glob("*") if f.is_file()]

    else:

        dirpath.mkdir(parents=True, exist_ok=True)


def printListOfLinesToFile(new_file, header, list_lines):
    with open(new_file, 'w', newline='') as tsvfile:
        print(f'We are writing file {new_file} ')

        writer = csv.writer(tsvfile, delimiter='\t', lineterminator='\n')

        writer.writerow(header)

        for l in list_lines:
            writer.writerow(l)


def splitStreamInBlocs(table, output_dir, maxRows):
    counterFile = 0

    it = iter(table)

    header = next(it)

    list_lines = []

    counter_lines = 0

    for line in it:

        list_lines.append(line)

        counter_lines += 1

        if counter_lines >= maxRows:
            # write the lines to the file

            counterFile += 1

            new_file = output_dir + "/" + str(counterFile) + ".tsv"

            printListOfLinesToFile(new_file, header, list_lines)

            # restart the varibles

            list_lines = []

            counter_lines = 0

    # print the remaining lines

    counterFile += 1

    new_file = output_dir + "/" + str(counterFile) + ".tsv"

    printListOfLinesToFile(new_file, header, list_lines)

    return counterFile;


def get_all_files_from_dir(pathDir):
    path = Path(pathDir)

    return list(path.iterdir())


################################

#   MAIN

################################

def extract_stations(new_file):
    print("Start extractings stations")

    t1 = scan_file("./data/gares-et-stations-du-reseau-ferre-dile-de-france-par-ligne@datailedefrance.csv", field_delimiter=";")

    t2 = projection(t1, ["Geo Shape","nom_long","id_ref_ZdC","nom_ZdC","res_com"])

    t3 = extract(t2, "Geo Shape")

    executeEndStore(t3, new_file)
    

def extract_traces(new_file):
    print("Start extracting traces")

    t1 = scan_file("./data/traces-du-reseau-ferre-idf.csv", field_delimiter=";")

    t2 = projection(t1, ["Geo Shape","res_com"])

    t3 = expand(t2, "Geo Shape")

    executeEndStore(t3, new_file)


def join_station_and_traces(new_file):
    print("Start extracting traces")

    t1 = scan_file("./new-dataset/_stations.tsv", field_delimiter="\t")

    t2 = scan_file("./new-dataset/_traces.tsv", field_delimiter="\t")

    t3 = equi_join(t1, t2, [("Geo Shape", "Geo Shape"), ("res_com", "res_com")])

    executeEndStore(t3, new_file)


clean_directory("./new-dataset")

extract_stations("./new-dataset/_stations.tsv")

extract_traces("./new-dataset/_traces.tsv")

join_station_and_traces("./new-dataset/_join.tsv")