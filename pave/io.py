from .utils import multigen
from pathlib import Path
import csv


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
