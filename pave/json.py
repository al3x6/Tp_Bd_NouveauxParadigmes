from .utils import multigen

################################

#   EXRACT json

################################

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

