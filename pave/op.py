from .utils import multigen


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

#   DISTINCT

################################

@multigen
def distinct(table):
    it = iter(table)
    header = next(it)
    yield header

    seen = set()  # A set to keep track of seen rows
    for row in it:
        # Convert the row to a tuple since lists are not hashable and cannot be added to a set
        tuple_row = tuple(row)
        if tuple_row not in seen:
            seen.add(tuple_row)
            yield row

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

   
    print(f'header for projection: {header} ')  


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
#   LIMIT
################################

@multigen
def limit(table, limit):
    it = iter(table)
    count = 0  # Counter to keep track of yielded rows

    for row in it:
        if count >= limit:
            break
        yield row
        count += 1