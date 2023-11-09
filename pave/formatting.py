from .utils import multigen


@multigen
def add_prefix(table, attribute, prefix):
    it = iter(table)

    # Pass the header along
    header = next(it)
    yield header

    # Get the index of the column for which the prefix needs to be added
    column_index = header.index(attribute)

    # Process rows
    for row in it:
        # Modify the value of the specified attribute by adding the prefix
        row[column_index] = prefix + str(row[column_index])
        yield row