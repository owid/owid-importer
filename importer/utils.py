import hashlib

def yesno(question):
    reply = input(question + ' (y/n): ').lower().strip()
    if reply[:1] == 'y':
        return True
    elif reply[:1] == 'n':
        return False
    else:
        return yesno("Sorry, was that a yes or a no?")

def strlist(iterable):
    return ", ".join(str(i) for i in iterable)

def get_row_values(row):
    get_cell_value = lambda cell: cell.value
    return tuple(get_cell_value(cell) for cell in row)

def starts_with(seq, start_seq):
    for i in range(len(start_seq)):
        if seq[i] != start_seq[i]:
            return False
    return True

def default(value, default_value=None):
    return value if value else default_value

# we will use the file checksum to check if the downloaded file has changed since we last saw it
def file_checksum(filename, blocksize=2**20):
    m = hashlib.md5()
    with open(filename, "rb") as f:
        while True:
            buffer = f.read(blocksize)
            if not buffer:
                break
            m.update(buffer)
    return m.hexdigest()

def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item

def extract_short_unit(unit: str):
    common_short_units = ['$', '£', '€', '%']  # used for extracting short forms of units of measurement
    short_unit = None
    if unit:
        if ' per ' in unit:
            short_form = unit.split(' per ')[0]
            if any(w in short_form for w in common_short_units):
                for x in common_short_units:
                    if x in short_form:
                        short_unit = x
                        break
            else:
                short_unit = short_form
        elif any(x in unit for x in common_short_units):
            for y in common_short_units:
                if y in unit:
                    short_unit = y
                    break
        elif 'percent' in unit.lower():
            short_unit = '%'
        elif len(unit) < 9:  # this length is sort of arbitrary at this point, taken from the unit 'hectares'
            short_unit = unit
    return short_unit
