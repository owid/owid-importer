import sys
import os
import hashlib
import csv
import json
import logging
import requests
import shutil
import zipfile
import time
from datetime import datetime
from openpyxl import load_workbook

# allow imports from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from db import connection
from utils import file_checksum, extract_short_unit, get_row_values, starts_with, default, yesno, strlist
from db_utils import normalise_country_name

DATASET_NAMESPACE = 'wdi'
PARENT_TAG_NAME = 'World Development Indicators'  # set the name of the root category of all data that will be imported by this script
ZIP_FILE_URL = 'http://databank.worldbank.org/data/download/WDI_excel.zip'
FIRST_YEAR = 1960

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
DOWNLOADS_PATH = os.path.join(CURRENT_PATH, '..', 'data', 'wdi')

DEFAULT_SOURCE_DESCRIPTION = {
    'dataPublishedBy': 'World Bank – World Development Indicators',
    'link': 'http://data.worldbank.org/data-catalog/world-development-indicators',
    'retrievedDate': time.strftime('%d-%B-%y')
}

# The column headers we expect the sheets to have.
# We will only check that the headers begin with the columns listed here, if
# there are additional columns, that's fine and it shouldn't affect our script.
SERIES_EXPECTED_HEADERS = ('Series Code', 'Topic', 'Indicator Name', 'Short definition', 'Long definition', 'Unit of measure', 'Periodicity', 'Base Period', 'Other notes', 'Aggregation method', 'Limitations and exceptions', 'Notes from original source', 'General comments', 'Source', 'Statistical concept and methodology', 'Development relevance', 'Related source links', 'Other web links', 'Related indicators', 'License Type')
DATA_EXPECTED_HEADERS = ('Country Name', 'Country Code', 'Indicator Name', 'Indicator Code', str(FIRST_YEAR))
COUNTRY_EXPECTED_HEADERS = ('Country Code', 'Short Name', 'Table Name', 'Long Name', '2-alpha code', 'Currency Unit', 'Special Notes', 'Region', 'Income Group', 'WB-2 code', 'National accounts base year', 'National accounts reference year', 'SNA price valuation', 'Lending category', 'Other groups', 'System of National Accounts', 'Alternative conversion factor', 'PPP survey year', 'Balance of Payments Manual in use', 'External debt Reporting status', 'System of trade', 'Government Accounting concept', 'IMF data dissemination standard', 'Latest population census', 'Latest household survey', 'Source of most recent Income and expenditure data', 'Vital registration complete', 'Latest agricultural census', 'Latest industrial data', 'Latest trade data')

logging.basicConfig(
    filename=os.path.join(CURRENT_PATH, '..', 'logs', '%s-wdi.log' % (os.environ['DB_NAME'])),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)

logger = logging.getLogger('importer')

def info(message):
    print(message)
    logger.info(message)

def terminate(message):
    logger.error(message)
    logger.info("Terminating script...")
    sys.exit(1)

def dataset_name_from_category(category):
    return 'World Development Indicators - ' + category

def normalise_indicator_code(code):
    return code.upper().strip()

# Extract indicator from row in Series worksheet
def indicator_from_row(row):
    values = get_row_values(row)

    code = normalise_indicator_code(values[0])
    category = values[1].split(':')[0]
    name = values[2]

    source_description = {
        **DEFAULT_SOURCE_DESCRIPTION,
        'dataPublisherSource': values[13],
        'additionalInfo': '\n'.join([
            title + ': ' + content
            for (title, content) in [
                ('Limitations and exceptions', values[10]),
                ('Notes from original source', values[11]),
                ('General comments', values[12]),
                ('Statistical concept and methodology', values[14]),
                ('Related source links', values[16]),
                ('Other web links', values[17])
            ]
            if content
        ])
    }

    # override dataPublishedBy if from the IEA
    if 'iea.org' in json.dumps(source_description).lower() or 'iea stat' in json.dumps(source_description).lower() or 'iea 2014' in json.dumps(source_description).lower():
        source_description['dataPublishedBy'] = 'International Energy Agency (IEA) via The World Bank'

    indicator = {
        'variableId': None,
        'datasetId': None,
        'sourceId': None,
        'code': code,
        'category': category,
        'datasetName': dataset_name_from_category(category),
        'name': name,
        'description': default(values[4], ''),
        'unit': default(values[5], ''),
        'shortUnit': None, # derived below
        'source': {
            'name': 'World Bank - WDI: ' + name,
            'description': json.dumps(source_description)
        }
    }
    # if no unit is specified, try to derive it from the name
    if not indicator['unit'] and '(' in indicator['name'] and ')' in indicator['name']:
        indicator['unit'] = indicator['name'][
            indicator['name'].rfind('(') + 1:
            indicator['name'].rfind(')')
        ]
    # derive the short unit
    indicator['shortUnit'] = extract_short_unit(indicator['unit'])
    return indicator

# Create a directory for holding the downloads.
if not os.path.exists(DOWNLOADS_PATH):
    os.makedirs(DOWNLOADS_PATH)

excel_filepath = os.path.join(DOWNLOADS_PATH, 'WDIEXCEL.xlsx')

if not os.path.isfile(excel_filepath) or yesno("The spreadsheet has been downloaded before. Download latest version?"):
    info("Getting the zip file...")
    request_header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(ZIP_FILE_URL, stream=True, headers=request_header)
    if r.ok:
        zip_filepath = os.path.join(DOWNLOADS_PATH, 'wdi.zip')
        with open(zip_filepath, 'wb') as out_file:
            shutil.copyfileobj(r.raw, out_file)
        info("Saved the zip file to disk.")
        z = zipfile.ZipFile(zip_filepath)
        z.extractall(DOWNLOADS_PATH)
        r = None  # we do not need the request anymore
        info("Successfully extracted the zip file.")
    else:
        terminate("The ZIP file could not be downloaded.")

# ==============================================================================
# Load the worksheets we need and check that they have the columns we expect.
# ==============================================================================

wb = load_workbook(excel_filepath, read_only=True)

series_ws_rows = wb['Series'].rows
data_ws_rows = wb['Data'].rows
country_ws_rows = wb['Country'].rows

series_headers = get_row_values(next(series_ws_rows))
data_headers = get_row_values(next(data_ws_rows))
country_headers = get_row_values(next(country_ws_rows))

# NOTE at this point, the rows generators have yielded once to retrieve the
# headers, so they will yield a data row next.

if not starts_with(series_headers, SERIES_EXPECTED_HEADERS):
    terminate("Headers mismatch on 'Series' worksheet")
if not starts_with(data_headers, DATA_EXPECTED_HEADERS):
    terminate("Headers mismatch on 'Data' worksheet")
if not starts_with(country_headers, COUNTRY_EXPECTED_HEADERS):
    terminate("Headers mismatch on 'Country' worksheet")


# ==============================================================================
# Initialise the data structures to track the state of the import.
# ==============================================================================

last_available_year = int(data_headers[-1])
timespan = str(FIRST_YEAR) + "-" + str(last_available_year)

indicators = [
    indicator_from_row(row)
    for row in series_ws_rows
]

indicator_by_code = {
    indicator['code']: indicator
    for indicator in indicators
}

# convert to set to dedupe
categories = list({
    indicator['category']
    for indicator in indicators
})

country_name_by_code = dict(
    (get_row_values(row)[0].upper().strip(), get_row_values(row)[2])
    for row in country_ws_rows
)

# Data sheet uses INX for 'Not classified', but Country sheet does not list it.
country_name_by_code['INX'] = 'Not classified'


# Using the connection as context creates an implicit transaction:
# https://github.com/PyMySQL/PyMySQL/blob/3ab3b275e3d60be733f2c3f1bf6cfd644863466c/pymysql/connections.py#L496-L505
# `c` is a cursor.
with connection as c:

    c.execute("""
        SELECT id FROM users WHERE email = 'daniel@gavrilov.co.uk'
    """)

    # The user ID that gets assigned in every user ID field
    user_id = c.fetchone()[0]

    # ==========================================================================
    # Rename any variable codes that changed since last import.
    # Make sure to update wdi_code_changes.csv with the latest code changes
    # which you can find on the WDI download website.
    # ==========================================================================

    c.execute("""
        SELECT import_time FROM importer_importhistory
        ORDER BY import_time DESC
        LIMIT 1
    """)

    last_import_time = c.fetchone()[0]
    last_code_change_time = None

    # Before 29 November 2018 we didn't handle code changes.
    # The import was first run on 6 July 2017, so we want to commit all code
    # changes since then.
    if last_import_time < datetime.strptime('29-Nov-18', '%d-%b-%y'):
        last_code_change_time = datetime.strptime('07-Jul-17', '%d-%b-%y')
    else:
        last_code_change_time = last_import_time

    code_changes = []

    with open(os.path.join(CURRENT_PATH, 'wdi_code_changes.csv')) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        next(reader) # skip headers
        for datestr, old, new in reader:
            date = datetime.strptime(datestr, '%d-%b-%y')
            old = normalise_indicator_code(old)
            new = normalise_indicator_code(new)
            if date > last_code_change_time:
                code_changes.append((date, old, new))

    code_changes = sorted(code_changes)

    for _, old_name, new_name in code_changes:
        c.execute("""
            UPDATE variables SET code = %(new_name)s
            WHERE code = %(old_name)s
        """, {
            'new_name': new_name,
            'old_name': old_name
        })


    # ==========================================================================
    # Clear the database from old data_values, variables & sources.
    # ==========================================================================

    # Get all variables that are in our database but not in the spreadsheet.
    c.execute("""
        SELECT DISTINCT
            variables.id,
            chart_dimensions.id IS NOT NULL
        FROM variables
        LEFT JOIN datasets ON datasets.id = variables.datasetId
        LEFT JOIN chart_dimensions ON chart_dimensions.variableId = variables.id
        WHERE
            datasets.namespace = %(namespace)s
            AND variables.code NOT IN %(all_codes)s
    """, {
        'namespace': DATASET_NAMESPACE,
        'all_codes': list(indicator_by_code.keys())
    })

    variables_to_maybe_remove = list(c.fetchall()) # convert to list, we will iterate it twice

    # Variables that are no longer present in the spreadsheet will be
    # removed if no chart uses them. Otherwise, they will be left to be
    # manually investigated (some variables may have been renamed).
    var_ids_to_remove      = [var_id for var_id, is_used in variables_to_maybe_remove if not is_used]
    var_ids_to_discontinue = [var_id for var_id, is_used in variables_to_maybe_remove if is_used]

    assert len(set(var_ids_to_remove) & set(var_ids_to_discontinue)) == 0

    # Store the source IDs that need to be removed. Since variables need to
    # be removed first, we will lose the reference to the unused sources
    # unless we save them.
    if var_ids_to_remove:

        c.execute("""
            SELECT DISTINCT sources.id
            FROM sources
            LEFT JOIN variables ON variables.sourceId = sources.id
            WHERE variables.id IN %s
        """, [var_ids_to_remove])

        source_ids_to_remove = [source_id for source_id in c.fetchall()]

        c.execute("""
            DELETE FROM data_values
            WHERE variableId IN %s
        """, [var_ids_to_remove])

        c.execute("""
            DELETE FROM variables
            WHERE id IN %s
        """, [var_ids_to_remove])

        # Only delete the sources if there are no variables referencing them
        if source_ids_to_remove:
            c.execute("""
                DELETE sources
                FROM sources
                LEFT JOIN variables ON variables.sourceId = sources.id
                WHERE
                    variables.id IS NULL
                    AND sources.id IN %s
            """, [source_ids_to_remove])

    # ==========================================================================
    # Upsert the tags & datasets.
    # ==========================================================================

    # If the parent tag doesn't exist, create it & store the ID

    def get_parent_tag_id():
        c.execute("""
            SELECT id
            FROM tags
            WHERE name = %s
            AND isBulkImport = TRUE
            AND parentId IS NULL
            LIMIT 1
        """, [PARENT_TAG_NAME])
        try:
            return c.fetchone()[0]
        except:
            return None

    parent_tag_id = get_parent_tag_id()

    if parent_tag_id is None:
        c.execute("""
            INSERT INTO tags (name, createdAt, updatedAt, isBulkImport)
            VALUES (%s, NOW(), NOW(), TRUE)
        """, [PARENT_TAG_NAME])
        parent_tag_id = get_parent_tag_id()

    # Upsert all subcategories
    # Relies on (name, parentId) UNIQUE constraint
    c.executemany("""
        INSERT INTO
            tags (name, parentId, createdAt, updatedAt, isBulkImport)
        VALUES
            (%s, %s, NOW(), NOW(), TRUE)
        ON DUPLICATE KEY UPDATE
            updatedAt = VALUES(updatedAt),
            isBulkImport = VALUES(isBulkImport)
    """, [
        (category, parent_tag_id)
        for category in categories
    ])

    c.execute("""
        SELECT name, id
        FROM tags
        WHERE parentId = %s
    """, [parent_tag_id])

    tag_id_by_category_name = {
        name: tag_id
        for name, tag_id in c.fetchall()
    }

    dataset_names_to_upsert = {
        dataset_name_from_category(category)
        for category in categories
    }

    # Upsert all the datasets
    # Relies on (name, namespace) UNIQUE constraint to detect duplicates
    c.executemany("""
        INSERT INTO
            datasets (name, description, namespace, createdAt, createdByUserId, updatedAt, metadataEditedAt, metadataEditedByUserId, dataEditedAt, dataEditedByUserId)
        VALUES
            (%s, %s, %s, NOW(), %s, NOW(), NOW(), %s, NOW(), %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            description = VALUES(description),
            namespace = VALUES(namespace),
            updatedAt = VALUES(updatedAt),
            metadataEditedAt = VALUES(metadataEditedAt),
            metadataEditedByUserId = VALUES(metadataEditedByUserId),
            dataEditedAt = VALUES(dataEditedAt),
            dataEditedByUserId = VALUES(dataEditedByUserId)
    """, [
        (name, 'This is a dataset imported by the automated fetcher', DATASET_NAMESPACE, user_id, user_id, user_id)
        for name in dataset_names_to_upsert
    ])

    c.execute("""
        SELECT name, id
        FROM datasets
        WHERE namespace = %s
    """, [DATASET_NAMESPACE])

    dataset_id_by_name = {
        name: d_id
        for name, d_id in c.fetchall()
    }

    for indicator in indicators:
        indicator['datasetId'] = dataset_id_by_name[indicator['datasetName']]

    # Associate each dataset with the appropriate tag
    c.executemany("""
        INSERT INTO
            dataset_tags (datasetId, tagId)
        VALUES
            (%s, %s)
        ON DUPLICATE KEY UPDATE
            tagId = VALUES(tagId)
    """, [ # ON DUPLICATE here only avoids error, it intentionally updates nothing
        (dataset_id_by_name[dataset_name_from_category(cat)], tag_id_by_category_name[cat])
        for cat in categories
    ])

    # ==========================================================================
    # Upsert the variables & sources.
    # ==========================================================================

    # Retrieve all variables and their sourceIds

    c.execute("""
        SELECT
            variables.code,
            variables.id,
            variables.sourceId
        FROM variables
        LEFT JOIN datasets ON datasets.id = variables.datasetId
        WHERE datasets.namespace = %s
    """, [DATASET_NAMESPACE])

    for code, var_id, source_id in c.fetchall():
        if code in indicator_by_code:
            indicator_by_code[code]['variableId'] = var_id
            indicator_by_code[code]['sourceId'] = source_id

    sources_to_update = [ind for ind in indicators if ind['sourceId']]
    sources_to_add    = [ind for ind in indicators if not ind['sourceId']]

    variables_to_update = [ind for ind in indicators if ind['variableId']]
    variables_to_add    = [ind for ind in indicators if not ind['variableId']]

    # Update the existing sources
    # This is actually a bulk UPDATE, since the primary key always clashes
    c.executemany("""
        INSERT INTO
            sources (id, name, description, datasetId, createdAt, updatedAt)
        VALUES
            (%s, %s, %s, %s, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            description = VALUES(description),
            datasetId = VALUES(datasetId),
            updatedAt = VALUES(updatedAt)
    """, [
        (x['sourceId'], x['source']['name'], x['source']['description'], x['datasetId'])
        for x in sources_to_update
    ])

    # Create new sources.
    c.executemany("""
        INSERT INTO
            sources (name, description, datasetId, createdAt, updatedAt)
        VALUES
            (%s, %s, %s, NOW(), NOW())
    """, [
        (x['source']['name'], x['source']['description'], x['datasetId'])
        for x in sources_to_add
    ])

    # Populate the sourceId field in all the indicators

    c.execute("""
        SELECT
            sources.name,
            sources.id
        FROM sources
        LEFT JOIN datasets ON datasets.id = sources.datasetId
        WHERE datasets.namespace = %s
    """, [DATASET_NAMESPACE])

    source_id_by_name = {
        name: i
        for name, i in c.fetchall()
    }

    for indicator in indicators:
        if not indicator['sourceId']:
            indicator['sourceId'] = source_id_by_name[indicator['source']['name']]

    # Update the existing variables
    # This is actually a bulk UPDATE, since the primary key always clashes
    c.executemany("""
        INSERT INTO
            variables (id, name, unit, shortUnit, description, code, timespan, datasetId, sourceId, coverage, display, createdAt, updatedAt)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, '', '{}', NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            unit = VALUES(unit),
            shortUnit = VALUES(shortUnit),
            description = VALUES(description),
            code = VALUES(code),
            timespan = VALUES(timespan),
            datasetId = VALUES(datasetId),
            sourceId = VALUES(sourceId),
            updatedAt = VALUES(updatedAt)
    """, [
        (x['variableId'], x['name'], x['unit'], x['shortUnit'], x['description'], x['code'], timespan, x['datasetId'], x['sourceId'])
        for x in variables_to_update
    ])

    # Insert the new variables
    c.executemany("""
        INSERT INTO
            variables (name, unit, shortUnit, description, code, timespan, datasetId, sourceId, coverage, display, createdAt, updatedAt)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, '', '{}', NOW(), NOW())
    """, [
        (x['name'], x['unit'], x['shortUnit'], x['description'], x['code'], timespan, x['datasetId'], x['sourceId'])
        for x in variables_to_add
    ])

    # Store the variable IDs in a dict
    c.execute("""
        SELECT
            variables.code,
            variables.id
        FROM variables
        LEFT JOIN datasets ON datasets.id = variables.datasetId
        WHERE datasets.namespace = %s
    """, [DATASET_NAMESPACE])

    variable_id_by_code = {
        code: var_id
        for code, var_id in c.fetchall()
    }


    # ==========================================================================
    # Create all the required entities.
    # ==========================================================================

    c.execute("""
        SELECT
            LOWER(country_name),
            LOWER(entities.name),
            entities.id AS id
        FROM entities
        LEFT JOIN
            country_name_tool_countrydata
            ON country_name_tool_countrydata.owid_name = entities.name
        LEFT JOIN
            country_name_tool_countryname
            ON country_name_tool_countryname.owid_country = country_name_tool_countrydata.id
        WHERE
            LOWER(country_name) IN %(country_names)s
            OR LOWER(entities.name) IN %(country_names)s
        ORDER BY entities.id ASC
    """, {
        'country_names': [normalise_country_name(x) for x in country_name_by_code.values()]
    })

    rows = c.fetchall()

    # Merge the two dicts
    # This will be updated with entities added later.
    entity_id_by_normalised_name = {
        # country_tool_name → entityId
        **dict((row[0], row[2]) for row in rows if row[0]),
        # entityName → entityId
        **dict((row[1], row[2]) for row in rows if row[1])
    }

    entity_names_to_add = set(
        country_name
        for country_name in country_name_by_code.values()
        if normalise_country_name(country_name) not in entity_id_by_normalised_name
    )

    if entity_names_to_add:

        c.executemany("""
            INSERT INTO
                entities (name, displayName, validated, createdAt, updatedAt)
            VALUES
                (%s, '', FALSE, NOW(), NOW())
        """, entity_names_to_add)

        c.execute("""
            SELECT name, id
            FROM entities
            WHERE name in %s
        """, [entity_names_to_add])

        for name, new_id in c.fetchall():
            entity_id_by_normalised_name[normalise_country_name(name)] = new_id

    # The WDI dataset can be inconsistent between sheets, e.g. Country sheet
    # uses 'Sub-Saharan Africa (IDA & IBRD)' while Data sheet uses
    # 'Sub-Saharan Africa (IDA & IBRD countries)'.
    # Matching by code is more reliable.
    entity_id_by_code = {
        code: entity_id_by_normalised_name[normalise_country_name(name)]
        for code, name in country_name_by_code.items()
    }


    # ==========================================================================
    # Confirmation to continue with the import.
    # ==========================================================================

    if variables_to_add: info("\n%d new variables will be added." % (len(variables_to_add)))
    if code_changes: info("\n%d variable codes will be renamed." % (len(code_changes)))
    if variables_to_update: info("\n%d variables will be updated." % (len(variables_to_update)))
    if var_ids_to_remove: info("\n%d variables are no longer published and WILL BE REMOVED." % (len(var_ids_to_remove)))
    if var_ids_to_discontinue:
        info("\n%d variables are no longer published but CANNOT BE REMOVED as they are used in charts." % (len(var_ids_to_discontinue)))
        info("Their IDs are: %s" % (strlist(var_ids_to_discontinue)))
        info("Please make sure to go over them afterwards and check if they may have been renamed.")
    if entity_names_to_add: info("\n%d new entity names will be added: %s" % (len(entity_names_to_add), strlist(entity_names_to_add)))

    print()

    confirmed = yesno("Do you wish to continue?")

    if not confirmed:
        terminate("User did not wish to continue")

    # ==========================================================================
    # Upsert all data_values.
    # This will take a while as more than 1 million rows are inserted.
    # ==========================================================================

    start_year = FIRST_YEAR
    end_year = last_available_year

    start_index = 4
    end_index = 4 + (end_year - start_year)

    index_year_pairs = list(zip(
        range(start_index, end_index + 1),
        range(start_year, end_year + 1)
    ))

    def data_values_from_row(row):
        values = get_row_values(row)
        country_code = values[1].upper().strip()
        indicator_code = values[3].upper().strip()
        entity_id = entity_id_by_code[country_code]
        variable_id = variable_id_by_code[indicator_code]
        return [
            (values[index], year, entity_id, variable_id)
            for index, year in index_year_pairs
            if values[index] is not None # only output a row if it has a value
        ]

    finished = False
    total_inserted = 0

    while not finished:

        finished = True
        data_values_to_insert = []

        for row in data_ws_rows:
            data_values_to_insert += data_values_from_row(row)
            if len(data_values_to_insert) > 50000:
                finished = False
                break

        message = "Inserting {} data_values rows, {} inserted so far.".format(len(data_values_to_insert), total_inserted)
        logger.info(message)
        print(message, end='\r')

        c.executemany("""
            INSERT INTO
                data_values (value, year, entityId, variableId)
            VALUES
                (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value)
        """, data_values_to_insert)

        total_inserted += len(data_values_to_insert)

        if finished:
            message = "Inserted {} data_values rows.".format(total_inserted)
            logger.info(message)
            print("\n" + message)

    c.execute("""
        INSERT INTO importer_importhistory (import_type, import_time, import_notes, import_state)
        VALUES ('wdi', NOW(), %(notes)s, %(state)s)
    """, {
        'notes': '',
        'state': json.dumps({ 'file_checksum': file_checksum(excel_filepath) })
    })

info("\nSuccessfully imported the whole spreadsheet... I mean, the whole thing is now in the database!")
if var_ids_to_discontinue:
    info("\nJust another reminder to look at the %s variables that are no longer published in the spreadsheet." % (len(var_ids_to_discontinue)))
