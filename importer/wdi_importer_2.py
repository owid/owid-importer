import sys
import os
import hashlib
import json
import logging
import requests
import unidecode
import shutil
import time
import zipfile
# allow imports from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import grapher_admin.wsgi
from openpyxl import load_workbook
# from grapher_admin.models import Entity, Dataset, DatasetTag, Source, Variable, Tag, DataValue, ChartDimension # rewrite removed DatasetSubcategory # rewrite removed DatasetCategory # rewrite removed VariableType
# from importer.models import ImportHistory, AdditionalCountryInfo
# from country_name_tool.models import CountryName
# from django.conf import settings
from django.db import connection, transaction
# from django.utils import timezone
# from django.urls import reverse
# from grapher_admin.views import write_dataset_csv

from importer_utils import file_checksum, extract_short_unit, get_row_values, starts_with, default

USER_ID = 29 # The user ID that gets assigned in every user ID field
DATASET_NAMESPACE = 'wdi'
PARENT_TAG_NAME = 'World Development Indicators'  # set the name of the root category of all data that will be imported by this script
ZIP_FILE_URL = 'http://databank.worldbank.org/data/download/WDI_excel.zip'
DATASET_NAME_PREFIX = 'World Development Indicators - '
FIRST_YEAR = 1960

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
DOWNLOADS_PATH = os.path.join(CURRENT_PATH, '..', 'data', 'wdi_downloads')

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

logger = logging.getLogger('importer')
start_time = time.time()

def terminate(message):
    logger.error(message)
    sys.exit(1)

def dataset_name_from_category(category):
    return 'World Development Indicators - ' + category

def normalise_country_name(country_name):
    return unidecode.unidecode(country_name.lower())

# Extract indicator from row in Series worksheet
def indicator_from_row(row):
    values = get_row_values(row)

    code = values[0].upper().strip()
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

# logger.info("Getting the zip file")
# request_header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
# r = requests.get(ZIP_FILE_URL, stream=True, headers=request_header)
# if r.ok:
#     with open(DOWNLOADS_PATH + 'wdi.zip', 'wb') as out_file:
#         shutil.copyfileobj(r.raw, out_file)
#     logger.info("Saved the zip file to disk.")
#     z = zipfile.ZipFile(DOWNLOADS_PATH + 'wdi.zip')
#     excel_filepath = DOWNLOADS_PATH + z.namelist()[0]  # there should be only one file inside the zipfile, so we will load that one
#     z.extractall(DOWNLOADS_PATH)
#     r = None  # we do not need the request anymore
#     logger.info("Successfully extracted the zip file")
# else:
#     logger.error("The file could not be downloaded. Stopping the script...")
#     sys.exit("Could not download file.")

excel_filepath = os.path.join(DOWNLOADS_PATH, 'WDIEXCEL.xlsx')

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

# Data sheet uses INX for 'Not classified', but Country sheet does not have it.
country_name_by_code['INX'] = 'Not classified'


with transaction.atomic():

    with connection.cursor() as c:

        # ======================================================================
        # Clear the database from old variables, data_values & sources.
        # ======================================================================

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

        variable_rows_to_maybe_remove = list(c.fetchall())

        # Variables that are no longer present in the spreadsheet will be
        # removed if no chart uses them OR flagged up for the person running the
        # script to handle in some way (the variables may have been renamed).
        var_ids_to_remove      = [x[0] for x in variable_rows_to_maybe_remove if x[1] == 0]
        var_ids_to_discontinue = [x[0] for x in variable_rows_to_maybe_remove if x[1] == 1]

        c.execute("""
            SELECT DISTINCT sources.id
            FROM sources
            LEFT JOIN variables ON variables.sourceId = sources.id
            WHERE variables.id IN %s
        """, [var_ids_to_remove])

        source_ids_to_remove = [x[0] for x in c.fetchall()]

        c.execute("""
            DELETE FROM data_values
            WHERE variableId IN %s
        """, [var_ids_to_remove])

        c.execute("""
            DELETE FROM variables
            WHERE id IN %s
        """, [var_ids_to_remove])

        # Only delete the sources if there are no variables referencing them.
        c.execute("""
            DELETE sources
            FROM sources
            LEFT JOIN variables ON variables.sourceId = sources.id
            WHERE
                variables.id IS NULL
                AND sources.id IN %s
        """, [source_ids_to_remove])

        # ======================================================================
        # Upsert the tags & datasets.
        # ======================================================================

        # If the parent tag doesn't exist, create it & store the ID.

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

        dataset_names_to_upsert = {
            dataset_name_from_category(category)
            for category in categories
        }

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
            (name, 'This is a dataset imported by the automated fetcher', DATASET_NAMESPACE, USER_ID, USER_ID, USER_ID)
            for name in dataset_names_to_upsert
        ])

        # Associate each dataset with the appropriate tag.
        for category in categories:
            c.execute("""
                INSERT INTO
                    dataset_tags (datasetId, tagId)
                SELECT
                    (
                        SELECT id FROM datasets
                        WHERE name = %(dataset_name)s
                        AND namespace = %(namespace)s
                        LIMIT 1
                    ) AS datasetId,
                    (
                        SELECT id FROM tags
                        WHERE name = %(category)s
                        AND parentId = %(parent_tag_id)s
                        LIMIT 1
                    ) AS tagId
                ON DUPLICATE KEY UPDATE
                    tagId = VALUES(tagId)
            """, { # ON DUPLICATE here only avoids error, it intentionally updates nothing
                'dataset_name': dataset_name_from_category(category),
                'namespace': DATASET_NAMESPACE,
                'category': category,
                'parent_tag_id': parent_tag_id
            })

        c.execute("""
            SELECT name, id
            FROM datasets
            WHERE namespace = %s
        """, [DATASET_NAMESPACE])

        dataset_id_by_name = {
            row[0]: row[1]
            for row in c.fetchall()
        }

        for indicator in indicators:
            indicator['datasetId'] = dataset_id_by_name[indicator['datasetName']]

        # ======================================================================
        # Upsert the variables & sources.
        # ======================================================================

        # Retrieve all variables and their sourceIds

        c.execute("""
            SELECT
                variables.id,
                variables.code,
                variables.sourceId
            FROM variables
            LEFT JOIN datasets ON datasets.id = variables.datasetId
            WHERE datasets.namespace = %s
        """, [DATASET_NAMESPACE])

        for (var_id, code, sourceId) in c.fetchall():
            if code in indicator_by_code:
                indicator_by_code[code]['sourceId'] = sourceId

        # Update existing sources & insert new ones

        variables_to_update = [
            indicator
            for indicator in indicators
            if indicator['sourceId']
        ]

        variables_to_add = [
            indicator
            for indicator in indicators
            if not indicator['sourceId']
        ]

        # Update the existing sources.
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
            (v['sourceId'], v['source']['name'], v['source']['description'], v['datasetId'])
            for v in variables_to_update
        ])

        # Create new sources for the new variables.
        c.executemany("""
            INSERT INTO
                sources (name, description, datasetId, createdAt, updatedAt)
            VALUES
                (%s, %s, %s, NOW(), NOW())
        """, [
            (v['source']['name'], v['source']['description'], v['datasetId'])
            for v in variables_to_add
        ])

        # Populate the indicators with sourceId for the new sources

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

        # Upsert all the variables
        # Relies on (code, datasetId) constraint
        c.executemany("""
            INSERT INTO
                variables (name, unit, shortUnit, description, code, timespan, datasetId, sourceId, coverage, display, createdAt, updatedAt)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, '', '{}', NOW(), NOW())
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
            (x['name'], x['unit'], x['shortUnit'], x['description'], x['code'], timespan, x['datasetId'], x['sourceId'])
            for x in indicators
        ])

        # Store the variable IDs
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


        # ======================================================================
        # Create all the required entities.
        # ======================================================================

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

        c.executemany("""
            INSERT INTO
                entities (name, displayName, validated, createdAt, updatedAt)
            VALUES
                (%s, %s, FALSE, NOW(), NOW())
        """, [
            (name, name)
            for name in entity_names_to_add
        ])

        c.execute("""
            SELECT name, id
            FROM entities
            WHERE name in %s
        """, [entity_names_to_add])

        for (name, new_id) in c.fetchall():
            entity_id_by_normalised_name[normalise_country_name(name)] = new_id

        # The WDI dataset can be inconsistent between sheets, e.g. Country sheet
        # uses 'Sub-Saharan Africa (IDA & IBRD)' while Data sheet uses
        # 'Sub-Saharan Africa (IDA & IBRD countries)'.
        entity_id_by_code = {
            code: entity_id_by_normalised_name[normalise_country_name(name)]
            for code, name in country_name_by_code.items()
        }


        # ======================================================================
        # Upsert all data_values.
        # ======================================================================

        start_year = FIRST_YEAR
        end_year = last_available_year

        start_index = 4
        end_index = 4 + (end_year - start_year)

        index_year_pairs = zip(
            range(start_index, end_index + 1), # index
            range(start_year, end_year + 1) # year
        )

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

        not_finished = True

        while not_finished:

            not_finished = False
            data_values_to_insert = []

            for row in data_ws_rows:
                data_values_to_insert += data_values_from_row(row)
                if len(data_values_to_insert) > 10000:
                    not_finished = True
                    break

            print("Inserting {} data_values rows...".format(len(data_values_to_insert)))

            c.executemany("""
                INSERT INTO
                    data_values (value, year, entityId, variableId)
                VALUES
                    (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    value = VALUES(value)
            """, data_values_to_insert)


        sys.exit(1)


        # TODO Flag variables that are used but removed from the new dataset
        # TODO Flag entities that are gonna be added
        # TODO check all DUPLICATE KEY updates correspond to correct names

        sys.exit(1)






