import sys
import os
import json
import unidecode
import time
import csv
import glob
import zipfile

# allow imports from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from db import connection
from utils import extract_short_unit, yesno
from db_utils import DBUtils

def get_standard_name(entity_name):
    if entity_name == 'Global':
        return 'World'
    elif entity_name == 'High-income North America':
        return 'North America'
    else:
        return entity_name

def get_metric_value(row):
    if row['metric_name'] == 'Percent':
        return str(float(row['val']) * 100)
    else:
        return row['val']


def import_csv_files(measure_names,
                     age_names,
                     metric_names,
                     sex_names,
                     parent_tag_name,
                     namespace,
                     csv_dir,
                     default_source_description,
                     get_key,
                     get_var_name,
                     get_var_code):

    with connection as c:

        db = DBUtils(c)

        total_data_values_upserted = 0

        def get_state():
            return {
                **db.get_counts(),
                'data_values_upserted': total_data_values_upserted,
            }

        def print_state():
            print(" · ".join(str(key) + ": " + str(value) for key, value in get_state().items()))

        # The user ID that gets assigned in every user ID field
        (user_id,) = db.fetch_one("""
            SELECT id FROM users WHERE email = 'daniel@gavrilov.co.uk'
        """)

        # Create the parent tag
        parent_tag_id = db.upsert_parent_tag(parent_tag_name)

        tag_id_by_name = {
            name: i
            for name, i in db.fetch_many("""
                SELECT name, id
                FROM tags
                WHERE parentId = %s
            """, parent_tag_id)
        }

        # Intentionally kept empty in order to force sources to be updated (in order
        # to update `retrievedDate`)
        dataset_id_by_name = {}

        var_id_by_code = {
            code: i
            for code, i in db.fetch_many("""
                SELECT variables.code, variables.id
                FROM variables
                LEFT JOIN datasets ON datasets.id = variables.datasetId
                WHERE datasets.namespace = %s
            """, [namespace])
        }

        # Keep track of variables that we have changed the `updatedAt` column for
        touched_var_codes = set()

        # Intentionally kept empty in order to force sources to be updated (in order
        # to update `retrievedDate`)
        source_id_by_name = {}

        data_values_to_insert = []
        insert_data_value_sql = """
            INSERT INTO data_values
                (value, year, entityId, variableId)
            VALUES
                (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value)
        """

        for filename in glob.glob(os.path.join(csv_dir, '*.csv')):
            with open(filename, 'r', encoding='utf8') as f:
                print('Processing: %s' % filename)
                reader = csv.DictReader(f)
                row_number = 0
                for row in reader:
                    row_number += 1

                    if row_number % 100 == 0:
                        time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time, the delay after each 100th row is 1 millisecond

                    key = get_key(row)

                    # Skip rows we don't want to import
                    if row['sex_name'] not in sex_names \
                        or row['age_name'] not in age_names \
                        or row['metric_name'] not in metric_names \
                        or row['measure_name'] not in measure_names:
                        continue

                    if key not in tag_id_by_name:
                        tag_id_by_name[key] = db.upsert_tag(key, parent_tag_id)

                    if key not in dataset_id_by_name:
                        dataset_id_by_name[key] = db.upsert_dataset(
                            name=key,
                            namespace=namespace,
                            tag_id=tag_id_by_name[key],
                            user_id=user_id
                        )

                    if key not in source_id_by_name:
                        source_id_by_name[key] = db.upsert_source(
                            name=key,
                            description=json.dumps(default_source_description),
                            dataset_id=dataset_id_by_name[key]
                        )

                    var_name = get_var_name(row)

                    var_code = get_var_code(row)

                    if var_code not in var_id_by_code:
                        var_id_by_code[var_code] = db.upsert_variable(
                            name=var_name,
                            code=var_code,
                            unit=row['metric_name'],
                            short_unit=extract_short_unit(row['metric_name']),
                            dataset_id=dataset_id_by_name[key],
                            source_id=source_id_by_name[key]
                        )
                        touched_var_codes.add(var_code)
                    elif var_code not in touched_var_codes:
                        db.touch_variable(var_id_by_code[var_code])
                        touched_var_codes.add(var_code)

                    var_id = var_id_by_code[var_code]
                    entity_name = get_standard_name(row['location_name'])
                    entity_id = db.get_or_create_entity(entity_name)
                    value = get_metric_value(row)
                    year = int(row['year'])

                    data_values_to_insert.append(
                        (value, year, entity_id, var_id)
                    )

                    if len(data_values_to_insert) >= 50000:
                        db.upsert_many(insert_data_value_sql, data_values_to_insert)
                        total_data_values_upserted += len(data_values_to_insert)
                        data_values_to_insert = []
                        print_state()

            if len(data_values_to_insert): # insert any leftover data_values
                db.upsert_many(insert_data_value_sql, data_values_to_insert)
                total_data_values_upserted += len(data_values_to_insert)
                data_values_to_insert = []
                print_state()

        confirm = yesno("\nApply import?")
        if not confirm:
            sys.exit(1)

        db.note_import(
            import_type=namespace,
            import_notes='A gbd import was performed',
            import_state=json.dumps(get_state())
        )
