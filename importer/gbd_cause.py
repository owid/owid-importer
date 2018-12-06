import sys
import os
import time

# allow imports from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from db import connection
from utils import extract_short_unit, yesno
from db_utils import DBUtils
from gbd_tools import import_csv_files

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

################################################################################
# This dataset download is a bit hard to automate, a human needs to follow the
# steps below instead:
#
# 1. Go to the GHDx results tool: http://ghdx.healthdata.org/gbd-results-tool
# 2. Select the following:
#
#    Measure:
#    - Deaths
#    - DALYs (Disability-Adjusted Life Years)
#
#    Age:
#    - All Ages
#    - Age-standardized
#    - Under 5
#    - 5-14 years
#    - 15-49 years
#    - 50-69 years
#    - 70+ years
#
#    Year: select all
#
#    Cause: select all
#
#    Context: Cause
#
#    Location: select all
#
#    Sex:
#    - Both
#
# 3. The tool will then create a dataset for you in chunks. Once it's finished
#    (which may take several hours) this command might be helpful to download
#    them all:
#
#         for i in {1..<number of files>}; do
#             wget http://s3.healthdata.org/gbd-api-2017-public/<hash of a file...>-$i.zip;
#         done
#
# 4. Then, unzip them all and put them in a single folder. This should be the
#    `csv_dir` specified below. Helpful command:
#
#         unzip \*.zip -x citation.txt -d csv/
#

# Below are the field values that will be included in the import.
# Rows that don't match these will be discarded

import_csv_files(
    csv_dir=os.path.join(CURRENT_PATH, '..', 'data', 'gbd_cause', 'csv'),
    measure_names=['Deaths', 'DALYs (Disability-Adjusted Life Years)'],
    age_names=['All Ages', 'Age-standardized', 'Under 5', '5-14 years', '15-49 years', '50-69 years', '70+ years'],
    metric_names=['Number', 'Rate', 'Percent'],
    sex_names=['Both'],
    parent_tag_name='Global Burden of Disease Datasets - Causes',
    namespace='gbd_cause',
    default_source_description = {
        'dataPublishedBy': "Global Burden of Disease Collaborative Network. Global Burden of Disease Study 2016 (GBD 2016) Results. Seattle, United States: Institute for Health Metrics and Evaluation (IHME), 2017.",
        'dataPublisherSource': None,
        'link': "http://ghdx.healthdata.org/gbd-results-tool",
        'retrievedDate': time.strftime('%d-%B-%y'),
        'additionalInfo': None
    }
)
