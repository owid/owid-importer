import sys
import os
import csv
import hashlib
from datetime import datetime
import json
import glob
import unidecode
import time
import zipfile

sys.path.insert(1, os.path.join(sys.path[0], '..'))
# from grapher_admin.models import Entity|DatasetSubcategory|DatasetCategory|Dataset|Source|Variable|VariableType|DataValue
# from importer.models import ImportHistory
# from country_name_tool.models import CountryName
# from django.conf import settings
# from django.db import connection, transaction
# from django.utils import timezone
# from grapher_admin.views import write_dataset_csv
from db import connection
from utils import extract_short_unit, file_checksum, starts_with
from db_utils import DBUtils

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

start_time = datetime.now()
# IMPORTANT: FAOSTAT's large bulk dataset download is a collection of 70+ zip files
# Each zip file contains only one csv file
# The link to the bulk download: http://fenixservices.fao.org/faostat/static/bulkdownloads/FAOSTAT.zip
# These csv files contain different structure variants
# Some of the zip files may have been compressed using a compression format not available in python
# Unzip those files, and put their csv files into the same directory where all zip files are located
# Here is what you need to to before running this script:
# Put all .zip and .csv files you want to parse in one directory
# make sure you have metadata csv files for each of your .zip or .csv file
# metadata files must be in a separate directory, and must have the same name as the .zip or .csv dataset file
# Metadata is not included in the bulk download, and can be downloaded from http://www.fao.org/faostat/en/?#data/
# Put each dataset file's name into an appropriate category in the category_files dict
# Fill in the files_to_exclude list with files you don't want to parse
# Please note that the datasets which don't have their corresponding .csv metadata files will have some of the "Sources" fields empty
# Check the column_types list and the logic for dealing with different column types in the process_csv_file function
# Fill in the file_dataset_names dict with names of datasets for each file
# The script will perform the necessary checks and will inform the user if anything is missing

db = None
processed_values = 0  # the total number of values processed
var_ids_to_delete = []

PARENT_TAG_NAME = 'FAOSTAT 2018'  # set the name of the root category of all data that will be imported by this script
DATASET_NAMESPACE = 'faostat_2018'

DEFAULT_SOURCE_DESCRIPTION = {
    'link': "http://www.fao.org/faostat/en/?#data/",
    'retrievedDate': time.strftime("%d-%B-%y")
}

category_files = {
    "Production": [
        "Production_Crops_E_All_Data_(Normalized).zip",
        "Production_CropsProcessed_E_All_Data_(Normalized).zip",
        "Production_Livestock_E_All_Data_(Normalized).zip",
        "Production_LivestockPrimary_E_All_Data_(Normalized).zip",
        "Production_LivestockProcessed_E_All_Data_(Normalized).zip",
        "Production_Indices_E_All_Data_(Normalized).zip",
        "Value_of_Production_E_All_Data_(Normalized).zip"
    ],
    "Trade": [
        "Trade_Crops_Livestock_E_All_Data_(Normalized).zip",
        "Trade_LiveAnimals_E_All_Data_(Normalized).zip",
        "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",  # this file cannot be extracted using python's zipfile module
        "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv",
        "Trade_Indices_E_All_Data_(Normalized).zip"
    ],
    "Food Balance": [
        "FoodBalanceSheets_E_All_Data_(Normalized).zip",  # this file cannot be extracted using python's zipfile module
        "FoodBalanceSheets_E_All_Data_(Normalized).csv",
        "CommodityBalances_Crops_E_All_Data_(Normalized).zip",
        "CommodityBalances_LivestockFish_E_All_Data_(Normalized).zip",
        "FoodSupply_Crops_E_All_Data_(Normalized).zip",
        "FoodSupply_LivestockFish_E_All_Data_(Normalized).zip"
    ],
    "Food Security": [
        "Indicators_from_Household_Surveys_E_All_Data_(Normalized).zip",
        "Food_Security_Data_E_All_Data_(Normalized).zip"
    ],
    "Prices": [
        "Prices_E_All_Data_(Normalized).zip",
        "Prices_Monthly_E_All_Data_(Normalized).zip",
        "Price_Indices_E_All_Data_(Normalized).zip",
        "PricesArchive_E_All_Data_(Normalized).zip",
        "ConsumerPriceIndices_E_All_Data_(Normalized).zip",
        "Deflators_E_All_Data_(Normalized).zip",
        "Exchange_rate_E_All_Data_(Normalized).zip"
    ],
    "Inputs": [
        "Inputs_FertilizersProduct_E_All_Data_(Normalized).zip",
        "Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip",
        "Inputs_FertilizersArchive_E_All_Data_(Normalized).zip",
        "Inputs_FertilizersTradeValues_E_All_Data_(Normalized).zip",
        "Inputs_Pesticides_Use_E_All_Data_(Normalized).zip",
        "Inputs_Pesticides_Trade_E_All_Data_(Normalized).zip",
        "Inputs_LandUse_E_All_Data_(Normalized).zip",
        "Employment_Indicators_E_All_Data_(Normalized).zip"
    ],
    "Population": [
        "Population_E_All_Data_(Normalized).zip"
    ],
    "Investment": [
        "Investment_Machinery_E_All_Data_(Normalized).zip",
        "Investment_MachineryArchive_E_All_Data_(Normalized).zip",
        "Investment_GovernmentExpenditure_E_All_Data_(Normalized).zip",
        "Investment_CreditAgriculture_E_All_Data_(Normalized).zip",
        "Development_Assistance_to_Agriculture_E_All_Data_(Normalized).zip",
        "Investment_ForeignDirectInvestment_E_All_Data_(Normalized).zip",
        "Investment_CountryInvestmentStatisticsProfile__E_All_Data_(Normalized).zip"
    ],
    "Macro-Statistics": [
        "Investment_CapitalStock_E_All_Data_(Normalized).zip",
        "Macro-Statistics_Key_Indicators_E_All_Data_(Normalized).zip"
    ],
    "Agri-Environmental Indicators": [
        "Environment_AirClimateChange_E_All_Data_(Normalized).zip",
        "Environment_Energy_E_All_Data_(Normalized).zip",
        "Environment_Fertilizers_E_All_Data_(Normalized).zip",
        "Environment_LandUse_E_All_Data_(Normalized).zip",
        "Environment_LandCover_E_All_Data_(Normalized).zip",
        "Environment_LivestockPatterns_E_All_Data_(Normalized).zip",
        "Environment_Pesticides_E_All_Data_(Normalized).zip",
        "Environment_Soil_E_All_Data_(Normalized).zip",
        "Environment_Water_E_All_Data_(Normalized).zip",
        "Environment_Emissions_by_Sector_E_All_Data_(Normalized).zip",
        "Environment_Emissions_intensities_E_All_Data_(Normalized).zip",
        "Environment_Livestock_E_All_Data_(Normalized).zip",
        "Environment_LivestockManure_E_All_Data_(Normalized).zip"
    ],
    "Emissions - Agriculture": [
        "Emissions_Agriculture_Agriculture_total_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Enteric_Fermentation_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Manure_Management_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Rice_Cultivation_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Synthetic_Fertilizers_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Manure_applied_to_soils_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Manure_left_on_pasture_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Crop_Residues_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Cultivated_Organic_Soils_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Burning_Savanna_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Burning_crop_residues_E_All_Data_(Normalized).zip",
        "Emissions_Agriculture_Energy_E_All_Data_(Normalized).zip"
    ],
    "Emissions - Land Use": [
        "Emissions_Land_Use_Land_Use_Total_E_All_Data_(Normalized).zip",
        "Emissions_Land_Use_Forest_Land_E_All_Data_(Normalized).zip",
        "Emissions_Land_Use_Cropland_E_All_Data_(Normalized).zip",
        "Emissions_Land_Use_Grassland_E_All_Data_(Normalized).zip",
        "Emissions_Land_Use_Burning_Biomass_E_All_Data_(Normalized).zip"
    ],
    "Forestry": [
        "Forestry_E_All_Data_(Normalized).zip",
        "Forestry_Trade_Flows_E_All_Data_(Normalized).zip"
    ],
    "ASTI R&D Indicators": [
        "ASTI_Research_Spending_E_All_Data_(Normalized).zip",
        "ASTI_Researchers_E_All_Data_(Normalized).zip"
    ],
    "Emergency Response": [
        "Food_Aid_Shipments_WFP_E_All_Data_(Normalized).zip"
    ]
}


file_dataset_names = {
    "ASTI_Research_Spending_E_All_Data_(Normalized).zip":
        "ASTI-Expenditures",
    "ASTI_Researchers_E_All_Data_(Normalized).zip":
        "ASTI-Researchers",
    "CommodityBalances_Crops_E_All_Data_(Normalized).zip":
        "Commodity Balances - Crops Primary Equivalent",
    "CommodityBalances_LivestockFish_E_All_Data_(Normalized).zip":
        "Commodity Balances - Livestock and Fish Primary Equivalent",
    "ConsumerPriceIndices_E_All_Data_(Normalized).zip":
        "Consumer Price Indices",
    "Deflators_E_All_Data_(Normalized).zip":
        "Deflators",
    "Development_Assistance_to_Agriculture_E_All_Data_(Normalized).zip":
        "Development Flows to Agriculture",
    "Emissions_Agriculture_Agriculture_total_E_All_Data_(Normalized).zip":
        "Agriculture Total",
    "Emissions_Agriculture_Burning_crop_residues_E_All_Data_(Normalized).zip":
        "Burning - Crop Residues",
    "Emissions_Agriculture_Burning_Savanna_E_All_Data_(Normalized).zip":
        "Burning - Savanna",
    "Emissions_Agriculture_Crop_Residues_E_All_Data_(Normalized).zip":
        "Crop Residues",
    "Emissions_Agriculture_Cultivated_Organic_Soils_E_All_Data_(Normalized).zip":
        "Cultivation of Organic Soils",
    "Emissions_Agriculture_Energy_E_All_Data_(Normalized).zip":
        "Energy Use",
    "Emissions_Agriculture_Enteric_Fermentation_E_All_Data_(Normalized).zip":
        "Enteric Fermentation",
    "Emissions_Agriculture_Manure_applied_to_soils_E_All_Data_(Normalized).zip":
        "Manure applied to Soils",
    "Emissions_Agriculture_Manure_left_on_pasture_E_All_Data_(Normalized).zip":
        "Manure left on Pasture",
    "Emissions_Agriculture_Manure_Management_E_All_Data_(Normalized).zip":
        "Manure Management",
    "Emissions_Agriculture_Rice_Cultivation_E_All_Data_(Normalized).zip":
        "Rice Cultivation",
    "Emissions_Agriculture_Synthetic_Fertilizers_E_All_Data_(Normalized).zip":
        "Synthetic Fertilizers",
    "Emissions_Land_Use_Burning_Biomass_E_All_Data_(Normalized).zip":
        "Burning - Biomass",
    "Emissions_Land_Use_Cropland_E_All_Data_(Normalized).zip":
        "Cropland",
    "Emissions_Land_Use_Forest_Land_E_All_Data_(Normalized).zip":
        "Forest Land",
    "Emissions_Land_Use_Grassland_E_All_Data_(Normalized).zip":
        "Grassland",
    "Emissions_Land_Use_Land_Use_Total_E_All_Data_(Normalized).zip":
        "Land Use Total",
    "Employment_Indicators_E_All_Data_(Normalized).zip":
        "Employment Indicators",
    "Environment_AirClimateChange_E_All_Data_(Normalized).zip":
        "Air and climate change",
    "Environment_Emissions_by_Sector_E_All_Data_(Normalized).zip":
        "Emissions by sector",
    "Environment_Emissions_intensities_E_All_Data_(Normalized).zip":
        "Emissions intensities",
    "Environment_Energy_E_All_Data_(Normalized).zip":
        "Energy",
    "Environment_Fertilizers_E_All_Data_(Normalized).zip":
        "Fertilizers",
    "Environment_LandCover_E_All_Data_(Normalized).zip":
        "Land Cover",
    "Environment_LandUse_E_All_Data_(Normalized).zip":
        "Land Use",
    "Environment_Livestock_E_All_Data_(Normalized).zip":
        "Livestock",
    "Environment_LivestockManure_E_All_Data_(Normalized).zip":
        "Livestock Manure",
    "Environment_LivestockPatterns_E_All_Data_(Normalized).zip":
        "Livestock Patterns",
    "Environment_Pesticides_E_All_Data_(Normalized).zip":
        "Pesticides",
    "Environment_Soil_E_All_Data_(Normalized).zip":
        "Soil",
    "Environment_Water_E_All_Data_(Normalized).zip":
        "Water",
    "Exchange_rate_E_All_Data_(Normalized).zip":
        "Exchange rates - Annual",
    "Food_Aid_Shipments_WFP_E_All_Data_(Normalized).zip":
        "Food Aid Shipments (WFP)",
    "Food_Security_Data_E_All_Data_(Normalized).zip":
        "Suite of Food Security Indicators",
    "FoodBalanceSheets_E_All_Data_(Normalized).zip":
        "Food Balance Sheets",
    "FoodSupply_Crops_E_All_Data_(Normalized).zip":
        "Food Supply - Crops Primary Equivalent",
    "FoodSupply_LivestockFish_E_All_Data_(Normalized).zip":
        "Food Supply - Livestock and Fish Primary Equivalent",
    "Forestry_E_All_Data_(Normalized).zip":
        "Forestry Production and Trade",
    "Forestry_Trade_Flows_E_All_Data_(Normalized).zip":
        "Forestry Trade Flows",
    "Indicators_from_Household_Surveys_E_All_Data_(Normalized).zip":
        "Indicators from Household Surveys (gender, area, socioeconomics)",
    "Inputs_FertilizersProduct_E_All_Data_(Normalized).zip":
        "Fertilizers",
    "Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip":
        "Fertilizers - Nutrient",
    "Inputs_FertilizersArchive_E_All_Data_(Normalized).zip":
        "Fertilizers archive",
    "Inputs_FertilizersTradeValues_E_All_Data_(Normalized).zip":
        "Fertilizers - Trade Value",
    "Inputs_LandUse_E_All_Data_(Normalized).zip":
        "Land Use",
    "Inputs_Pesticides_Trade_E_All_Data_(Normalized).zip":
        "Pesticides Trade",
    "Inputs_Pesticides_Use_E_All_Data_(Normalized).zip":
        "Pesticides Use",
    "Investment_CapitalStock_E_All_Data_(Normalized).zip":
        "Capital Stock",
    "Investment_CountryInvestmentStatisticsProfile__E_All_Data_(Normalized).zip":
        "Country Investment Statistics Profile",
    "Investment_CreditAgriculture_E_All_Data_(Normalized).zip":
        "Credit to Agriculture",
    "Investment_ForeignDirectInvestment_E_All_Data_(Normalized).zip":
        "Foreign Direct Investment (FDI)",
    "Investment_GovernmentExpenditure_E_All_Data_(Normalized).zip":
        "Government Expenditure",
    "Investment_Machinery_E_All_Data_(Normalized).zip":
        "Machinery",
    "Investment_MachineryArchive_E_All_Data_(Normalized).zip":
        "Machinery Archive",
    "Macro-Statistics_Key_Indicators_E_All_Data_(Normalized).zip":
        "Macro Indicators",
    "Population_E_All_Data_(Normalized).zip":
        "Annual population",
    "Price_Indices_E_All_Data_(Normalized).zip":
        "Producer Price Indices - Annual",
    "Prices_E_All_Data_(Normalized).zip":
        "Producer Prices - Annual",
    "Prices_Monthly_E_All_Data_(Normalized).zip":
        "Producer Prices - Monthly",
    "PricesArchive_E_All_Data_(Normalized).zip":
        "Producer Prices - Archive",
    "Production_Crops_E_All_Data_(Normalized).zip":
        "Crops",
    "Production_CropsProcessed_E_All_Data_(Normalized).zip":
        "Crops processed",
    "Production_Indices_E_All_Data_(Normalized).zip":
        "Production Indices",
    "Production_Livestock_E_All_Data_(Normalized).zip":
        "Live Animals",
    "Production_LivestockPrimary_E_All_Data_(Normalized).zip":
        "Livestock Primary",
    "Production_LivestockProcessed_E_All_Data_(Normalized).zip":
        "Livestock Processed",
    "Trade_Crops_Livestock_E_All_Data_(Normalized).zip":
        "Crops and livestock products",
    "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip":
        "Detailed trade matrix",
    "Trade_Indices_E_All_Data_(Normalized).zip":
        "Trade Indices",
    "Trade_LiveAnimals_E_All_Data_(Normalized).zip":
        "Live animals",
    "Value_of_Production_E_All_Data_(Normalized).zip":
        "Value of Agricultural Production",
    "FoodBalanceSheets_E_All_Data_(Normalized).csv":
        "Food Balance Sheets",
    "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv":
        "Detailed trade matrix"
}

# the different column name variants found in the FAO dataset files
column_types = [
    # 11 columns
    tuple(["Area Code", "Area", "Item Code", "Item", "ISO Currency Code", "Currency", "Year Code", "Year", "Unit", "Value", "Flag"]),
    tuple(["CountryCode", "Country", "ItemCode", "Item", "ElementGroup", "ElementCode", "Element", "Year", "Unit", "Value", "Flag"]),
    tuple(["Area Code", "Area", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
    tuple(["Country Code", "Country", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
    tuple(["Country Code", "Country", "Source Code", "Source", "Indicator Code", "Indicator", "Year Code", "Year", "Unit", "Value", "Flag"]),
    tuple(["Recipient Country Code", "Recipient Country", "Item Code", "Item", "Donor Country Code", "Donor Country", "Year Code", "Year", "Unit", "Value", "Flag"]),
    # 13 columns
    tuple(["Reporter Country Code", "Reporter Countries", "Partner Country Code", "Partner Countries", "Item Code", "Item", "Element Code", "Element", "Year Code", "Year", "Unit", "Value", "Flag"]),
    # 15 columns
    tuple(["Donor Code", "Donor", "Recipient Country Code", "Recipient Country", "Item Code", "Item", "Element Code", "Element", "Purpose Code", "Purpose", "Year Code", "Year", "Unit", "Value", "Flag"])
]

files_to_exclude = [
    "CommodityBalances_Crops_E_All_Data_(Normalized).zip",
    "CommodityBalances_LivestockFish_E_All_Data_(Normalized).zip",
    "FoodSupply_Crops_E_All_Data_(Normalized).zip",
    "FoodSupply_LivestockFish_E_All_Data_(Normalized).zip",
    "Indicators_from_Household_Surveys_E_All_Data_(Normalized).zip",
    "Population_E_All_Data_(Normalized).zip",
    "Prices_Monthly_E_All_Data_(Normalized).zip",
    "PricesArchive_E_All_Data_(Normalized).zip",
    "ConsumerPriceIndices_E_All_Data_(Normalized).zip",
    "Investment_CapitalStock_E_All_Data.zip",
    "Environment_Temperature_change_E_All_Data_(Normalized).zip",
    "Environment_LivestockManure_E_All_Data_(Normalized).zip"
]

all_dataset_files_dir = os.path.join(CURRENT_PATH, '..', 'data', 'faostat', 'FAOSTAT')
metadata_dir = os.path.join(CURRENT_PATH, '..', 'data', 'faostat', 'metadata')

all_files_cat = []  # will contain all the files that are assigned to a category in the category_files variable
file_to_category_dict = {}  # will hold the corresponding category of a file
for category, files in category_files.items():
    for each in files:
        all_files_cat.append(each)
        file_to_category_dict[each] = category

all_files_meta = []  # will contain all the .csv metadata files in the folder given by metadata_dir variable
for file in glob.glob(os.path.join(metadata_dir, "*.csv")):
    all_files_meta.append(os.path.splitext(os.path.basename(file))[0] + ".zip")
    # dataset files that can't be extracted by python will be put in csv format in the folder with other .zip files
    all_files_meta.append(os.path.splitext(os.path.basename(file))[0] + ".csv")

print("########################################################################################")
print("Please make sure you set the ulimit on your operating system to a value higher than 5000")
print("########################################################################################")

# Will now perform the checks for files, dataset categories and file structures
parsing_notes = []

for file in glob.glob(os.path.join(all_dataset_files_dir, "*.zip")):
    one_file = os.path.basename(file)
    if one_file not in all_files_cat and one_file not in files_to_exclude:
        parsing_notes.append("The file %s is not found in the category_files dict." % one_file)
    if one_file not in all_files_meta and one_file not in files_to_exclude:
        parsing_notes.append("The metadata file for %s was not found." % one_file)
    if one_file in files_to_exclude:
        parsing_notes.append("File %s will be excluded from parsing." % one_file)
    if one_file not in files_to_exclude:
        file_extracted = 1
        zip_ref = zipfile.ZipFile(file, 'r')
        csv_filename = zip_ref.namelist()[0]
        try:
            zip_ref.extractall("/tmp")
        except:
            file_extracted = 0
        zip_ref.close()

        if file_extracted:
            with open(os.path.join("/tmp", csv_filename), encoding='latin-1') as csvfile:
                reader = csv.DictReader(csvfile)
                columns = tuple(reader.fieldnames)
                if not any([starts_with(columns, c) for c in column_types]):
                    parsing_notes.append("The file %s contains columns that are not defined in the column_types list." % one_file)
            os.remove("/tmp/%s" % csv_filename)
    if one_file not in files_to_exclude:
        if one_file not in file_dataset_names:
            parsing_notes.append(
                "The file %s does not have a dataset name defined in file_dataset_names dict." % one_file)


for file in glob.glob(os.path.join(all_dataset_files_dir, "*.csv")):
    one_file = os.path.basename(file)
    if one_file not in all_files_cat and one_file not in files_to_exclude:
        parsing_notes.append("The file %s is not found in the category_files dict." % one_file)
    if one_file not in all_files_meta and one_file not in files_to_exclude:
        parsing_notes.append("The metadata file for %s was not found." % one_file)
    if one_file in files_to_exclude:
        parsing_notes.append("File %s will be excluded from parsing." % one_file)
    if one_file not in files_to_exclude:
        with open(file, encoding='latin-1') as csvfile:
            reader = csv.DictReader(csvfile)
            columns = tuple(reader.fieldnames)
            if not any([starts_with(columns, c) for c in column_types]):
                parsing_notes.append(
                    "The file %s contains columns that are not defined in the column_types list." % one_file)
    if one_file not in files_to_exclude:
        if one_file not in file_dataset_names:
            parsing_notes.append(
                "The file %s does not have a dataset name defined in file_dataset_names dict." % one_file)

if len(parsing_notes) > 0:
    for each in parsing_notes:
        print(each)
    user_answer = input("Do you want to proceed? (Enter Y or N)")
    while user_answer.lower() != "y" and user_answer.lower() != "n":
        user_answer = input("Do you want to proceed? (Enter Y or N)")
    if user_answer.lower() == "n":
        sys.exit()


def process_csv_file_update(filename_to_process: str, original_filename: str):
    print('Processing: %s' % original_filename)

    global db

    # we will now construct the list of all unique variable names found in one file

    unique_var_names = []
    global var_ids_to_delete

    with open(filename_to_process, encoding='latin-1') as currentfile:
        currentreader = csv.DictReader(currentfile)
        filecolumns = tuple(currentreader.fieldnames)

        if any([starts_with(filecolumns, c) for c in column_types[0:5]]):
            for row in currentreader:
                if starts_with(filecolumns, column_types[0]):
                    variablename = row['Item']
                if starts_with(filecolumns, column_types[1]):
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                if starts_with(filecolumns, column_types[2]):
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                if starts_with(filecolumns, column_types[3]):
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                if starts_with(filecolumns, column_types[4]):
                    variablename = '%s - %s' % (row['Indicator'], row['Source'])

                if original_filename == 'Emissions_Agriculture_Energy_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                if original_filename == 'Production_LivestockPrimary_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                if original_filename == 'Trade_LiveAnimals_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                variablename = file_dataset_names[original_filename] + ': ' + variablename
                if variablename not in unique_var_names:
                    unique_var_names.append(variablename)

        if any([starts_with(filecolumns, c) for c in column_types[5:8]]):
            if starts_with(filecolumns, column_types[5]):
                iterations = [
                    {
                        'varname_format': '%s - Donors'
                    },
                    {
                        'varname_format': '%s - Recipients'
                    }]
            if starts_with(filecolumns, column_types[6]):
                iterations = [
                    {
                        'varname_format': '%s - %s - Reporters'
                    },
                    {
                        'varname_format': '%s - %s - Partners'
                    }]
            if starts_with(filecolumns, column_types[7]):
                iterations = [
                    {
                        'varname_format': '%s - %s - Donors'
                    },
                    {
                        'varname_format': '%s - %s - Recipients'
                    }]
            for oneiteration in iterations:
                currentfile.seek(0)
                row_counter = 0
                for row in currentreader:
                    if row['Year'] == 'Year':
                        continue
                    row_counter += 1
                    if row_counter % 300 == 0:
                        time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time
                    if starts_with(filecolumns, column_types[5]):
                        variablename = oneiteration['varname_format'] % row['Item']
                    if starts_with(filecolumns, column_types[6]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Element'])
                    if starts_with(filecolumns, column_types[7]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Purpose'])

                    variablename = file_dataset_names[original_filename] + ': ' + variablename
                    if variablename not in unique_var_names:
                        unique_var_names.append(variablename)

        var_ids_to_delete += [
            row[0]
            for row in db.fetch_many("""
                SELECT id
                FROM variables
                JOIN datasets ON datasets.id = variables.datasetId
                WHERE datasets.namespace = %(namespace)s
                AND variables.name IN %(var_names)s
            """, {
                'namespace': DATASET_NAMESPACE,
                'var_names': unique_var_names
            })
        ]

        db.execute_until_empty("""
            DELETE FROM data_values
            WHERE variableId IN %s
            LIMIT 100000
        """, [var_ids_to_delete])

    process_csv_file_insert(filename_to_process, original_filename)


def process_csv_file_insert(filename_to_process: str, original_filename: str):
    print('Processing: %s' % original_filename)

    global unique_data_tracker
    global db

    current_file_vars_countries = set()  # keeps track of variables+countries we saw in the current file
    current_file_var_codes = set()
    current_file_var_names = set()
    previous_row = tuple()

    category_name = file_to_category_dict[original_filename]

    # inserting a subcategory
    if category_name not in tag_id_by_name:
        tag_id_by_name[category_name] = db.upsert_tag(
            name=category_name,
            parent_id=parent_tag_id
        )

    # inserting a dataset
    dataset_name = '%s: %s' % (file_to_category_dict[original_filename], file_dataset_names[original_filename])

    if dataset_name not in dataset_id_by_name:
        dataset_id_by_name[dataset_name] = db.upsert_dataset(
            name=dataset_name,
            namespace=DATASET_NAMESPACE,
            tag_id=tag_id_by_name[category_name],
            user_id=user_id
        )

    insert_string = 'INSERT into data_values (value, year, entityId, variableId) VALUES (%s, %s, %s, %s)'  # this is used for constructing the query for mass inserting to the data_values table
    data_values_tuple_list = []

    # reading source information from a csv file in metadata_dir
    metadata_file_path = os.path.join(metadata_dir, os.path.splitext(original_filename)[0] + ".csv")
    data_published_by = 'Food and Agriculture Organization of the United Nations (FAO)'
    data_publishers_source = ''
    additional_information = ''
    variable_description = ''
    if os.path.isfile(metadata_file_path):
        with open(metadata_file_path, encoding='latin-1') as metadatacsv:
            metadatareader = csv.DictReader(metadatacsv)
            metadatacolumns = tuple(metadatareader.fieldnames)
            for row in metadatareader:
                if row['Subsection Code'] == '1.1':
                    data_published_by = row['Metadata']
                if row['Subsection Code'] == '3.1':
                    variable_description = row['Metadata']
                if row['Subsection Code'] == '3.4':
                    additional_information = row['Metadata']
                if row['Subsection Code'] == '20.1':
                    data_publishers_source = row['Metadata']

    # inserting a dataset source
    if category_name not in source_id_by_name:
        source_description = {
            **DEFAULT_SOURCE_DESCRIPTION,
            'dataPublishedBy': data_published_by,
            'dataPublisherSource': data_publishers_source,
            'additionalInfo': additional_information
        }
        source_id_by_name[category_name] = db.upsert_source(
            name=category_name,
            description=json.dumps(source_description),
            dataset_id=dataset_id_by_name[dataset_name]
        )

    var_id_by_name = {
        name: i
        for name, i in db.fetch_many("""
            SELECT variables.name, variables.id
            FROM variables
            JOIN datasets ON datasets.id = variables.datasetId
            WHERE datasets.namespace = %s
        """, [DATASET_NAMESPACE])
    }

    with open(filename_to_process, encoding='latin-1') as currentfile:
        currentreader = csv.DictReader(currentfile)
        filecolumns = tuple(currentreader.fieldnames)

        # these column types are very similar
        if any([starts_with(filecolumns, c) for c in column_types[0:5]]):
            for row in currentreader:
                if starts_with(filecolumns, column_types[0]):
                    countryname = row['Area']
                    variablename = row['Item']
                    variablecode = row['Item Code']
                if starts_with(filecolumns, column_types[1]):
                    countryname = row['Country']
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                    variablecode = '%s - %s' % (row['ItemCode'], row['ElementCode'])
                if starts_with(filecolumns, column_types[2]):
                    countryname = row['Area']
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                    variablecode = '%s - %s' % (row['Item Code'], row['Element Code'])
                if starts_with(filecolumns, column_types[3]):
                    countryname = row['Country']
                    variablename = '%s - %s' % (row['Item'], row['Element'])
                    variablecode = '%s - %s' % (row['Item Code'], row['Element Code'])
                if starts_with(filecolumns, column_types[4]):
                    countryname = row['Country']
                    variablename = '%s - %s' % (row['Indicator'], row['Source'])
                    variablecode = '%s - %s' % (row['Indicator Code'], row['Source Code'])

                if original_filename == 'Emissions_Agriculture_Energy_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                if original_filename == 'Production_LivestockPrimary_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                if original_filename == 'Trade_LiveAnimals_E_All_Data_(Normalized).zip':
                    variablename += ' - %s' % row['Unit']

                # avoiding duplicate rows
                if original_filename == 'Inputs_Pesticides_Use_E_All_Data_(Normalized).zip':
                    if row['Item Code'] not in current_file_var_codes and row['Item'] not in current_file_var_names:
                        current_file_var_codes.add(row['Item Code'])
                        current_file_var_names.add(row['Item'])
                    elif row['Item Code'] in current_file_var_codes and row['Item'] in current_file_var_names:
                        pass
                    else:
                        continue

                # avoiding duplicate rows
                if original_filename == 'FoodBalanceSheets_E_All_Data_(Normalized).csv':
                    temp_row = [rowvalue for rowkey, rowvalue in row.items()]
                    if tuple(temp_row) == previous_row:
                        previous_row = tuple(temp_row)
                        continue
                    else:
                        previous_row = tuple(temp_row)
                    if row['Item Code'] not in current_file_var_codes and row['Item'] not in current_file_var_names:
                        current_file_var_codes.add(row['Item Code'])
                        current_file_var_names.add(row['Item'])
                    elif row['Item Code'] in current_file_var_codes and row['Item'] in current_file_var_names:
                        pass
                    else:
                        continue

                try:
                    year = int(row['Year'])
                    value = float(row['Value'])
                except ValueError:
                    year = False
                    value = False

                variablename = file_dataset_names[original_filename] + ': ' + variablename

                current_file_vars_countries.add(tuple([countryname, variablecode]))

                process_one_row(year, value, countryname, variablecode, variablename, var_id_by_name,
                                row['Unit'], source_id_by_name[category_name], dataset_id_by_name[dataset_name], variable_description, data_values_tuple_list)

            unique_data_tracker.update(current_file_vars_countries)

        # these are the files that require several iterations over all rows
        if any([starts_with(filecolumns, c) for c in column_types[5:8]]):
            if starts_with(filecolumns, column_types[5]):
                iterations = [
                    {
                        'country_field': 'Donor Country',
                        'varname_format': '%s - Donors'
                    },
                    {
                        'country_field': 'Recipient Country',
                        'varname_format': '%s - Recipients'
                    }]
            if starts_with(filecolumns, column_types[6]):
                iterations = [
                    {
                        'country_field': 'Reporter Countries',
                        'varname_format': '%s - %s - Reporters'
                    },
                    {
                        'country_field': 'Partner Countries',
                        'varname_format': '%s - %s - Partners'
                    }]
            if starts_with(filecolumns, column_types[7]):
                iterations = [
                    {
                        'country_field': 'Donor',
                        'varname_format': '%s - %s - Donors'
                    },
                    {
                        'country_field': 'Recipient Country',
                        'varname_format': '%s - %s - Recipients'
                    }]
            for oneiteration in iterations:
                file_stream_holder = {}  # we will break down these files into smaller files
                dict_writer_holder = {}
                separate_files_names = {}  # we will keep the filenames in this dict
                unique_vars = []
                # first we collect all variable names
                currentfile.seek(0)
                row_counter = 0
                for row in currentreader:
                    if row['Year'] == 'Year':
                        continue
                    row_counter += 1
                    if row_counter % 300 == 0:
                        time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time
                    if starts_with(filecolumns, column_types[5]):
                        variablename = oneiteration['varname_format'] % row['Item']
                    if starts_with(filecolumns, column_types[6]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Element'])
                    if starts_with(filecolumns, column_types[7]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Purpose'])
                    if variablename not in unique_vars:
                        unique_vars.append(variablename)
                # then we break the dataset into files named after the variable names
                for varname in unique_vars:
                    separate_files_names[varname.replace('/', '+') + '.csv'] = varname
                    file_stream_holder[varname] = open(os.path.join('/tmp', varname.replace('/', '+') + '.csv'),
                                                       'w+', encoding='latin-1')
                    dict_writer_holder[varname] = csv.DictWriter(file_stream_holder[varname],
                                                                 fieldnames=['Country', 'Variable', 'Varcode', 'Year',
                                                                             'Unit', 'Value'])
                    dict_writer_holder[varname].writeheader()
                # go back to the beginning of the file
                currentfile.seek(0)
                row_counter = 0
                for row in currentreader:
                    if row['Year'] == 'Year':
                        continue
                    row_counter += 1
                    if row_counter % 300 == 0:
                        time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time
                    if starts_with(filecolumns, column_types[5]):
                        variablename = oneiteration['varname_format'] % row['Item']
                        variablecode = row['Item Code']
                        dict_writer_holder[variablename].writerow({'Country': row[oneiteration['country_field']], 'Variable': variablename,
                                                                   'Varcode': variablecode, 'Unit': row['Unit'],
                                                                   'Year': row['Year'], 'Value': row['Value']})
                    if starts_with(filecolumns, column_types[6]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Element'])
                        variablecode = '%s - %s' % (row['Item Code'], row['Element Code'])
                        dict_writer_holder[variablename].writerow({'Country': row[oneiteration['country_field']], 'Variable': variablename,
                                                                   'Varcode': variablecode, 'Unit': row['Unit'], 'Year': row['Year'],
                                                                   'Value': row['Value']})
                    if starts_with(filecolumns, column_types[7]):
                        variablename = oneiteration['varname_format'] % (row['Item'], row['Purpose'])
                        variablecode = '%s - %s' % (row['Item Code'], row['Purpose Code'])
                        dict_writer_holder[variablename].writerow({'Country': row[oneiteration['country_field']], 'Variable': variablename,
                                                                   'Varcode': variablecode, 'Unit': row['Unit'],
                                                                   'Year': row['Year'], 'Value': row['Value']})
                    if row_counter % 100000 == 0:
                        for fileholder, actual_file in file_stream_holder.items():
                            actual_file.flush()
                            os.fsync(actual_file.fileno())
                for fileholder, actual_file in file_stream_holder.items():
                    actual_file.close()

                # now parsing and importing each file individually

                for each_separate_file, file_variable_name in separate_files_names.items():
                    unique_records_holder = {}
                    with open('/tmp/%s' % each_separate_file, encoding='latin-1') as separate_file:
                        separate_file_reader = csv.DictReader(separate_file)
                        row_counter = 0
                        for row in separate_file_reader:
                            row_counter += 1
                            if row_counter % 300 == 0:
                                time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time
                            countryname = row['Country']
                            variablecode = row['Varcode']
                            variableunit = row['Unit']
                            year = row['Year']
                            value = row['Value']

                            try:
                                year = int(year)
                                value = float(value)
                            except ValueError:
                                year = False
                                value = False
                            if year is not False and value is not False:
                                unique_record = tuple([countryname, year])
                                if unique_record not in unique_records_holder:
                                    unique_records_holder[unique_record] = value
                                else:
                                    unique_records_holder[unique_record] += value
                    for key, value in unique_records_holder.items():
                        variablename = file_dataset_names[original_filename] + ': ' + file_variable_name
                        process_one_row(list(key)[1], str(value), list(key)[0], variablecode, variablename, var_id_by_name,
                                        variableunit, source_id_by_name[category_name], dataset_id_by_name[dataset_name], variable_description, data_values_tuple_list)

                    os.remove('/tmp/%s' % each_separate_file)

        if len(data_values_tuple_list):  # insert any leftover data_values
            with connection.cursor() as c:
                c.executemany(insert_string, data_values_tuple_list)


def process_one_row(year, value, countryname, variablecode, variablename, var_id_by_name,
                    unit, source_id, dataset_id, var_desc, data_values_tuple_list):

    global unique_data_tracker
    global processed_values
    global var_ids_to_delete
    global db

    processed_values += 1
    if processed_values % 300 == 0:
        time.sleep(0.001)  # this is done in order to not keep the CPU busy all the time

    insert_string = 'INSERT into data_values (value, year, entityId, variableId) VALUES (%s, %s, %s, %s)'  # this is used for constructing the query for mass inserting to the data_values table

    if year is not False and value is not False:
        if tuple([countryname, variablecode]) not in unique_data_tracker:
            entity_id = db.get_or_create_entity(countryname)

            # Daniel: I decided not to use variable codes any more because they
            # are not unique and cause conflicts. Previously, they were added
            # but then set to NULL if they conflicted with an existing variable.
            # I think this makes them pointless since different variables will
            # get a code based on the order of imports.
            if variablename not in var_id_by_name or var_id_by_name[variablename] in var_ids_to_delete:
                var_id_by_name[variablename] = db.upsert_variable(
                    name=variablename,
                    code=None,
                    unit=unit if unit else '',
                    short_unit=extract_short_unit(unit),
                    description=var_desc,
                    dataset_id=dataset_id,
                    source_id=source_id
                )
                if var_id_by_name[variablename] in var_ids_to_delete:
                    del var_ids_to_delete[var_ids_to_delete.index(var_id_by_name[variablename])]

            data_values_tuple_list.append(
                (str(value), int(year), entity_id, var_id_by_name[variablename])
            )
            if len(data_values_tuple_list) > 3000:  # insert when the length of the list goes over 3000
                with connection.cursor() as c:
                    c.executemany(insert_string, data_values_tuple_list)
                del data_values_tuple_list[:]


with connection as c:

    global import_history_states

    db = DBUtils(c)

    import_history_states = [
        json.loads(row[0])
        for row in db.fetch_many("""
            SELECT import_state
            FROM importer_importhistory
            WHERE import_type = %s
        """, DATASET_NAMESPACE)
    ]

    # The user ID that gets assigned in every user ID field
    (user_id,) = db.fetch_one("""
        SELECT id FROM users WHERE email = 'daniel@gavrilov.co.uk'
    """)

    parent_tag_id = db.upsert_parent_tag(PARENT_TAG_NAME)

    tag_id_by_name = {
        name: i
        for name, i in db.fetch_many("""
            SELECT name, id
            FROM tags
            WHERE parentId = %s
        """, parent_tag_id)
    }

    dataset_id_by_name = {}

    source_id_by_name = {}

    unique_data_tracker = set()  # this set will keep track of variable-country combinations

    for eachfile in glob.glob(os.path.join(all_dataset_files_dir, "*.zip")):
        if os.path.basename(eachfile) not in files_to_exclude:
            file_extracted = 1
            zip_ref = zipfile.ZipFile(eachfile, 'r')
            csv_filename = zip_ref.namelist()[0]
            try:
                zip_ref.extractall("/tmp")
            except:
                file_extracted = 0
                print("Could not extract file: %s" % eachfile)
            zip_ref.close()

            if file_extracted:
                file_imported_before = False
                for state in import_history_states:
                    if state['file_name'] == os.path.basename(eachfile):
                        file_imported_before = True
                        imported_before_hash = state['file_hash']
                if not file_imported_before:
                    process_csv_file_insert("/tmp/%s" % csv_filename, os.path.basename(eachfile))
                    os.remove("/tmp/%s" % csv_filename)
                    db.note_import(
                        import_type=DATASET_NAMESPACE,
                        import_notes='Importing file %s' % os.path.basename(eachfile),
                        import_state=json.dumps({
                            'file_hash': file_checksum(eachfile),
                            'file_name': os.path.basename(eachfile)
                        })
                    )
                else:
                    if imported_before_hash == file_checksum(eachfile):
                        print('No updates available for file %s.' % os.path.basename(eachfile))
                    else:
                        process_csv_file_update("/tmp/%s" % csv_filename, os.path.basename(eachfile))
                        os.remove("/tmp/%s" % csv_filename)
                        db.note_import(
                            import_type=DATASET_NAMESPACE,
                            import_notes='Importing file %s' % os.path.basename(eachfile),
                            import_state=json.dumps({
                                'file_hash': file_checksum(eachfile),
                                'file_name': os.path.basename(eachfile)
                            })
                        )

    for eachfile in glob.glob(os.path.join(all_dataset_files_dir, "*.csv")):
        if os.path.basename(eachfile) not in files_to_exclude:
            file_imported_before = False
            for state in import_history_states:
                if state['file_name'] == os.path.basename(eachfile):
                    file_imported_before = True
                    imported_before_hash = state['file_hash']
            if not file_imported_before:
                process_csv_file_insert(eachfile, os.path.basename(eachfile))
                db.note_import(
                    import_type=DATASET_NAMESPACE,
                    import_notes='Importing file %s' % os.path.basename(eachfile),
                    import_state=json.dumps({
                        'file_hash': file_checksum(eachfile),
                        'file_name': os.path.basename(eachfile)
                    })
                )
            else:
                if imported_before_hash == file_checksum(eachfile):
                    print('No updates available for file %s.' % os.path.basename(eachfile))
                else:
                    process_csv_file_update(eachfile, os.path.basename(eachfile))
                    db.note_import(
                        import_type=DATASET_NAMESPACE,
                        import_notes='Importing file %s' % os.path.basename(eachfile),
                        import_state=json.dumps({
                            'file_hash': file_checksum(eachfile),
                            'file_name': os.path.basename(eachfile)
                        })
                    )

print("Script execution time: %s" % (datetime.now() - start_time))
