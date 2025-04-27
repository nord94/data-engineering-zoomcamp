#!/bin/bash

# URL of the Global Power Plant Database Excel file
data_url="https://datasets.wri.org/dataset/8c19235a-3fef-4c8b-ae00-96a98c231ab5/resource/b6964da2-e2c2-4dd4-9ab4-fb8ba09f4ac0/download/global_power_plant_database.xlsx"

# PostgreSQL database connection details
DB_NAME="test"
DB_USER="test"
DB_PASS="test"
DB_HOST="localhost:5432"
TABLE_NAME="global_power_plants"

# Download the Excel file
curl -L "$data_url" -o global_power_plant_database.xlsx

# Convert Excel to CSV using in2csv (csvkit)
in2csv --sheet "global_power_plant_database" global_power_plant_database.xlsx > global_power_plants.csv

# Drop existing table and create a new one based on CSV headers
csvsql --db "postgresql://$DB_USER:$DB_PASS@$DB_HOST/$DB_NAME" \
       --tables "$TABLE_NAME" --no-inference --overwrite global_power_plants.csv

# Import CSV data into PostgreSQL database
csvsql --db "postgresql://$DB_USER:$DB_PASS@$DB_HOST/$DB_NAME" \
       --tables "$TABLE_NAME" --insert global_power_plants.csv

# Cleanup
rm global_power_plant_database.xlsx global_power_plants.csv

echo "Data loaded successfully into PostgreSQL."
