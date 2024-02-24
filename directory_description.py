import os
import csv

# Path to the 'equity' directory
directory = 'equity'

# Get a list of all folders inside the 'equity' directory
folders = [folder for folder in os.listdir(directory) if os.path.isdir(os.path.join(directory, folder))]

# Path to the output CSV file
output_file = 'equity_folder_file_list.csv'

# Open the CSV file in write mode
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(['Folder Name', 'File Name'])

    # Write each folder name as a row in the CSV file
    for folder in folders:
        #writer.writerow([folder])
        # for each folder, write the files in the folder in the next column
        files = os.listdir(os.path.join(directory, folder))
        for file in files:
            writer.writerow([folder, file])

print(f"CSV file '{output_file}' created successfully.")

# Path to the 'equity' directory
directory = 'factor\eps'

# Get a list of all folders inside the 'equity' directory
folders = [folder for folder in os.listdir(directory) if os.path.isdir(os.path.join(directory, folder))]

# Path to the output CSV file
output_file = 'factor_folder_file_list.csv'

# Open the CSV file in write mode
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(['Folder Name', 'File Name'])

    # Write each folder name as a row in the CSV file
    for folder in folders:
        #writer.writerow([folder])
        # for each folder, write the files in the folder in the next column
        files = os.listdir(os.path.join(directory, folder))
        for file in files:
            writer.writerow([folder, file])

print(f"CSV file '{output_file}' created successfully.")