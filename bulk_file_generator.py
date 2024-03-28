import os
import glob
import pandas as pd

equity_folder = 'equity'

# Get a list of all directories in the equity folder
directories = [d for d in os.listdir(equity_folder) if os.path.isdir(os.path.join(equity_folder, d))]

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame()

# Iterate over each directory
for directory in directories:
    # Get a list of all .csv files in the directory
    csv_files = glob.glob(os.path.join(equity_folder, directory, '*.csv'))
    
    # Read each .csv file and append it to the combined DataFrame
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        columns = list(df.columns)
        for index, column in enumerate(columns):
            split = column.split('.')
            if len(split) == 1:
                columns[index] = column + " OPEN"
            elif split[1] == '1':
                columns[index] = split[0] + " HIGH"
            elif split[1] == '2':
                columns[index] = split[0] + " LOW"
            elif split[1] == '3':
                columns[index] = split[0] + " LAST"
            elif split[1] == '4':
                columns[index] = split[0] + " VOLUME"
        df.columns = columns
        #join the dataframes
        print(csv_file)
        try:
            combined_df = pd.merge(combined_df, df, how='outer')
        except pd.errors.MergeError:
            print("Error merging dataframes")
            combined_df = df

            
# remove all rows before 2020
combined_df = combined_df[combined_df.iloc[:, 0] >= '2020-01-01']
print(combined_df.head())

# Write the combined DataFrame to a new .csv file
combined_df.to_csv('combined_equity_data.csv', index=False)


# convert the first column of combined_df to datetime
combined_df.iloc[:, 0] = pd.to_datetime(combined_df.iloc[:, 0])


# look through each .csv file in each directory in the 'equity' folder and print out each date in the first column
# of each .csv file
for directory in directories:
    csv_files = glob.glob(os.path.join(equity_folder, directory, '*.csv'))
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
       
        df.iloc[:, 0] = df.iloc[:, 0].isin(combined_df.iloc[:, 0])
        # if there are any false values in the first column of the dataframe, print the name of the .csv file
        if False in df.iloc[:, 0]:
            print(csv_file)

# loop through the combined dataframe and create a .csv with each column name and its correponding index
column_names = list(combined_df.columns)
column_index = list(range(len(column_names)))
column_dict = dict(zip(column_names, column_index))
column_df = pd.DataFrame(column_dict.items(), columns=['Column Name', 'Index'])
column_df.to_csv('equity_index.csv', index=False)


equity_folder = 'factor\eps'

# Get a list of all directories in the equity folder
directories = [d for d in os.listdir(equity_folder) if os.path.isdir(os.path.join(equity_folder, d))]

# Initialize an empty DataFrame to store the combined data
combined_df = pd.DataFrame()

# Iterate over each directory
for directory in directories:
    # Get a list of all .csv files in the directory
    csv_files = glob.glob(os.path.join(equity_folder, directory, '*.csv'))
    
    # Read each .csv file and append it to the combined DataFrame
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        columns = list(df.columns)
        for index, column in enumerate(columns):
            split = column.split('.')
            if len(split) == 1:
                columns[index] = column + " BEST_EPS"
            elif split[1] == '1':
                columns[index] = split[0] + " BEST_EPS_NXT_YR"
        df.columns = columns
        #join the dataframes
        print(csv_file)
        try:
            combined_df = pd.merge(combined_df, df, how='outer')
        except pd.errors.MergeError:
            print("Error merging dataframes")
            combined_df = df

# move the last row to be the second row
            

combined_df = combined_df[combined_df.iloc[:, 0] >= '2020-01-01']
print(combined_df.head())
# Write the combined DataFrame to a new .csv file
combined_df.to_csv('combined_factor_eps_data.csv', index=False)


# convert the first column of combined_df to datetime
combined_df.iloc[:, 0] = pd.to_datetime(combined_df.iloc[:, 0])


# look through each .csv file in each directory in the 'equity' folder and print out each date in the first column
# of each .csv file
for directory in directories:
    csv_files = glob.glob(os.path.join(equity_folder, directory, '*.csv'))
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
       
        df.iloc[:, 0] = df.iloc[:, 0].isin(combined_df.iloc[:, 0])
        # if there are any false values in the first column of the dataframe, print the name of the .csv file
        if False in df.iloc[:, 0]:
            print(csv_file)

column_names = list(combined_df.columns)
column_index = list(range(len(column_names)))
column_dict = dict(zip(column_names, column_index))
column_df = pd.DataFrame(column_dict.items(), columns=['Column Name', 'Index'])
column_df.to_csv('factor_eps_index.csv')

