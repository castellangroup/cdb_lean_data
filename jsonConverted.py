import pandas as pd
# open equity_index.csv as a dataframe and store it as a json file
equity_index = pd.read_csv('equity_index.csv')
print(equity_index.head())
print(equity_index.dtypes)
equity_index.to_json('equity_index.json', orient='records')


equity_index = pd.read_csv('factor_eps_index.csv')
equity_index.to_json('factor_eps_index.json', orient='records')


equity_index = pd.read_json('equity_index.json')
print(equity_index.head())
print(equity_index.dtypes)