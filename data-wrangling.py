import pandas as pd

def load_and_prepare_texas_data(file_path):
    ###
    ### Loads Texas data and prepares it by extracting the age in years and grouping by custom age groups.
    ###
    data = pd.read_csv(file_path)
    age_groups_custom = {
        'Under 15': list(range(0, 15)),
        '15 to 19': list(range(15, 20)),
        '20 to 24': list(range(20, 25)),
        '25 to 29': list(range(25, 30)),
        '30 to 34': list(range(30, 35)),
        '35 to 39': list(range(35, 40)),
        '40+': list(range(40, 100))
    }
    data['Numerical Age'] = data['Age'].str.extract('(\d+)').astype(int)
    data['Age Group'] = data['Numerical Age'].map({age: group for group, ages in age_groups_custom.items() for age in ages})
    return data

def aggregate_texas_data_by_age_and_race(data):
    ###
    ### Aggregates the provided Texas data by age groups and race, returning a DataFrame.
    ###
    races = ['White', 'Black', 'Asian', 'Other', 'Hispanic']
    aggregated_data = pd.DataFrame()
    for race in races:
        for age_group in data['Age Group'].unique():
            column_name = f"{race} {age_group}"
            race_age_group_sum = data[data['Age Group'] == age_group].groupby(['County', 'FIPS'])[f'Total {race}'].sum().reset_index(name=column_name)
            if aggregated_data.empty:
                aggregated_data = race_age_group_sum
            else:
                aggregated_data = pd.merge(aggregated_data, race_age_group_sum, on=['County', 'FIPS'], how='outer')
    return aggregated_data

def load_and_merge_itop_data(race_path, age_path):
    ###
    ### Loads ITOP data for race and age from specified paths, cleans, and merges them on 'County'.
    ###
    itop_race_data = pd.read_csv(race_path)
    itop_age_data = pd.read_csv(age_path)
    itop_age_data.columns = itop_age_data.iloc[0]
    itop_age_data = itop_age_data[1:].reset_index(drop=True)
    itop_age_data.columns = ['County', 'Total', 'Under 15 Years', '15 to 19 Years', '20 to 24 Years', '25 to 29 Years', '30 to 34 Years', '35 to 39 Years', '40+ Years']
    itop_race_data['County'] = itop_race_data['County'].str.strip()
    itop_age_data['County'] = itop_age_data['County'].str.strip()
    return pd.merge(itop_race_data, itop_age_data, on='County', how='outer')

def clean_and_save_merged_data(merged_data, output_file_path):
    ###
    ### Cleans the merged ITOP data by removing unnecessary columns and renames others, then saves to a CSV file.
    ###
    merged_data = merged_data.loc[:, ~merged_data.columns.str.contains('Unnamed')]
    merged_data.rename(columns={'Total': 'Total Population'}, inplace=True)
    return merged_data

def main():
    file_path = 'input-files/Texas_2021_Population_Aggregated.csv'
    texas_data = load_and_prepare_texas_data(file_path)
    aggregated_texas_data = aggregate_texas_data_by_age_and_race(texas_data)
    output_file_path = 'output-files/Aggregated_Texas_Data.csv'
    aggregated_texas_data.to_csv(output_file_path, index=False)

    # Example Usage:
    race_path = 'input-files/2021-itop-race-ethnicity-county.csv'
    age_path = 'input-files/2021-itop-age-county.csv'
    itop_merged_data = load_and_merge_itop_data(race_path, age_path)
    output_file_path = 'output-files/Texas_2021_ITOP_County_Age_Race.csv'
    itop_merged_data = clean_and_save_merged_data(itop_merged_data, output_file_path)
    itop_merged_data.to_csv(output_file_path, index=False)

main()