# -*- coding: utf-8 -*-
"""
Created on Sat Nov  2 16:28:52 2024

@author: Shahroz
"""

import pandas as pd
import os

def select_and_rename_columns(df):
    """
    Selects relevant columns and renames them for clarity.
    """
    selected_columns = [
        'UPRN', 'UDPRN', 'EASTING', 'NORTHING', 'LATITUDE', 'LONGITUDE', 
        'POSTCODE', 'CATEGORY', 'ORGANISATION', 'BUILDING_NAME', 
        'SUB_BUILDING', 'BUILDING_NUMBER', 'STREET_NAME', 'TOWN_NAME', 
        'EASTING', 'NORTHING', 'CATEGORY', 'BOROUGH', 'PARENT_UPRN', 
        'CLASSIFICATION_CODE', 'MDU_COUNT'
    ]
    
    renamed_columns = [
        'uprn', 'udprn', 'x_coordina', 'y_coordina', 'latitude', 
        'longitude', 'postcode', 'category', 'Organisation name', 
        'Building name', 'Sub building name', 'Building number', 
        'Street', 'Town', 'Eastings', 'Northings', 'main_category', 
        'Borough', 'parent_uprn', 'classification_code', 'mdu_count'
    ]
    
    filtered_df = df[selected_columns]
    filtered_df.columns = renamed_columns
    return filtered_df

def validate_and_insert_columns(df):
    """
    Ensures the dataframe has a fixed number of columns and inserts additional columns as needed.
    """
    max_columns = 29
    current_columns = df.columns.tolist()
    
    while len(current_columns) < max_columns:
        current_columns.append(f'Empty_{len(current_columns)}')
    
    df = df.reindex(columns=current_columns)
    
    # Insert specific value columns
    df.insert(14, 'Council', '')
    df.insert(19, 'year_created', '2024')
    df.insert(20, 'house_polygon', '')
    df.insert(21, 'geometry', '')
    df.insert(22, 'udprn_available', '')
    df.insert(23, 'highest_point_latitude', '')
    df.insert(24, 'highest_point_longitude', '')
    df.insert(26, 'has_parent_uprn', '')
    
    return df

def clean_and_filter_data(df):
    """
    Cleans the data by removing placeholder columns, dropping unnecessary columns,
    and filtering out unwanted categories.
    """
    # Drop placeholder and classification_code columns
    df = df.drop(columns=[col for col in df.columns if 'Empty_' in col])
    df = df.drop(columns='classification_code')

    # Remove unwanted categories
    categories_to_remove = ['Land and Pathways', 'Street Furniture', 'Utilities']
    df = df[~df['category'].isin(categories_to_remove)]
    
    return df

def assign_main_category(df):
    """
    Assigns a main category based on the predefined categories for residential and business.
    """
    residential_ls = ['residential', 'multi-occupancy residential', 'unclassified']
    business_ls = ['industrial', 'leisure', 'multi-occupancy commercial',
                   'place of worship', 'public sectors', 'pubs and hotels']
    
    main_category = []
    for category in df['category']:
        category_lower = category.lower()
        if category_lower in residential_ls:
            main_category.append('residential')
        elif category_lower in business_ls:
            main_category.append('business')
        else:
            main_category.append('other')  # Optional fallback for uncategorized data

    df['main_category'] = main_category
    return df

def main(input_file, output_directory=None):
    """
    Main function to coordinate the data processing steps.
    
    Parameters:
    input_file (str): Path to the input CSV file
    output_directory (str, optional): Directory where the output CSV will be saved.
                                    If None, saves in the same directory as the input file.
    
    Returns:
    str: Path to the saved CSV file
    """
    # Read the input file
    all_addresses = pd.read_csv(input_file)
    
    # Process the data
    filtered_df = select_and_rename_columns(all_addresses)
    filtered_df = validate_and_insert_columns(filtered_df)
    filtered_df = clean_and_filter_data(filtered_df)
    filtered_df = assign_main_category(filtered_df)
    filtered_df = filtered_df[filtered_df['main_category']!="other"]
    
    # Create output filename
    input_filename = os.path.basename(input_file)
    output_filename = f"processed_{input_filename}"
    
    # Determine output directory
    if output_directory is None:
        output_directory = os.path.dirname(input_file)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Combine directory and filename for full output path
    output_path = os.path.join(output_directory, output_filename)
    
    # Save the processed data
    filtered_df.to_csv(output_path, index=False)
    
    print(f"Processed data saved to: {output_path}")
    return output_path

# Example usage:
if __name__ == "__main__":
    input_file = 'C:/Users/Shahroz/Desktop/Shahroz/Postcode Sector/Combined onnet 11.11.24/GPON/Address_ABP_Premium/Blackburn/processed_data_formatted_within_boundary/merged_addresses.csv'
    output_directory = 'C:/Users/Shahroz/Desktop/Shahroz/Postcode Sector/Combined onnet 11.11.24/GPON/Address_ABP_Premium/Blackburn/processed_data_formatted_within_boundary/processed_data_formatted'
    
    output_path = main(input_file, output_directory)