import pandas as pd
import os

def combine_cleaned_data():
    col = [
        'surface_m2', 'rooms', 'floor', 'max_floor', 'price', 
        'neighborhood', 'year_built', 'elevator', 'construction_material', 
        'latitude', 'longitude', 'metro_proximity', 'stb_proximity'
    ]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    storia_df = pd.read_csv(os.path.join(script_dir, 'storia_cleaned.csv'))[col]
    imobiliare_df = pd.read_csv(os.path.join(script_dir, 'imobiliare_cleaned.csv'))[col]
    combined_df = pd.concat([storia_df, imobiliare_df], ignore_index=True)
    combined_df.to_csv(os.path.join(script_dir, 'full_clean_input.csv'), index=False)

combine_cleaned_data()