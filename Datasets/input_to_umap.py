import pandas as pd
import numpy as np
import os
import umap
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

def apply_umap():
    
    script_dir = "Datasets"
    
    input_path = "Datasets/dataset.csv"
    output_path = os.path.join(script_dir, 'umap_dataset.csv')

    df = pd.read_csv(input_path)

    y = df['price']
    X = df.drop(columns=['price'])

    numeric_features = X.select_dtypes(include=['int64', 'float64']).columns.tolist()
    categorical_features = X.select_dtypes(include=['object', 'bool']).columns.tolist()
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ])

    X_processed = preprocessor.fit_transform(X)
    reducer = umap.UMAP(n_components=5, random_state=42)
    X_umap = reducer.fit_transform(X_processed)

    umap_cols = [f'UMAP{i+1}' for i in range(X_umap.shape[1])]
    umap_df = pd.DataFrame(X_umap, columns=umap_cols)
    umap_df['price'] = y.values
    umap_df.to_csv(output_path, index=False)
    
apply_umap()