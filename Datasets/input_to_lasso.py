import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from sklearn.linear_model import LassoCV, Lasso
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

def apply_lasso():
    
    input_path = 'Datasets/dataset.csv'
    output_path =  'Datasets/lasso_dataset.csv'
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

    print("Preprocessing data...")
    X_processed = preprocessor.fit_transform(X)

    feature_names = preprocessor.get_feature_names_out()
    feature_names = [name.split('__', 1)[-1] for name in feature_names]

    alphas = np.logspace(-2, 4, 50)
    lasso_cv = LassoCV(alphas=alphas, cv=5, random_state=42, max_iter=10000)
    lasso_cv.fit(X_processed, y)
    best_alpha = lasso_cv.alpha_
    coef = lasso_cv.coef_
    selected_mask = coef != 0
    selected_features = np.array(feature_names)[selected_mask]
    
    print(f"features selected by Lasso: {len(selected_features)}")

    X_lasso = X_processed[:, selected_mask]
    lasso_df = pd.DataFrame(X_lasso, columns=selected_features)
    lasso_df['price'] = y.values

    lasso_df.to_csv(output_path, index=False)
    print(f"Success! Lasso dataset saved to: {output_path}")

    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    m_log_alphas = -np.log10(lasso_cv.alphas_)
    mean_mse = lasso_cv.mse_path_.mean(axis=-1)
    plt.plot(m_log_alphas, mean_mse, label='Mean MSE Across Folds', color='blue')
    plt.axvline(-np.log10(best_alpha), linestyle='--', color='k', label='Optimal Alpha')
    plt.xlabel('-log10(alpha)')
    plt.ylabel('Mean Squared Error (Loss)')
    plt.title('Cross-Validation Loss vs Alpha')
    plt.legend()
    plt.subplot(1, 2, 2)
    non_zeros = []
    for alpha in lasso_cv.alphas_:
        l = Lasso(alpha=alpha, max_iter=1000)
        l.fit(X_processed, y)
        non_zeros.append(np.sum(l.coef_ != 0))

    plt.plot(m_log_alphas, non_zeros, color='green')
    plt.axvline(-np.log10(best_alpha), linestyle='--', color='k', label='Optimal Alpha')
    plt.xlabel('-log10(alpha)')
    plt.ylabel('Number of Non-Zero Features')
    plt.title('Features Retained vs Alpha')
    plt.legend()

    plt.tight_layout()
    plot_path =  'Datasets/lasso_plot.png'
    plt.savefig(plot_path)
    plt.show()

apply_lasso()