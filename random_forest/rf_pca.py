from pathlib import Path
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split, KFold, cross_val_score, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from sklearn.ensemble import RandomForestRegressor

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR.parent / "Datasets" / "pca_dataset.csv"
MODEL_FILE = BASE_DIR / "rf_pipeline_pca.joblib"
PLOT_DIR = BASE_DIR

HYPERPARAM_SEARCH = True
HYPERPARAM_GRID = [
    {"n_estimators": 200, "min_samples_split": 2, "max_depth": 6},
    {"n_estimators": 300, "min_samples_split": 2, "max_depth": 4},
    {"n_estimators": 400, "min_samples_split": 5, "max_depth": 6},
    {"n_estimators": 150, "min_samples_split": 2, "max_depth": 8},
    {"n_estimators": 500, "min_samples_split": 5, "max_depth": 4},
    {"n_estimators": 800, "min_samples_split": 10, "max_depth": 6},
    {"n_estimators": 300, "min_samples_split": 10, "max_depth": 3},
    {"n_estimators": 200, "min_samples_split": 5, "max_depth": 8},
    {"n_estimators": 600, "min_samples_split": 10, "max_depth": 4},
]


def load_data():
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        sys.exit(1)

    df = pd.read_csv(DATA_FILE)
    if "price" not in df.columns:
        print("Missing 'price' column in dataset.")
        sys.exit(1)

    df = df.dropna(subset=["price"]).reset_index(drop=True)
    if df.shape[0] < 10:
        print("Not enough rows with price to train.")
        sys.exit(1)

    X = df.drop(columns=["price"]).copy()
    y = df["price"].values
    return X, y


def build_model_pipeline(model_params=None):
    params = {"n_estimators": 200, "min_samples_split": 2, "max_depth": 6, "random_state": 42, "n_jobs": -1}
    if model_params:
        params.update(model_params)
    model = RandomForestRegressor(**params)
    return Pipeline(steps=[("model", model)])


def save_plots(y_true, y_pred, suffix=""):
    suffix = f"_{suffix}" if suffix else ""

    plt.figure(figsize=(7, 6))
    sns.scatterplot(x=y_true, y=y_pred, alpha=0.6)
    max_val = max(np.max(y_true), np.max(y_pred))
    plt.plot([0, max_val], [0, max_val], color="red", linestyle="--", linewidth=1)
    plt.xlabel("Actual price")
    plt.ylabel("Predicted price")
    plt.title("Predicted vs Actual")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / f"predicted_vs_actual{suffix}.png", dpi=150)
    plt.close()

    residuals = y_true - y_pred
    plt.figure(figsize=(7, 6))
    sns.scatterplot(x=y_pred, y=residuals, alpha=0.6)
    plt.axhline(0, color="red", linestyle="--", linewidth=1)
    plt.xlabel("Predicted price")
    plt.ylabel("Residual (actual - predicted)")
    plt.title("Residuals vs Predicted")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / f"residuals_vs_predicted{suffix}.png", dpi=150) 
    plt.close()

    plt.figure(figsize=(7, 6))
    sns.histplot(residuals, bins=40, kde=True)
    plt.xlabel("Residual (actual - predicted)")
    plt.title("Residual Distribution")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / f"residual_distribution{suffix}.png", dpi=150)
    plt.close()

    return [
        f"predicted_vs_actual{suffix}.png",
        f"residuals_vs_predicted{suffix}.png",
        f"residual_distribution{suffix}.png",
    ]


def save_feature_importances(pipeline, feature_names, suffix=""):
    importances = pipeline.named_steps["model"].feature_importances_
    fi = pd.DataFrame({"feature": feature_names, "importance": importances})
    fi = fi.sort_values("importance", ascending=False).head(20)
    plt.figure(figsize=(8, 6))
    sns.barplot(data=fi, x="importance", y="feature")
    plt.title("Top Feature Importances")
    plt.tight_layout()
    suffix = f"_{suffix}" if suffix else ""
    plt.savefig(PLOT_DIR / f"feature_importances{suffix}.png", dpi=150)
    plt.close()

    return f"feature_importances{suffix}.png"


def evaluate_pipeline(name, pipeline, X, y, X_train, X_test, y_train, y_test, cv, feature_names, suffix="", save_model_path=None, save_artifacts=True):
    print(f"\n{name}")
    print("Running cross-validation (MAE)...")
    cv_mae = -cross_val_score(pipeline, X, y, scoring="neg_mean_absolute_error", cv=cv, n_jobs=-1)
    print(f"CV MAE: mean={cv_mae.mean():.2f}, std={cv_mae.std():.2f}")

    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)
    rmsle = np.sqrt(mean_squared_error(np.log1p(y_test), np.log1p(np.maximum(preds, 0.0))))
    print(f"MAE={mae:.2f}, RMSE={rmse:.2f}, R2={r2:.3f}, RMSLE={rmsle:.3f}")

    sample_df = pd.DataFrame({"actual_price": y_test, "predicted_price": preds})
    sample_df["abs_error"] = np.abs(sample_df["actual_price"] - sample_df["predicted_price"])
    sample_df = sample_df.sample(n=min(10, len(sample_df)), random_state=42).round(2)
    print("Sample predictions vs actual:")
    print(sample_df.to_string(index=False))

    if save_artifacts:
        save_plots(y_test, preds, suffix=suffix)
        save_feature_importances(pipeline, feature_names=feature_names, suffix=suffix)

        if save_model_path:
            joblib.dump(pipeline, save_model_path)
            print(f"Saved trained pipeline to: {save_model_path}")

    return {"mae": mae, "rmse": rmse, "r2": r2, "rmsle": rmsle}


def main():
    X, y = load_data()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    sns.set_theme(style="whitegrid")

    if HYPERPARAM_SEARCH:
        grid_params = [
            {
                "model__n_estimators": [params["n_estimators"]],
                "model__min_samples_split": [params["min_samples_split"]],
                "model__max_depth": [params["max_depth"]],
            }
            for params in HYPERPARAM_GRID
        ]
        search = GridSearchCV(
            build_model_pipeline(),
            grid_params,
            scoring="neg_mean_absolute_error",
            cv=cv,
            n_jobs=-1,
        )
        search.fit(X, y)
        best_params = {k.replace("model__", ""): v for k, v in search.best_params_.items()}
        print(f"Best params: {best_params}")
        print(f"Best CV MAE: {-search.best_score_:.2f}")

        pipeline = build_model_pipeline(model_params=best_params)
        evaluate_pipeline(
            f"PCA best params={best_params}",
            pipeline,
            X,
            y,
            X_train,
            X_test,
            y_train,
            y_test,
            cv,
            feature_names=X.columns,
            suffix="pca_best",
            save_model_path=None,
            save_artifacts=False,
        )
        return

    pipeline = build_model_pipeline()
    metrics = evaluate_pipeline(
        "PCA",
        pipeline,
        X,
        y,
        X_train,
        X_test,
        y_train,
        y_test,
        cv,
        feature_names=X.columns,
        suffix="pca",
        save_model_path=MODEL_FILE,
    )

    print("\nSummary metrics:")
    print(f"pca: MAE={metrics['mae']:.2f}, RMSE={metrics['rmse']:.2f}, R2={metrics['r2']:.3f}, RMSLE={metrics['rmsle']:.3f}")


if __name__ == "__main__":
    main()
