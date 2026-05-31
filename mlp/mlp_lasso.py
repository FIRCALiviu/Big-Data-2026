from pathlib import Path
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.compose import TransformedTargetRegressor
from sklearn.model_selection import train_test_split, KFold, cross_val_score, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from sklearn.neural_network import MLPRegressor

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR.parent / "Datasets" / "dataset.csv"
MODEL_FILE = BASE_DIR / "mlp_pipeline_lasso.joblib"
PLOT_DIR = BASE_DIR

HYPERPARAM_SEARCH = True
HYPERPARAM_GRID = [
    {"hidden_layer_sizes": (128, 64), "alpha": 0.0001, "learning_rate_init": 0.001},
    {"hidden_layer_sizes": (128, 64), "alpha": 0.001, "learning_rate_init": 0.001},
    {"hidden_layer_sizes": (64, 32), "alpha": 0.0001, "learning_rate_init": 0.001},
    {"hidden_layer_sizes": (64, 32), "alpha": 0.001, "learning_rate_init": 0.001},
    {"hidden_layer_sizes": (128, 64, 32), "alpha": 0.0001, "learning_rate_init": 0.0005},
    {"hidden_layer_sizes": (256, 128), "alpha": 0.0001, "learning_rate_init": 0.0005},
]
APPLY_LOG_TARGET = True
LOG_FEATURES = ["metro_proximity", "surface_m2"]


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
    X = apply_log_features(X)
    y = df["price"].values
    return X, y


def apply_log_features(X):
    X = X.copy()
    for col in LOG_FEATURES:
        if col in X.columns:
            col_vals = X[col]
            if (col_vals <= -1).any():
                continue
            X[col] = np.log1p(col_vals)
    return X


def build_model_pipeline(model_params=None):
    params = {
        "hidden_layer_sizes": (128, 64),
        "alpha": 0.0001,
        "learning_rate_init": 0.001,
        "max_iter": 400,
        "random_state": 42,
        "early_stopping": True,
        "tol": 1e-3,
        "n_iter_no_change": 10,
    }
    if model_params:
        params.update(model_params)
    model = MLPRegressor(**params)
    return Pipeline(steps=[("scaler", StandardScaler()), ("model", model)])


def build_estimator(model_params=None):
    pipeline = build_model_pipeline(model_params=model_params)
    if APPLY_LOG_TARGET:
        return TransformedTargetRegressor(regressor=pipeline, func=np.log1p, inverse_func=np.expm1)
    return pipeline


def normalize_best_params(best_params):
    cleaned = {}
    for k, v in best_params.items():
        k = k.replace("regressor__model__", "").replace("model__", "").replace("regressor__", "")
        cleaned[k] = v
    return cleaned


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


def get_pipeline_model(estimator):
    if hasattr(estimator, "regressor_"):
        return estimator.regressor_.named_steps["model"]
    return estimator.named_steps["model"]


def save_feature_importances(estimator, feature_names, suffix=""):
    model = get_pipeline_model(estimator)
    if not hasattr(model, "coefs_"):
        return None

    input_weights = model.coefs_[0]
    importances = np.mean(np.abs(input_weights), axis=1)
    fi = pd.DataFrame({"feature": feature_names, "importance": importances})
    fi = fi.sort_values("importance", ascending=False).head(20)

    plt.figure(figsize=(8, 6))
    sns.barplot(data=fi, x="importance", y="feature")
    plt.title("Top Feature Importances (MLP)")
    plt.tight_layout()
    suffix = f"_{suffix}" if suffix else ""
    plt.savefig(PLOT_DIR / f"feature_importances{suffix}.png", dpi=150)
    plt.close()

    return f"feature_importances{suffix}.png"


def evaluate_pipeline(name, estimator, X, y, X_train, X_test, y_train, y_test, cv, feature_names, suffix="", save_model_path=None, save_artifacts=True):
    print(f"\n{name}")
    print("Running cross-validation (MAE)...")
    cv_mae = -cross_val_score(estimator, X, y, scoring="neg_mean_absolute_error", cv=cv, n_jobs=1)
    print(f"CV MAE: mean={cv_mae.mean():.2f}, std={cv_mae.std():.2f}")

    estimator.fit(X_train, y_train)
    preds = estimator.predict(X_test)
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
        save_feature_importances(estimator, feature_names=feature_names, suffix=suffix)

        if save_model_path:
            joblib.dump(estimator, save_model_path)
            print(f"Saved trained pipeline to: {save_model_path}")

    return {"mae": mae, "rmse": rmse, "r2": r2, "rmsle": rmsle}


def main():
    X, y = load_data()
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    sns.set_theme(style="whitegrid")

    if HYPERPARAM_SEARCH:
        prefix = "regressor__model__" if APPLY_LOG_TARGET else "model__"
        grid_params = [
            {
                f"{prefix}hidden_layer_sizes": [params["hidden_layer_sizes"]],
                f"{prefix}alpha": [params["alpha"]],
                f"{prefix}learning_rate_init": [params["learning_rate_init"]],
            }
            for params in HYPERPARAM_GRID
        ]
        search = GridSearchCV(
            build_estimator(),
            grid_params,
            scoring="neg_mean_absolute_error",
            cv=cv,
            n_jobs=1,
        )
        search.fit(X, y)
        best_params = normalize_best_params(search.best_params_)
        print(f"Best params: {best_params}")
        print(f"Best CV MAE: {-search.best_score_:.2f}")

        pipeline = build_estimator(model_params=best_params)
        evaluate_pipeline(
            f"LASSO best params={best_params}",
            pipeline,
            X,
            y,
            X_train,
            X_test,
            y_train,
            y_test,
            cv,
            feature_names=X.columns,
            suffix="lasso_best",
            save_model_path=None,
            save_artifacts=False,
        )
        return

    pipeline = build_estimator()
    metrics = evaluate_pipeline(
        "LASSO",
        pipeline,
        X,
        y,
        X_train,
        X_test,
        y_train,
        y_test,
        cv,
        feature_names=X.columns,
        suffix="lasso",
        save_model_path=MODEL_FILE,
    )

    print("\nSummary metrics:")
    print(f"lasso: MAE={metrics['mae']:.2f}, RMSE={metrics['rmse']:.2f}, R2={metrics['r2']:.3f}, RMSLE={metrics['rmsle']:.3f}")


if __name__ == "__main__":
    main()
