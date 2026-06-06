from pathlib import Path
import datetime
import os
import platform
import sys
import time
import resource

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split, KFold, cross_val_score, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from sklearn.neural_network import MLPRegressor

try:
    import psutil
except ImportError:
    psutil = None

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR.parent / "Datasets" / "dataset.csv"
MODEL_FILE = BASE_DIR / "mlp_pipeline_raw.joblib"
PLOT_DIR = BASE_DIR

LOG_FILE = None

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


def init_run_log(model_name):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return PLOT_DIR / f"{model_name}_run_{timestamp}.log"


def log(message):
    print(message)
    if LOG_FILE:
        with open(LOG_FILE, "a", encoding="utf-8") as handle:
            handle.write(f"{message}\n")


def get_memory_info():
    info = {}
    if psutil:
        proc = psutil.Process(os.getpid())
        mem = proc.memory_info()
        info["rss_bytes"] = mem.rss
        vm = psutil.virtual_memory()
        info["system_total_bytes"] = vm.total
        info["system_available_bytes"] = vm.available
        return info

    usage = resource.getrusage(resource.RUSAGE_SELF)
    info["rss_bytes"] = usage.ru_maxrss * 1024
    return info


def get_rss_bytes():
    if psutil:
        return psutil.Process(os.getpid()).memory_info().rss
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss * 1024


def average_bytes(samples):
    if not samples:
        return None
    return int(sum(samples) / len(samples))


def log_environment():
    log("=== Environment ===")
    log(f"Timestamp: {datetime.datetime.now().isoformat(timespec='seconds')}")
    log(f"Platform: {platform.platform()}")
    log(f"Python: {sys.version.replace(os.linesep, ' ')}")
    log(f"CPU count: {os.cpu_count()}")
    log(f"Memory: {get_memory_info()}")


def extract_first_number(value):
    if pd.isna(value):
        return None
    text = str(value)
    digits = []
    for char in text:
        if char.isdigit() or char in ".,-":
            digits.append(char)
    if not digits:
        return None
    cleaned = "".join(digits).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_price(value):
    if pd.isna(value):
        return None
    text = str(value).replace("\xa0", " ").lower()
    nums = []
    current = []
    for char in text:
        if char.isdigit() or char in ".,":
            current.append(char)
        elif current:
            nums.append("".join(current))
            current = []
    if current:
        nums.append("".join(current))
    if not nums:
        return None

    raw = nums[0].replace(" ", "")
    if raw.count(".") > 1 and raw.count(",") == 0:
        raw = raw.replace(".", "")
    elif raw.count(",") > 1 and raw.count(".") == 0:
        raw = raw.replace(",", "")
    elif raw.count(".") == 1 and raw.count(",") == 1:
        raw = raw.replace(".", "").replace(",", ".")
    elif raw.count(",") == 1 and raw.count(".") == 0:
        integer, decimal = raw.split(",")
        if len(decimal) == 3:
            raw = integer + decimal
        else:
            raw = raw.replace(",", ".")
    elif raw.count(".") == 1 and raw.count(",") == 0:
        integer, decimal = raw.split(".")
        if len(decimal) == 3:
            raw = integer + decimal

    try:
        amount = float(raw)
    except ValueError:
        return None

    if ("mil" in text or "mii" in text) and amount < 1000:
        amount *= 1000
    return amount


def prepare_dataframe(df):
    df = df.copy()
    for col in [
        "surface_m2",
        "year_built",
        "number_bathrooms",
        "latitude",
        "longitude",
        "metro_proximity",
        "stb_proximity",
    ]:
        if col in df.columns:
            df[col] = df[col].apply(extract_first_number)

    if "rooms" in df.columns:
        df["rooms"] = df["rooms"].apply(extract_first_number)

    if "floor" in df.columns:
        df["floor"] = df["floor"].apply(extract_first_number)

    if "price" in df.columns:
        df["price_value"] = df["price"].apply(parse_price)

    for col in ["city", "elevator", "construction_material"]:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": None, "N/A": None})

    return df


def build_preprocessor(numeric_features, categorical_features):
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )

    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ],
        remainder="drop",
    )


def build_model_pipeline(preprocessor, model_params=None):
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
    pipeline = Pipeline(steps=[("preprocess", preprocessor), ("model", model)])
    if APPLY_LOG_TARGET:
        return TransformedTargetRegressor(regressor=pipeline, func=np.log1p, inverse_func=np.expm1)
    return pipeline


def normalize_best_params(best_params):
    cleaned = {}
    for key, value in best_params.items():
        key = key.replace("regressor__model__", "").replace("model__", "").replace("regressor__", "")
        cleaned[key] = value
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


def get_pipeline_model(estimator):
    if hasattr(estimator, "regressor_"):
        return estimator.regressor_.named_steps["model"]
    return estimator.named_steps["model"]


def save_feature_importances(estimator, feature_names, suffix=""):
    model = get_pipeline_model(estimator)
    if not hasattr(model, "coefs_"):
        return None

    importances = np.mean(np.abs(model.coefs_[0]), axis=1)
    fi = pd.DataFrame({"feature": feature_names, "importance": importances})
    fi = fi.sort_values("importance", ascending=False).head(20)

    plt.figure(figsize=(8, 6))
    sns.barplot(data=fi, x="importance", y="feature")
    plt.title("Top Feature Importances (MLP)")
    plt.tight_layout()
    suffix = f"_{suffix}" if suffix else ""
    plt.savefig(PLOT_DIR / f"feature_importances{suffix}.png", dpi=150)
    plt.close()


def evaluate_pipeline(name, estimator, X, y, X_train, X_test, y_train, y_test, cv, feature_names, suffix="", save_model_path=None, save_artifacts=True):
    log(f"\n{name}")
    log("Running cross-validation (MAE)...")
    cv_mae = -cross_val_score(estimator, X, y, scoring="neg_mean_absolute_error", cv=cv, n_jobs=1)
    log(f"CV MAE: mean={cv_mae.mean():.2f}, std={cv_mae.std():.2f}")

    estimator.fit(X_train, y_train)
    preds = estimator.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)
    rmsle = np.sqrt(mean_squared_error(np.log1p(y_test), np.log1p(np.maximum(preds, 0.0))))
    log(f"MAE={mae:.2f}, RMSE={rmse:.2f}, R2={r2:.3f}, RMSLE={rmsle:.3f}")

    sample_df = pd.DataFrame({"actual_price": y_test, "predicted_price": preds})
    sample_df["abs_error"] = np.abs(sample_df["actual_price"] - sample_df["predicted_price"])
    sample_df = sample_df.sample(n=min(10, len(sample_df)), random_state=42).round(2)
    log("Sample predictions vs actual:")
    log(sample_df.to_string(index=False))

    if save_artifacts:
        save_plots(y_test, preds, suffix=suffix)
        save_feature_importances(estimator, feature_names=feature_names, suffix=suffix)

        if save_model_path:
            joblib.dump(estimator, save_model_path)
            log(f"Saved trained pipeline to: {save_model_path}")

    return {"mae": mae, "rmse": rmse, "r2": r2, "rmsle": rmsle}


def main():
    global LOG_FILE
    LOG_FILE = init_run_log("mlp_raw")
    start_time = time.perf_counter()
    memory_samples = [get_rss_bytes()]
    log_environment()
    log(f"Data file: {DATA_FILE}")

    if not DATA_FILE.exists():
        log(f"Data file not found: {DATA_FILE}")
        sys.exit(1)

    df = pd.read_csv(DATA_FILE)
    df = prepare_dataframe(df)

    numeric_features = [
        "surface_m2",
        "rooms",
        "floor",
        "year_built",
        "number_bathrooms",
        "latitude",
        "longitude",
        "metro_proximity",
        "stb_proximity",
    ]
    categorical_features = ["city", "elevator", "construction_material"]

    df = df[df["price_value"].notna()].reset_index(drop=True)
    if df.shape[0] < 10:
        log("Not enough rows with price to train.")
        sys.exit(1)

    X = df[[c for c in numeric_features + categorical_features if c in df.columns]].copy()
    y = df["price_value"].values

    memory_samples.append(get_rss_bytes())
    log(f"Rows: {len(y)}, Features: {X.shape[1]}")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    memory_samples.append(get_rss_bytes())
    preprocessor = build_preprocessor(
        [c for c in numeric_features if c in X.columns],
        [c for c in categorical_features if c in X.columns],
    )

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
            build_model_pipeline(preprocessor),
            grid_params,
            scoring="neg_mean_absolute_error",
            cv=cv,
            n_jobs=1,
        )
        search.fit(X, y)
        memory_samples.append(get_rss_bytes())
        best_params = normalize_best_params(search.best_params_)
        log(f"Best params: {best_params}")
        log(f"Best CV MAE: {-search.best_score_:.2f}")

        estimator = build_model_pipeline(preprocessor, model_params=best_params)
        evaluate_pipeline(
            f"RAW best params={best_params}",
            estimator,
            X,
            y,
            X_train,
            X_test,
            y_train,
            y_test,
            cv,
            feature_names=X.columns,
            suffix="raw_best",
            save_model_path=None,
            save_artifacts=False,
        )
        memory_samples.append(get_rss_bytes())
        log(f"Elapsed seconds: {time.perf_counter() - start_time:.2f}")
        log(f"Average RSS bytes: {average_bytes(memory_samples)}")
        log(f"Memory: {get_memory_info()}")
        return

    estimator = build_model_pipeline(preprocessor)
    metrics = evaluate_pipeline(
        "RAW",
        estimator,
        X,
        y,
        X_train,
        X_test,
        y_train,
        y_test,
        cv,
        feature_names=X.columns,
        suffix="raw",
        save_model_path=MODEL_FILE,
    )
    memory_samples.append(get_rss_bytes())

    log("\nSummary metrics:")
    log(f"raw: MAE={metrics['mae']:.2f}, RMSE={metrics['rmse']:.2f}, R2={metrics['r2']:.3f}, RMSLE={metrics['rmsle']:.3f}")
    log(f"Elapsed seconds: {time.perf_counter() - start_time:.2f}")
    log(f"Average RSS bytes: {average_bytes(memory_samples)}")
    log(f"Memory: {get_memory_info()}")


if __name__ == "__main__":
    main()
