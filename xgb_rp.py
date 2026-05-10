from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.random_projection import GaussianRandomProjection
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from xgboost import XGBRegressor

BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "imobiliare_apartments.csv"
MODEL_RP_FILE = BASE_DIR / "xgb_pipeline_rp.joblib"
PLOT_DIR = BASE_DIR

USE_RP = True
RP_COMPONENTS = 50
RP_RANDOM_STATE = 42

HYPERPARAM_SEARCH = True
HYPERPARAM_GRID = [
	{"n_estimators": 200, "learning_rate": 0.1, "max_depth": 6},
	{"n_estimators": 300, "learning_rate": 0.1, "max_depth": 4},
	{"n_estimators": 400, "learning_rate": 0.05, "max_depth": 6},
	{"n_estimators": 150, "learning_rate": 0.1, "max_depth": 8},
	{"n_estimators": 500, "learning_rate": 0.05, "max_depth": 4},
	{"n_estimators": 800, "learning_rate": 0.03, "max_depth": 6},
	{"n_estimators": 300, "learning_rate": 0.2, "max_depth": 3},
	{"n_estimators": 200, "learning_rate": 0.05, "max_depth": 8},
	{"n_estimators": 600, "learning_rate": 0.03, "max_depth": 4},
]


def extract_first_number(value):
	if pd.isna(value):
		return None
	m = re.search(r"-?\d+(?:\.\d+)?", str(value))
	return float(m.group(0)) if m else None


def parse_price(value):
	if pd.isna(value):
		return None
	text = str(value).replace("\xa0", " ").lower()
	nums = re.findall(r"\d+(?:[.,]\d+)*", text)
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
		parts = raw.split(",")
		if len(parts[1]) == 3:
			raw = "".join(parts)
		else:
			raw = raw.replace(",", ".")
	elif raw.count(".") == 1 and raw.count(",") == 0:
		parts = raw.split(".")
		if len(parts[1]) == 3:
			raw = "".join(parts)

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
		df["floor"] = df["floor"].apply(lambda v: extract_first_number(v))

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

	preprocess = ColumnTransformer(
		transformers=[
			("num", numeric_transformer, numeric_features),
			("cat", categorical_transformer, categorical_features),
		],
		remainder="drop",
	)

	return preprocess


def build_model_pipeline(preprocessor, use_rp=False, model_params=None):
	params = {"n_estimators": 200, "learning_rate": 0.1, "max_depth": 6, "random_state": 42, "n_jobs": -1}
	if model_params:
		params.update(model_params)
	model = XGBRegressor(**params)

	steps = [("preprocess", preprocessor)]
	if use_rp:
		steps.append(("rp", GaussianRandomProjection(n_components=RP_COMPONENTS, random_state=RP_RANDOM_STATE)))
	steps.append(("model", model))

	return Pipeline(steps=steps)


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


def save_feature_importances(pipeline, use_rp=False, suffix=""):
	try:
		if use_rp:
			n_components = pipeline.named_steps["rp"].n_components
			feature_names = [f"rp_{i + 1}" for i in range(int(n_components))]
		else:
			feature_names = pipeline.named_steps["preprocess"].get_feature_names_out()
	except Exception:
		feature_names = [f"f_{i}" for i in range(pipeline.named_steps["model"].feature_importances_.shape[0])]

	importances = pipeline.named_steps["model"].feature_importances_
	fi = pd.DataFrame({"feature": feature_names, "importance": importances})
	fi = fi.sort_values("importance", ascending=False).head(20)

	plt.figure(figsize=(8, 6))
	sns.barplot(data=fi, x="importance", y="feature")
	plt.title("Top Feature Importances" if not use_rp else "Top RP Component Importances")
	plt.tight_layout()
	suffix = f"_{suffix}" if suffix else ""
	plt.savefig(PLOT_DIR / f"feature_importances{suffix}.png", dpi=150)
	plt.close()

	return f"feature_importances{suffix}.png"


def evaluate_pipeline(name, pipeline, X, y, X_train, X_test, y_train, y_test, cv, suffix="", save_model_path=None, use_rp=False, save_artifacts=True):
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
		save_feature_importances(pipeline, use_rp=use_rp, suffix=suffix)

		if save_model_path:
			joblib.dump(pipeline, save_model_path)
			print(f"Saved trained pipeline to: {save_model_path}")

	return {"mae": mae, "rmse": rmse, "r2": r2, "rmsle": rmsle}


def main():
	if not DATA_FILE.exists():
		print(f"Data file not found: {DATA_FILE}")
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
		print("Not enough rows with price to train.")
		sys.exit(1)

	X = df[[c for c in numeric_features + categorical_features if c in df.columns]].copy()
	y = df["price_value"].values

	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

	preprocessor = build_preprocessor(
		[c for c in numeric_features if c in X.columns],
		[c for c in categorical_features if c in X.columns],
	)

	cv = KFold(n_splits=5, shuffle=True, random_state=42)
	sns.set_theme(style="whitegrid")

	if not USE_RP:
		print("Enable RP to run the model")
		return

	if HYPERPARAM_SEARCH:
		results = []
		total = len(HYPERPARAM_GRID)
		for idx, params in enumerate(HYPERPARAM_GRID, start=1):
			rp_pipeline = build_model_pipeline(preprocessor, use_rp=True, model_params=params)
			metrics = evaluate_pipeline(
				f"RP search {idx}/{total} params={params}",
				rp_pipeline,
				X,
				y,
				X_train,
				X_test,
				y_train,
				y_test,
				cv,
				suffix=f"rp_search_{idx}",
				save_model_path=None,
				use_rp=True,
				save_artifacts=False,
			)
			results.append({"params": params, **metrics})

		results = sorted(results, key=lambda r: r["mae"])
		print("\nSearch summary (sorted by MAE):")
		for r in results:
			print(
				f"MAE={r['mae']:.2f}, RMSE={r['rmse']:.2f}, R2={r['r2']:.3f}, RMSLE={r['rmsle']:.3f} | params={r['params']}"
			)
		return

	rp_pipeline = build_model_pipeline(preprocessor, use_rp=True)
	metrics = evaluate_pipeline(
		f"RP (n_components={RP_COMPONENTS})",
		rp_pipeline,
		X,
		y,
		X_train,
		X_test,
		y_train,
		y_test,
		cv,
		suffix="rp",
		save_model_path=MODEL_RP_FILE,
		use_rp=True,
	)

	print("\nSummary metrics:")
	print(f"rp: MAE={metrics['mae']:.2f}, RMSE={metrics['rmse']:.2f}, R2={metrics['r2']:.3f}, RMSLE={metrics['rmsle']:.3f}")


if __name__ == "__main__":
	main()
