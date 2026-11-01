"""
Kafka Configuration Recommendation System
Step 1: Preprocessing Pipeline
"""

import json
import pandas as pd
import numpy as np
import tempfile
from pathlib import Path
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split

# ──────────────────────────────────────────────────────────
# 1. LOAD DATA
# ──────────────────────────────────────────────────────────

def load_data(path: str) -> pd.DataFrame:
    """
    Load from JSON array file.
    Adjust this function if your data is CSV or JSON Lines.
    """
    with open(path, "r") as f:
        records = json.load(f)
    return pd.DataFrame(records)


# ──────────────────────────────────────────────────────────
# 2. CLEAN & TYPE-FIX
# ──────────────────────────────────────────────────────────

def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Fix types that arrive as strings
    df["batchSize"] = df["batchSize"].astype(int)

    return df


# ──────────────────────────────────────────────────────────
# 3. AVERAGE RUNS
# ──────────────────────────────────────────────────────────

CONFIG_COLS = ["batchSize", "lingerMs", "compressionType", "billSize"]

def average_runs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups rows by unique config (batchSize, lingerMs, compressionType, billSize)
    and averages throughput across repeated runs.

    Example:
        config X - run1: 54000 docs/sec
        config X - run2: 57000 docs/sec
        config X - avg : 55500 docs/sec
    """
    before = len(df)
    df_avg = (
        df.groupby(CONFIG_COLS, as_index=False)["throughput"]
        .mean()
    )
    after = len(df_avg)
    runs  = round(before / after, 1)

    print(f"  Rows before averaging : {before}")
    print(f"  Rows after averaging  : {after}")
    print(f"  Avg runs per config   : {runs}")

    return df_avg


# ──────────────────────────────────────────────────────────
# 4. SPLIT FEATURES / TARGETS
# ──────────────────────────────────────────────────────────

NUMERICAL_FEATURES   = ["batchSize", "lingerMs", "billSize"]
CATEGORICAL_FEATURES = ["compressionType"]
FEATURES             = NUMERICAL_FEATURES + CATEGORICAL_FEATURES
TARGETS              = ["throughput"]


def split_xy(df: pd.DataFrame):
    X = df[FEATURES]
    y = df[TARGETS]
    return X, y


# ──────────────────────────────────────────────────────────
# 4. BUILD SKLEARN PREPROCESSOR
# ──────────────────────────────────────────────────────────

def build_preprocessor() -> ColumnTransformer:
    """
    - Numerical columns  → StandardScaler  (zero mean, unit variance)
    - compressionType    → OneHotEncoder   (none, snappy, lz4, gzip)

    Note for GPR: also normalize targets separately (see normalize_targets).
    """
    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERICAL_FEATURES),
            ("cat", OneHotEncoder(
                categories=[["none", "snappy", "lz4", "gzip"]],
                sparse_output=False,
                handle_unknown="ignore"
            ), CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )


def get_feature_names(preprocessor: ColumnTransformer) -> list[str]:
    num_names = NUMERICAL_FEATURES
    cat_names = list(
        preprocessor
        .named_transformers_["cat"]
        .get_feature_names_out(CATEGORICAL_FEATURES)
    )
    return num_names + cat_names


# ──────────────────────────────────────────────────────────
# 5. TARGET NORMALIZATION  (needed for GPR)
# ──────────────────────────────────────────────────────────

class TargetScaler:
    """
    Thin wrapper so we can inverse-transform predictions easily.
    Fits one StandardScaler per target column.
    """
    def __init__(self):
        self.scalers = {t: StandardScaler() for t in TARGETS}

    def fit_transform(self, y: pd.DataFrame) -> np.ndarray:
        cols = []
        for t in TARGETS:
            scaled = self.scalers[t].fit_transform(y[[t]])
            cols.append(scaled)
        return np.hstack(cols)

    def transform(self, y: pd.DataFrame) -> np.ndarray:
        cols = []
        for t in TARGETS:
            scaled = self.scalers[t].transform(y[[t]])
            cols.append(scaled)
        return np.hstack(cols)

    def inverse_transform(self, y_scaled: np.ndarray) -> pd.DataFrame:
        cols = {}
        for i, t in enumerate(TARGETS):
            cols[t] = self.scalers[t].inverse_transform(
                y_scaled[:, i].reshape(-1, 1)
            ).flatten()
        return pd.DataFrame(cols)


# ──────────────────────────────────────────────────────────
# 6. MAIN PIPELINE
# ──────────────────────────────────────────────────────────

def preprocess(path: str, test_size: float = 0.2, random_state: int = 42):
    """
    Full preprocessing pipeline.

    Returns
    -------
    X_train, X_test : np.ndarray  — scaled feature matrices
    y_train, y_test : pd.DataFrame — raw targets  (for RF / XGBoost)
    y_train_s, y_test_s : np.ndarray — scaled targets (for GPR)
    feature_names : list[str]
    preprocessor : fitted ColumnTransformer
    target_scaler : fitted TargetScaler
    """

    # Load & clean
    df = load_data(path)
    df = clean(df)

    # Average repeated runs
    print("Averaging runs...")
    df = average_runs(df)

    # Describe the dataset
    print("=" * 55)
    print(f"Dataset shape    : {df.shape}")
    print(f"\nFeatures used    : {FEATURES}")
    print(f"Targets          : {TARGETS}")
    print(f"\ncompression_type distribution:")
    print(df["compressionType"].value_counts().to_string())
    print(f"\nbatchSize unique : {sorted(df['batchSize'].unique())}")
    print(f"lingerMs range   : {df['lingerMs'].min()} – {df['lingerMs'].max()}")
    print(f"billSize unique  : {sorted(df['billSize'].unique())}")
    print(f"\nThroughput stats :\n{df['throughput'].describe().round(2).to_string()}")
    if "latency" in df.columns:
        print(f"\nLatency stats    :\n{df['latency'].describe().round(2).to_string()}")
    print("=" * 55)

    # Features / targets
    X, y = split_xy(df)

    # Train / test split  (stratify not used — continuous targets)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    # Fit preprocessor on train, apply to both splits
    preprocessor = build_preprocessor()
    X_train_s = preprocessor.fit_transform(X_train)
    X_test_s  = preprocessor.transform(X_test)

    feature_names = get_feature_names(preprocessor)
    print(f"\nFinal feature names ({len(feature_names)}):")
    print(feature_names)
    print(f"\nX_train : {X_train_s.shape}  |  X_test : {X_test_s.shape}")
    print(f"y_train : {y_train.shape}   |  y_test : {y_test.shape}")

    # Target scaling  (for GPR)
    target_scaler = TargetScaler()
    y_train_s = target_scaler.fit_transform(y_train)
    y_test_s  = target_scaler.transform(y_test)

    return (
        X_train_s, X_test_s,
        y_train, y_test,
        y_train_s, y_test_s,
        feature_names,
        preprocessor,
        target_scaler,
    )


# ──────────────────────────────────────────────────────────
# 7. QUICK TEST WITH SYNTHETIC DATA
# ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os

    # Build a tiny synthetic dataset matching your schema
    rng   = np.random.default_rng(0)
    n     = 1216
    linger_vals = list(range(5, 506, 20))          # 5, 25, 45, ..., 505
    batch_vals  = [16384, 32768, 49152, 65536]
    comp_vals   = ["none", "snappy", "lz4", "gzip"]
    bill_vals   = [309000, 618000, 927000]          # ≈ 300K / 600K / 900K

    synthetic = pd.DataFrame({
        "batchSize":       rng.choice(batch_vals,  n).astype(str),
        "lingerMs":        rng.choice(linger_vals, n),
        "compressionType": rng.choice(comp_vals,   n),
        "billSize":        rng.choice(bill_vals,   n),
        "throughput":      rng.uniform(20_000, 100_000, n),
        "latency":         rng.uniform(10, 500, n),
    })

    # Create a temporary file path compatible with all operating systems
    tmp = Path(tempfile.gettempdir()) / "kafka_synthetic.json"

    synthetic.to_json(tmp, orient="records", indent=2)

    print(f"Synthetic dataset written to {tmp}\n")

    results = preprocess(str(tmp))

    results = preprocess(tmp)
    (
        X_train_s, X_test_s,
        y_train, y_test,
        y_train_s, y_test_s,
        feature_names,
        preprocessor,
        target_scaler,
    ) = results

    # Verify inverse-transform round-trips
    y_rt = target_scaler.inverse_transform(y_test_s)
    max_err = (y_rt - y_test.values).abs().max().max()
    print(f"\nMax inverse-transform error : {max_err:.6f}  (should be ~0)")
    print("\nPreprocessing complete. Ready for model training.")
