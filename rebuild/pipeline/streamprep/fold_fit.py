"""Per-fold feature selection, imputation and scaling (protocol 4.1.4).

Everything is fitted on one fold's training NORMAL windows only: role train and label NORMAL.
The selection steps are the R1 ones (sagemaker/preprocessing.py select_features): drop all-NaN
columns, drop variance below 0.01, then for pairs with |Pearson r| above 0.95 drop the lower
variance column. Imputation is the per-column median and scaling is mean and population standard
deviation, as sklearn SimpleImputer(median) and StandardScaler do in R1 training.py.

Two changes from R1 (Review 4 CAM-4, decision 25), set in config/pipeline.json fold_fit:
1. Protected columns (the TaskManager count, the TaskManager missingness features, the eight derived
   features and the named event counters) are never removed by the variance or the correlation
   filter. They also never cause another column to be removed: the correlation filter runs on the
   unprotected columns only, exactly as in R1. A protected column that is NaN in every train NORMAL
   window cannot be imputed and is still removed, and the report lists it.
2. A column whose train NORMAL values (after imputation) are all equal keeps unit scale and is
   centred on that exact value, so train NORMAL maps to exactly 0 and rounding in the mean can never
   divide by a standard deviation of 1e-17. R1 dropped such columns before scaling.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .contract import BuildError

FEATURE_PREFIX = "f__"


def feature_columns(df: pd.DataFrame, category: str | None = None, exclude_category: str | None = None,
                    exclude_derived: bool = False) -> list[str]:
    cols = sorted(c for c in df.columns if c.startswith(FEATURE_PREFIX))
    if category:
        cols = [c for c in cols if c.startswith(f"f__{category}__")]
    if exclude_category:
        cols = [c for c in cols if not c.startswith(f"f__{exclude_category}__")]
    if exclude_derived:
        cols = [c for c in cols if not c.startswith("f__derived__")]
    return cols


def train_normal_mask(roles: pd.Series, labels: pd.Series) -> np.ndarray:
    return ((roles == "train") & (labels == "NORMAL")).to_numpy()


@dataclass
class FoldTransform:
    variance_threshold: float = 0.01
    correlation_threshold: float = 0.95
    protected_patterns: tuple[str, ...] = ()
    exempt_from: tuple[str, ...] = ("variance", "correlation")
    selected: list[str] = field(default_factory=list)
    medians: np.ndarray | None = None
    means: np.ndarray | None = None
    scales: np.ndarray | None = None
    report: dict = field(default_factory=dict)

    @classmethod
    def from_config(cls, cfg: dict) -> "FoldTransform":
        """Settings from config/pipeline.json fold_fit."""
        ff = cfg["fold_fit"]
        prot = ff.get("protected", {})
        exempt = tuple(prot.get("exempt_from", ()))
        bad = set(exempt) - {"variance", "correlation"}
        if bad:
            raise BuildError(f"fold_fit.protected.exempt_from has unknown filters {sorted(bad)}")
        return cls(float(ff["variance_threshold"]), float(ff["correlation_threshold"]),
                   tuple(p["regex"] for p in prot.get("patterns", ())), exempt)

    def protected_mask(self, cols: list[str]) -> tuple[set[str], list[str]]:
        pats = [re.compile(p) for p in self.protected_patterns]
        prot = {c for c in cols if any(p.search(c) for p in pats)}
        unmatched = [p.pattern for p in pats if not any(p.search(c) for c in cols)]
        return prot, unmatched

    def fit(self, X_train_normal: pd.DataFrame) -> "FoldTransform":
        if len(X_train_normal) < 2:
            raise BuildError("fewer than two training NORMAL windows in this fold")
        cols = list(X_train_normal.columns)
        rep = {"initial_features": len(cols)}
        protected, unmatched = self.protected_mask(cols)
        rep["protected_columns"] = len(protected)
        rep["protected_patterns_unmatched"] = unmatched
        all_nan = [c for c in cols if X_train_normal[c].isna().all()]
        cols = [c for c in cols if c not in set(all_nan)]
        rep["removed_all_nan"] = len(all_nan)
        rep["protected_removed_all_nan"] = sorted(set(all_nan) & protected)
        X = X_train_normal[cols]
        variances = X.var()
        guard_var = protected if "variance" in self.exempt_from else set()
        low = set(variances[variances < self.variance_threshold].index) - guard_var
        keep = [c for c in cols if c not in low]
        rep["removed_low_variance"] = len(low)
        rep["protected_below_variance_threshold"] = int(sum(1 for c in guard_var if c in variances.index
                                                            and not variances[c] >= self.variance_threshold))
        guard_corr = protected if "correlation" in self.exempt_from else set()
        free = [c for c in keep if c not in guard_corr]
        if len(free) > 1:
            corr = X[free].corr().abs()
            upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
            to_drop: set[str] = set()
            for col in upper.columns:
                for hc in upper.index[upper[col] > self.correlation_threshold].tolist():
                    if hc in to_drop:
                        continue
                    if variances.get(col, 0) >= variances.get(hc, 0):
                        to_drop.add(hc)
                    else:
                        to_drop.add(col)
            keep = [c for c in keep if c not in to_drop]
            rep["removed_correlated"] = len(to_drop)
        else:
            rep["removed_correlated"] = 0
        if not keep:
            raise BuildError("feature selection left no column in this fold")
        self.selected = keep
        A = X_train_normal[keep].to_numpy(np.float64)
        rep["nan_cells_imputed_in_train_normal"] = int(np.isnan(A).sum())
        self.medians = np.nanmedian(A, axis=0)
        A = np.where(np.isnan(A), self.medians[None, :], A)
        constant = A.max(axis=0) == A.min(axis=0)
        self.means = np.where(constant, A[0], A.mean(axis=0))
        std = A.std(axis=0)
        self.scales = np.where(~constant & (std > 0), std, 1.0)
        rep["constant_unit_scale"] = int(constant.sum())
        rep["protected_kept"] = int(sum(1 for c in keep if c in protected))
        rep["final_features"] = len(keep)
        self.report = rep
        return self

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        if self.medians is None:
            raise BuildError("FoldTransform.transform called before fit")
        A = X[self.selected].to_numpy(np.float64)
        A = np.where(np.isnan(A), self.medians[None, :], A)
        A = np.nan_to_num(A, nan=0.0, posinf=0.0, neginf=0.0)
        return ((A - self.means[None, :]) / self.scales[None, :]).astype(np.float32)

    def to_dict(self) -> dict:
        return {"selected": self.selected, "report": self.report,
                "variance_threshold": self.variance_threshold,
                "correlation_threshold": self.correlation_threshold,
                "protected_patterns": list(self.protected_patterns), "exempt_from": list(self.exempt_from)}
