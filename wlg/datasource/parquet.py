# wlg/datasource/parquet.py
import pandas as pd
import pyarrow.dataset as ds
from typing import Iterable, Optional, Dict, List

class ParquetDataSource:
    def __init__(self, path: str, sample_rows: Optional[int] = None):
        self._dataset = ds.dataset(path, format="parquet")
        self._sample_rows = sample_rows
        # NEW: date parsing config
        self._date_parse_explicit: List[str] = []
        self._date_parse_infer: bool = True
        self._cached_schema: Optional[Dict[str, str]] = None

    # NEW: called by CLI
    def configure_date_parsing(self, explicit: List[str], infer: bool):
        self._date_parse_explicit = explicit or []
        self._date_parse_infer = infer

    def schema(self) -> Dict[str, str]:
        """Logical schema after parsing (datetime64 -> 'date')."""
        if self._cached_schema is None:
            for batch in self.scan_batches():
                dtypes = batch.dtypes
                out = {}
                for col, dtype in dtypes.items():
                    if pd.api.types.is_datetime64_any_dtype(dtype):
                        out[col] = "date"
                    elif pd.api.types.is_integer_dtype(dtype):
                        out[col] = "int64"
                    elif pd.api.types.is_float_dtype(dtype):
                        out[col] = "double"
                    else:
                        out[col] = "string"
                self._cached_schema = out
                break
            if self._cached_schema is None:
                # fallback when no rows
                self._cached_schema = {f.name: str(f.type) for f in self._dataset.schema}
        return self._cached_schema

    def scan_batches(self) -> Iterable[pd.DataFrame]:
        """Yield a single DataFrame batch; parse date columns per config."""
        table = self._dataset.to_table()
        df = table.to_pandas()

        # sampling
        if self._sample_rows is not None and len(df) > self._sample_rows:
            df = df.sample(self._sample_rows, random_state=0).reset_index(drop=True)

        # --- date parsing ---
        # 1) explicit columns
        for c in self._date_parse_explicit:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.normalize()

        # 2) optional inference for object-like columns (≥90% parse success)
        if self._date_parse_infer:
            obj_like = [c for c in df.columns
                        if not pd.api.types.is_datetime64_any_dtype(df[c].dtype)
                        and df[c].dtype == object]
            for c in obj_like:
                s = pd.to_datetime(df[c], errors="coerce", infer_datetime_format=True)
                if len(s) and (s.notna().mean() >= 0.90):
                    df[c] = s.dt.normalize()

        yield df
