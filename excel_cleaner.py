import pandas as pd
import numpy as np
import re
from datetime import datetime
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCleaningPipeline:
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.errors = []
        self.cleaned_rows = 0
        self.total_rows = 0
        self.logger = logger

    def clean_and_validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main pipeline: Handle missing, types, constraints, outliers."""
        self.total_rows = len(df)
        self.logger.info(f"Starting pipeline with {self.total_rows} rows")
        df = self._handle_missing_values(df)
        df = self._validate_data_types(df)
        df = self._apply_constraints(df)
        df = self._remove_outliers(df)
        self.cleaned_rows = len(df)
        self._generate_report()
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        for column, rules in self.schema.items():
            if column in df.columns:
                if rules.get('required', False):
                    missing_count = df[column].isnull().sum()
                    if missing_count > 0:
                        self.errors.append(f"Removed {missing_count} rows with missing {column}")
                        df = df.dropna(subset=[column])
                else:
                    if df[column].dtype in ['int64', 'float64']:
                        df[column].fillna(df[column].median(), inplace=True)
                    else:
                        df[column].fillna('Unknown', inplace=True)
        return df

    def _validate_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        for column, rules in self.schema.items():
            if column in df.columns:
                expected_type = rules['type']
                try:
                    if expected_type == 'datetime':
                        df[column] = pd.to_datetime(df[column], errors='coerce')
                    elif expected_type == int:
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif expected_type == float:
                        df[column] = pd.to_numeric(df[column], errors='coerce')
                    invalid_count = df[column].isnull().sum()
                    if invalid_count > 0:
                        self.errors.append(f"Removed {invalid_count} rows with invalid {column}")
                        df = df.dropna(subset=[column])
                except Exception as e:
                    self.logger.error(f"Type conversion error for {column}: {e}")
        return df

    def _apply_constraints(self, df: pd.DataFrame) -> pd.DataFrame:
        for column, rules in self.schema.items():
            if column in df.columns:
                initial_count = len(df)
                if 'min_value' in rules:
                    df = df[df[column] >= rules['min_value']]
                if 'max_value' in rules:
                    df = df[df[column] <= rules['max_value']]
                if 'pattern' in rules and df[column].dtype == 'object':
                    pattern = re.compile(rules['pattern'])
                    df = df[df[column].astype(str).str.match(pattern, na=False)]
                removed_count = initial_count - len(df)
                if removed_count > 0:
                    self.errors.append(f"Removed {removed_count} rows failing {column} constraints")
        return df

    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for column in numeric_columns:
            if column in self.schema:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = (df[column] < lower_bound) | (df[column] > upper_bound)
                outlier_count = outliers.sum()
                if outlier_count > 0:
                    df = df[~outliers]
                    self.errors.append(f"Removed {outlier_count} outliers from {column}")
        return df

    def _generate_report(self):
        self.logger.info(f"Pipeline completed: {self.cleaned_rows}/{self.total_rows} rows retained")
        for error in self.errors:
            self.logger.warning(error)
