import pandas as pd
import json
import logging
from typing import Any, Dict, List
import os
import time

logger = logging.getLogger(__name__)

def check_all_statuses_success(check_runs_list: List[Dict[str, Any]]) -> bool:
    if not check_runs_list:
        return True

    return all(run.get('conclusion') == 'success' for run in check_runs_list if run.get('conclusion') is not None)

def save_data_to_parquet(data: pd.DataFrame) -> str:
    os.makedirs("data", exist_ok=True)

    filename = f"transformed_data_{time.time() * 1000:.0f}.parquet"
    filepath = os.path.join("data", filename)

    try:
        data.to_parquet(filepath, index=False)
        print(f"Data saved to {filepath}")

    except AttributeError:
        print("Input data must be a Pandas DataFrame to save as Parquet.")
        raise

    except IOError as e:
        print(f"Failed to save data to {filepath}: {e}")
        raise

    return filepath


def transform_data_logic(input_file_path: str, **kwargs: Any) -> str:
    logger.info(f"מתחיל טרנספורמציה. קורא נתונים מ: {input_file_path}")

    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            data_list = json.load(f)
    except Exception as e:
        logger.error(f"כשל בטעינת קובץ ה-JSON: {e}")
        raise

    df = pd.json_normalize(data_list, sep='.')

    df['code_review_passed'] = df['review_info.approved_count'] >= 1
    df['status_checks_passed'] = df['status_checks.checks'].apply(check_all_statuses_success)
    df['is_compliant'] = df['code_review_passed'] & df['status_checks_passed']

    final_df = df[[
        'metadata.number',
        'metadata.title',
        'metadata.merged_at',
        'metadata.author.login',
        'code_review_passed',
        'status_checks_passed',
        'is_compliant'
    ]].copy()

    final_df.columns = final_df.columns.str.replace('metadata.', '', regex=False)

    logger.info(f"הסתיימה טרנספורמציה. {len(final_df)} רשומות מעובדות.")

    parquet_filepath = save_data_to_parquet(final_df)

    return parquet_filepath