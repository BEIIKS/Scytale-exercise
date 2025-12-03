import pandas as pd
import os
import logging
from typing import Any

logger = logging.getLogger(__name__)

def load_data_logic(input_filepath: str, output_dir: str, execution_date: str, **kwargs: Any) -> str:
    if not input_filepath or not isinstance(input_filepath, str):
        logger.error(f"לא התקבל נתיב קובץ תקין מ-XCom. התקבל: {input_filepath}")
        raise ValueError("Load failed: Invalid input filepath received.")

    try:
        input_df = pd.read_parquet(input_filepath)
        logger.info(f"נתונים נטענו בהצלחה מנתיב: {input_filepath}")
    except Exception as e:
        logger.error(f"כשל בקריאת קובץ Parquet מ- {input_filepath}: {e}")
        raise

    if input_df.empty:
        logger.warning("קיבל DataFrame ריק. מדלג על שמירה.")
        return "Load Skipped (Empty Data)"

    logger.info(f"מתחיל טעינת {len(input_df)} רשומות לקובץ Parquet.")

    filename = f"compliance_report_{execution_date}.parquet"
    output_path = os.path.join(output_dir, filename)

    os.makedirs(output_dir, exist_ok=True)

    try:
        input_df.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"הנתונים נשמרו בהצלחה כ-Parquet בנתיב: {output_path}")

    except Exception as e:
        logger.error(f"כשל בשמירה ל-Parquet ב- {output_path}: {e}")
        raise

    return output_path