import pandas as pd

def replace_unknown_placeholders(df):
    """
    Standardise common unknown/placeholder values to pandas NA.

    Purpose:
    - Raw datasets often encode missing values inconsistently
      (e.g. '', 'n/a', '-', 'unknown').
    - Normalising these early simplifies downstream cleaning, filtering,
      and null handling in analytics (and Athena/SQL later on).
    """
    unknown_placeholders = ['','n/a','na','none','nan','-','?','unknown']

    df = df.replace(unknown_placeholders, pd.NA)

    return df
