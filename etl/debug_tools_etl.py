import logging

def debug_function(df, debug, columns):
    """
    Emit lightweight data-quality diagnostics to logs (optional).

    Purpose:
    - To quickly bring to light things that would case bad data to be loaded
      (unknown placeholders, empty strings, unexpected top values).
    - Putting this function behind a `debug` flag lets one turn it on during local/devlopment stages
      without spamming Airflow logs in normal runs.

    :param df: Dataframe to inspect
    :param debug: If True, logs diagnostics, if False, does nothing
    :param columns: Column name or list of column names to check
    """
    if debug:
    #check for unknown placeholders
        unknown_placeholders = ['','n/a','na','none','nan','-','?','unknown']
        condition = df[columns].isin(unknown_placeholders)
        logging.info('Unknown_placeholders counts per column:\n%s', condition.sum())
    #check what are the most common values are to see if you can see empty strings / unknown placeholders
        for column in df[columns]:
            logging.info('Most common values for %s:\n%s', column, df[column].value_counts().head(20))
    #check for empty strings and NAs present
        for column in df[columns]:
            empty_string_count = (df[column] == '').sum()
            na_count = df[column].isna().sum()
            logging.info('%s empty_string count: %s', column, empty_string_count)
            logging.info('%s NA count: %s', column, na_count)
