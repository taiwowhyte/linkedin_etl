
def validate_table(df_new, wanted_columns, VAR_CHAR_LIMITS, natural_key_columns):
        """
        Validate a transformed DataFrame before writing it downstream

        Purpose:
        - Check for schema mismatch. Required columns are present
        - Key integrity: no duplicates / nulls / empty strings in natural keys
        (Natural keys are how you identify unique records without surrogate IDs.)
        - Check size constraints: max string lengths do not exceed configured VARCHAR limits.
        (Useful if later put into SQL / dashboards with fixed-width fields)

        Args:
        :param df_new: DataFrame to validate.
        :param wanted_columns: Columns that must be present for dataset
        :param VAR_CHAR_LIMITS: Dictionary of {column_name: max_length}.
        :param natural_key_columns: Column(s) that define uniqueness for the table
        """        
        missing = set(wanted_columns) - set(df_new.columns)
        if missing:
            raise KeyError(f'column mismatch. Missing: {missing}')

        #look for duplicates in job_link as that is the determiner
        dup_total = df_new.duplicated(subset=natural_key_columns).sum()
        na_total = df_new[natural_key_columns].isna().sum()
        empty_string_total = (df_new[natural_key_columns] == '').sum()

        if dup_total > 0:
            raise ValueError(f'The {natural_key_columns} column(s) contains {dup_total} duplicates')
        
        if (na_total > 0).any():
            raise ValueError(f"NAs total by column: {na_total}")
        
        if (empty_string_total > 0).any():
            raise ValueError(f'Empty strings total by column: {empty_string_total}')

        #Schema sizing check to prevent MySQL truncation errors
        violations = {}

        for col, limit in VAR_CHAR_LIMITS.items():
            if col in wanted_columns:
                observed_max = df_new[col].astype('string').str.len().dropna().max()
                if observed_max is None:
                    observed_max = 0
                if observed_max > limit:

                    violations.update({col: {'max_len': observed_max, 'limit': limit}})
        #skip if column not in VAR_CHAR_LIMITS. Do not raise an error as it is just the dictionary is broader than the table
            else:
                continue
        if violations:
            raise ValueError(f'MySQL VARCHAR length limits exceeded: {violations}')
