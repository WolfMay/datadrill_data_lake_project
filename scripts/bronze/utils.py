def validate_post_load(df, validation_rules):
    def validate_cell_type(series, expected_type):
        # Checks if all non-null values in a Series are of a specific type.
        is_valid = series.dropna().apply(lambda x: isinstance(x, expected_type)).all()
        
        return is_valid

    # Perform validation
    for column, expected_type in validation_rules.items():
        if column in df.columns:
            if not validate_cell_type(df[column], expected_type):
                print(f'Column {column} contains invalid types.')
                # Find the invalid rows for a more detailed report
                invalid_rows = df[~df[column].dropna().apply(lambda x: isinstance(x, expected_type))]
                print(invalid_rows)