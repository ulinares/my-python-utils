def make_fillna_dict_from_pandas_schema(
    data_schema: dict, na_by_dtype: dict
) -> dict:
    fillna_dict = {}

    for col_name, dtype in data_schema.items():
        for dtype_, fillna_value in na_by_dtype.items():
            if dtype == dtype_:
                fillna_dict[col_name] = fillna_value

    return fillna_dict
