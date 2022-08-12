import pandas as pd
import pytest

from my_python_utils.data.pandas_utils import make_fillna_dict_from_pandas_schema


@pytest.fixture
def df():
    data = [
        (1., 5, "str", False),
        (6.6, 321, None, True),
        (1241, 131, " ", False),
        (None, None, "", None)
    ]

    df = pd.DataFrame(data, columns=["col1", "col2", "col3", "col4"])
    return df


def test_make_fillna_dict_from_pandas_schema():
    data_schema = {
        "col1": float,
        "col2": int,
        "col3": str,
        "col4": bool
    }

    na_by_dtype = {str: "", bool: False, int: 0}
    test_fillna_values = make_fillna_dict_from_pandas_schema(data_schema, na_by_dtype)
    true_fillna_values = {"col2": 0, "col3": "", "col4": False}


    assert test_fillna_values == true_fillna_values


def test_fillna_df(df: pd.DataFrame):
    data_schema = {
        "col1": float,
        "col2": int,
        "col3": str,
        "col4": bool
    }

    na_by_dtype = {str: "", bool: False, int: 0}
    fillna_values = make_fillna_dict_from_pandas_schema(data_schema, na_by_dtype)
    data = [
        (1., 5, "str", False),
        (6.6, 321, "", True),
        (1241, 131, " ", False),
        (None, 0, "", False)
    ]
    df_test = df.fillna(fillna_values).astype(data_schema)
    df_true = pd.DataFrame(data, columns=["col1", "col2", "col3", "col4"])

    assert df_test.equals(df_true)