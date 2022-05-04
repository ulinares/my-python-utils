import fastavro
import pandas as pd
from numpy import nan

PANDAS_TO_AVRO_DTYPES = {
    "object": "string",
    "float64": "float",
    "int64": "int",
    "float32": "float",
    "int32": "int"
}


def generate_avro_schema(
    df: pd.DataFrame,
    schema_namespace: str,
    schema_name: str,
):
    """
    Generate an avro schema from a Pandas dataframe.

    Params
    ------
    df: pd.DataFrame
        Pandas dataframe to use for the avro schema.

    schema_namespace: str
        Schema namespace.

    schema_name: str
        Schema name.

    Returns
    -------
    avro_schema: dict like
        The avro schema generated from df.
    """
    fields = []
    schema = {
        "namespace": schema_namespace,
        "name": schema_name,
        "type": "record",
        "fields": []
    }

    for col_name, col_dtype in df.dtypes.iteritems():
        avro_dtype = PANDAS_TO_AVRO_DTYPES[str(col_dtype)]
        if df[col_name].hasnans:
            data_type = ["null", avro_dtype]
        else:
            data_type = avro_dtype

        field = {"name": col_name, "type": data_type}
        fields.append(field)

    schema["fields"] = fields

    return fastavro.parse_schema(schema)


def pandas_df_to_avro(df: pd.DataFrame, schema: dict, avro_filename: str, **avro_kwargs):
    """
    Export a Pandas dataframe as avro file.

    Params
    ------
    df: pd.DataFrame
        Pandas dataframe.

    schema: dict like
        Avro schema.

    avro_filename: str
        Name of the output avro file.
    """
    df = df.replace(nan, None)
    records = df.to_dict("records")

    with open(avro_filename, "wb") as f:
        fastavro.writer(f, schema, records, **avro_kwargs)

