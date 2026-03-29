import pyarrow.parquet as pq

file_path = 'D:\\git\\IGTI-PA\\arquivos\\har\\empresas\\empresas.snappy.parquet'


import pandas as pd
import pyarrow.parquet as pq

def read_parquet_schema_df(uri: str) -> pd.DataFrame:
    """Return a Pandas dataframe corresponding to the schema of a local URI of a parquet file."""
    schema = pq.read_schema(uri)
    schema_df = pd.DataFrame({
        "column": schema.names,
        "pa_dtype": [str(dtype) for dtype in schema.types]
    })
    return schema_df

# Example usage:
schema_info = read_parquet_schema_df(file_path)
# Set pandas option to ensure all columns are displayed if there are many
pd.set_option('display.max_columns', None)
print(schema_info)
