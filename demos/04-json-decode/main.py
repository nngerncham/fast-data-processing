import json
import random
import polars as pl
import time


def generate_big_json_data(num_records):
    data = []
    for i in range(num_records):
        record = {
            "id": i,
            "name": f"user_{i}",
            "is_active": random.choice([True, False]),
            "score": random.uniform(0, 1000),
            "attributes": {
                "last_login": f"2023-01-01T{random.randint(0, 23)}:{random.randint(0, 59)}:{random.randint(0, 59)}Z",
                "preferences": [
                    {"key": "theme", "value": "dark"},
                    {"key": "language", "value": "en-us"},
                ],
                "tags": [random.choice(["a", "b", "c"]) for _ in range(3)],
            },
        }
        data.append(json.dumps(record))
    return data


num_records = 10_000_000
json_strings = generate_big_json_data(num_records)
json_schema = pl.Struct(
    [
        pl.Field("id", pl.Int64),
        pl.Field("name", pl.Utf8),
        pl.Field("is_active", pl.Boolean),
        pl.Field("score", pl.Float64),
        pl.Field(
            "attributes",
            pl.Struct(
                [
                    pl.Field("last_login", pl.Utf8),
                    pl.Field(
                        "preferences",
                        pl.List(
                            pl.Struct(
                                [
                                    pl.Field("key", pl.Utf8),
                                    pl.Field("value", pl.Utf8),
                                ]
                            )
                        ),
                    ),
                    pl.Field("tags", pl.List(pl.Utf8)),
                ]
            ),
        ),
    ]
)

print(
    f"Generated a list of {num_records} JSON strings. Total size is approximately:",
    sum(len(s) for s in json_strings),
    "bytes.\n",
)

try:
    # Time Polars' native `str.json_decode`
    start_time_pl_native = time.time()
    df_native = (
        pl.DataFrame({"json_data": json_strings})
        .lazy()
        .with_columns(pl.col("json_data").str.json_decode(dtype=json_schema))
        .collect()
    )
    end_time_pl_native = time.time()

    print(
        f"Decoded {num_records} JSON records using Polars' `str.json_decode` in {end_time_pl_native - start_time_pl_native:.6f} seconds.\n"
    )

    # Time Polars' `map_elements` with `json.loads`
    start_time_pl_map = time.time()
    df_map = (
        pl.DataFrame({"json_data": json_strings})
        .lazy()
        .with_columns(
            pl.col("json_data").map_elements(json.loads, return_dtype=json_schema)
        )
        .collect()
    )
    end_time_pl_map = time.time()

    print(
        f"Decoded {num_records} JSON records using Polars' `map_elements` with `json.loads` in {end_time_pl_map - start_time_pl_map:.6f} seconds.\n"
    )

except Exception as e:
    print(f"An error occurred with Polars decoding: {e}")
    print(
        "This might happen if the JSON structure is too complex for Polars' default schema inference. For production code, you might need to specify the schema."
    )
