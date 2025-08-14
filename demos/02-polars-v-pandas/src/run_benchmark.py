import csv
import datetime
import time
from typing import Callable

from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame

from pandas_version import process_once_pandas
from polars_version import process_once_polars, process_once_polars_df


def run_once(
    raw_file_path: str,
    output_file_path: str,
    run_id: int,
    processsor: Callable[[str], PandasDataFrame | PolarsDataFrame],
) -> float:
    print(f"Run ID: {run_id}")
    start_time = time.time()

    processed_data = processsor(raw_file_path)
    if isinstance(processed_data, PandasDataFrame):
        processed_data.to_csv(output_file_path)
    else:
        processed_data.write_csv(output_file_path)

    end_time = time.time()
    return end_time - start_time


if __name__ == "__main__":
    N_RUNS = 50

    input_file = "../../dataset/steam-reviews/steam_reviews.csv"
    with open("../../outputs/02/run_times.csv", "w") as csv_fp:
        csv_writer = csv.writer(csv_fp)
        csv_writer.writerow(["library", "run_idx", "run_time"])

        print(datetime.datetime.now().isoformat())
        print("Running Polars benchmark")
        polars_times = [
            (
                "polars",
                i + 1,
                run_once(
                    input_file,
                    "../../outputs/02/polars_results.csv",
                    i + 1,
                    process_once_polars,
                ),
            )
            for i in range(N_RUNS)
        ]
        csv_writer.writerows(polars_times)

        print(datetime.datetime.now().isoformat())
        print("Running Polars (eager) benchmark")
        polars_times = [
            (
                "polars_eager",
                i + 1,
                run_once(
                    input_file,
                    "../../outputs/02/polars_eager_results.csv",
                    i + 1,
                    process_once_polars_df,
                ),
            )
            for i in range(N_RUNS)
        ]
        csv_writer.writerows(polars_times)

        print(datetime.datetime.now().isoformat())
        print("Running Pandas benchmark")
        pandas_times = [
            (
                "pandas",
                i + 1,
                run_once(
                    input_file,
                    "../../outputs/02/pandas_results.csv",
                    i + 1,
                    process_once_pandas,
                ),
            )
            for i in range(N_RUNS)
        ]
        csv_writer.writerows(pandas_times)
