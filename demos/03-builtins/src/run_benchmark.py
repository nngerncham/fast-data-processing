import csv
import datetime
import time
from typing import Callable

from polars import DataFrame as PolarsDataFrame

from polars_version import process_once_polars, process_once_polars_df


def run_once(
    raw_file_path: str,
    output_file_path: str,
    run_id: int,
    processsor: Callable[[str], PolarsDataFrame],
) -> float:
    print(f"Run ID: {run_id}")
    start_time = time.time()

    processed_data = processsor(raw_file_path)
    processed_data.write_csv(output_file_path)

    end_time = time.time()
    return end_time - start_time


if __name__ == "__main__":
    N_RUNS = 20

    input_file = "../../dataset/steam-reviews/steam_reviews.csv"
    with open("../../outputs/03/run_times.csv", "w") as csv_fp:
        csv_writer = csv.writer(csv_fp)
        csv_writer.writerow(["func_type", "run_idx", "run_time"])

        print(datetime.datetime.now().isoformat())
        print("Running built-in benchmark")
        polars_times = [
            (
                "builtin",
                i + 1,
                run_once(
                    input_file,
                    "../../outputs/03/builtin_results.csv",
                    i + 1,
                    process_once_polars,
                ),
            )
            for i in range(N_RUNS)
        ]
        csv_writer.writerows(polars_times)

        print(datetime.datetime.now().isoformat())
        print("Running custom benchmark")
        polars_times = [
            (
                "custom",
                i + 1,
                run_once(
                    input_file,
                    "../../outputs/03/custom_results.csv",
                    i + 1,
                    process_once_polars_df,
                ),
            )
            for i in range(N_RUNS)
        ]
        csv_writer.writerows(polars_times)
