import csv
import os
import time


def parse_raw(raw_file_directory: str) -> list[tuple[str, str]]:
    email_data = []

    for root_path, _, file_names in os.walk(raw_file_directory, topdown=True):
        for file_name in file_names:
            email_path = f"{root_path}/{file_name}"

            with open(email_path, "r", encoding="latin-1") as email_fp:
                email_content = email_fp.read()
                email_data.append((email_path, email_content))

    return email_data


def write_to_csv(parsed_data: list[tuple[str, str]], output_path: str) -> None:
    with open(output_path, "w") as csv_file:
        writer = csv.writer(csv_file, quotechar="|", quoting=csv.QUOTE_ALL)
        writer.writerow(["path", "content"])
        writer.writerows(parsed_data)


def parse_and_write_once(input_path: str, output_path: str):
    parsed_data = parse_raw(input_path)
    write_to_csv(parsed_data, output_path)


def benchmark_once(run_idx: int, input_path: str, output_path: str) -> float:
    print(f"Run number {run_idx + 1}", end=" ")

    start_time = time.time()
    parse_and_write_once(input_path, output_path)
    end_time = time.time()

    run_time = end_time - start_time
    print(f"took {run_time}")
    return run_time


if __name__ == "__main__":
    N_RUNS = 100
    run_times = [
        benchmark_once(
            run_idx,
            "../../../dataset/enron-emails/unzipped_files/maildir/",
            "../../../outputs/01/python_result.csv",
        )
        for run_idx in range(N_RUNS)
    ]

    avg_run_times_all = sum(run_times) / len(run_times)
    avg_run_times_warm_cache = sum(run_times[1:]) / len(run_times[1:])

    with open("../../../outputs/01/python_run_times.csv", "w") as run_times_csv_fp:
        writer = csv.writer(run_times_csv_fp)
        writer.writerow(["run_id", "run_time_seconds"])
        writer.writerows(
            (run_idx + 1, run_time)
            for run_idx, run_time in zip(range(N_RUNS), run_times)
        )

    print(f"Avg: {avg_run_times_all}, warm cache run times: {avg_run_times_warm_cache}")
