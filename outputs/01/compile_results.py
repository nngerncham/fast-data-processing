import pandas as pd
import matplotlib.pyplot as plt

# Load the data from the two CSV files
python_df = pd.DataFrame(pd.read_csv("python_run_times.csv"))
rust_df = pd.DataFrame(pd.read_csv("rust_run_times.csv"))

# Add a 'language' column to each DataFrame to distinguish between the two
python_df["language"] = "Python"
rust_df["language"] = "Rust"

# Concatenate the two DataFrames into a single DataFrame
combined_df = (
    pd.concat([python_df, rust_df]).groupby("language")["run_time_seconds"].mean()
)

# Create a box plot to visualize the run times
plt.figure(figsize=(5, 3))
combined_df.plot(kind="bar")
plt.title("Compiling Enron email files into a single CSV")
plt.xlabel("Language")
plt.ylabel("Run time (seconds)")
plt.suptitle("")
plt.tight_layout()

# Save the plot to a file
file_name = "enron_compilation_time.jpg"
plt.savefig(f"../../slides/assets/results/{file_name}")
print(f"Box plot successfully generated and saved as {file_name}")
