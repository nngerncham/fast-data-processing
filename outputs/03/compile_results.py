import pandas as pd
import matplotlib.pyplot as plt

# Load the data from the two CSV files
df = pd.DataFrame(pd.read_csv("run_times.csv"))

# Concatenate the two DataFrames into a single DataFrame
df = df.groupby("func_type")["run_time"].mean()

pandas_v_polars_df = df.loc[["pandas", "polars"]]

# Create a box plot to visualize the run times
plt.figure(figsize=(5, 3))
pandas_v_polars_df.plot(kind="bar")
plt.title("Time spent aggregating")
plt.xlabel("Function Type")
plt.ylabel("Run time (seconds)")
plt.suptitle("")
plt.tight_layout()

# Save the plot to a file
file_name = "steam_review_builtin_v_custom.jpg"
plt.savefig(f"../../slides/assets/results/{file_name}")
print(f"Box plot successfully generated and saved as {file_name}")
