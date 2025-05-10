import pandas as pd

# Update paths if needed
file1 = "app/Redfin/Output/redfin_data_1000s.csv"
file2 = "app/Redfin/Output/redfin_data.csv"

# Load both files
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Normalize column names by stripping whitespace
df1.columns = df1.columns.str.strip()
df2.columns = df2.columns.str.strip()

# Compare column names
columns1 = set(df1.columns)
columns2 = set(df2.columns)

print("== Column name differences ==")
print("Only in file1:", columns1 - columns2)
print("Only in file2:", columns2 - columns1)

# Compare column types for common columns
print("\n== Column type differences ==")
common_cols = columns1 & columns2
for col in sorted(common_cols):
    dtype1 = df1[col].dtype
    dtype2 = df2[col].dtype
    if dtype1 != dtype2:
        print(f"{col}: file1={dtype1}, file2={dtype2}")
