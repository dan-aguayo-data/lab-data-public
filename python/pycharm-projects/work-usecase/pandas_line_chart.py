import pandas as pd
import matplotlib.pyplot as plt

# File path
file_path = r'C:\Users\DanielAguayo\Downloads\elonmusk.csv'

# Read the CSV file
data = pd.read_csv(file_path, delimiter=',', quotechar='"', on_bad_lines='skip')

# Append the year 2024 to the dates
data['created_at'] = data['created_at'].astype(str) + " 2024"

# Convert 'created_at' to datetime while ignoring unrecognized timezones
data['created_at'] = pd.to_datetime(data['created_at'], format='%b %d, %I:%M:%S %p %Z %Y', errors='coerce')

# Drop rows where 'created_at' is NaT (invalid or blank)
data = data.dropna(subset=['created_at'])

# Retain only the 'created_at' column or any other relevant columns
data = data[['created_at']]  # Adjust this list if other columns are required

# Extract the date (without time)
data['date'] = data['created_at'].dt.date

# Group data by date and count occurrences
daily_counts = data.groupby('date').size()

# Plot the line chart
plt.figure(figsize=(10, 6))
plt.plot(daily_counts.index, daily_counts.values, marker='o', linestyle='-')
plt.title('Count of Entries Per Day')
plt.xlabel('Day')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()

# Show the chart
plt.show()
