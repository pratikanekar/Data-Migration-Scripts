import calendar
from datetime import datetime, timedelta

# Decrementing Datetime
end_time = datetime.now()

# Set the variable for the desired month (e.g., "sep")
desired_month = ("june")

# Get the current year
current_year = end_time.year

# Get the month number for the desired month
month_number = list(calendar.month_abbr).index(desired_month[:3].capitalize())

# Calculate the first day of the month
first_day_of_month = datetime(current_year, month_number, 1, 0, 0, 0)

# Calculate the last day of the month
_, last_day_of_month = calendar.monthrange(current_year, month_number)
last_day_of_month = datetime(current_year, month_number, last_day_of_month, 23, 59, 59)

# Adjust start_time and end_time accordingly
start_time = first_day_of_month
end_time = last_day_of_month

print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
