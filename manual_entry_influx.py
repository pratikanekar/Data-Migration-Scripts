from influxdb_client import InfluxDBClient
from influxdb_client import Point
import calendar
import pytz
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta, tzinfo

# # Destination InfluxDB 2.0 connection settings
# dest_influx2_url = 'http://10.11.12.35:8086'
# dest_influx2_org = 'iam'
# dest_influx2_username = 'root'
# dest_influx2_password = 'rootrootroot'
# dest_influx2_bucket = 'data'
# dest_influx2_token = '1013_Qx0-F__xgNuENCGC1sWwdACgFWjJ6Esjon79vk971ajwxb231oVEgm4hY7_smC_fqCNDfYDNz8qdyE9iQ=='


# Destination InfluxDB 2.0 connection settings
dest_influx2_url = 'http://10.129.2.23:8086'
dest_influx2_org = 'iam'
dest_influx2_username = 'root'
dest_influx2_password = 'rootrootroot'
dest_influx2_bucket = 'mgr_last_final_test'
dest_influx2_token = '9rcqoz4onukDdYJsWqauL5i_-VqA0QZl4H_71zgaUJKwkUmZqlzUm3AbwYhR1kVy-2_wWxihEWeAxV4WZVcYJg=='



desired_datetime = datetime(2023, 11, 1, 12, 59, 59)

# Convert the desired datetime to UTC
desired_datetime_utc = desired_datetime.replace(tzinfo=pytz.utc)

# Format the time in the required string format
time = desired_datetime_utc.strftime('%Y-%m-%dT%H:%M:%SZ')

def write_data_to_influx2(dest_client, data):
    try:
        write_api = dest_client.write_api(write_options=SYNCHRONOUS)
        dest_client.ready()  # Check if the connection is successful
        write_api.write(dest_influx2_bucket, dest_influx2_org, data)
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        dest_client.close()


if __name__ == "__main__":

    data = [{
  "measurement": "energy",
  "tags": {
    'panel_no': "iam-gw-210104",
    'device_code': "MOD02",
    'device_name': "Capacitor(Office-3)",
    'zone': "KWHEXP",
    'min_alarm': "0.0",
    'max_alarm': "0.0",
    'name': "Active Energy Export (Into Load)",
    'alarm_name': "NA",
    'unique_tag': "MOD02-KWHEXP"
  },
  "time": time,
  "fields": {
    'value': 873.58
  }
},
{
  "measurement": "energy",
  "tags": {
    'panel_no': "iam-gw-210104",
    'device_code': "MOD03",
    'device_name': "UPS(Office-4)",
    'zone': "KWHEXP",
    'min_alarm': "0.0",
    'max_alarm': "0.0",
    'name': "Active Energy Export (Into Load)",
    'alarm_name': "NA",
    'unique_tag': "MOD03-KWHEXP"
  },
  "time": time,
  "fields": {
    'value': 61472.09
  }
},
{
  "measurement": "energy",
  "tags": {
    'panel_no': "iam-gw-210104",
    'device_code': "MOD04",
    'device_name': "Lighting(office-1)",
    'zone': "KWHEXP",
    'min_alarm': "0.0",
    'max_alarm': "0.0",
    'name': "Active Energy Export (Into Load)",
    'alarm_name': "NA",
    'unique_tag': "MOD04-KWHEXP"
  },
  "time": time,
  "fields": {
    'value': 74036.91
  }
},
{
  "measurement": "energy",
  "tags": {
    'panel_no': "iam-gw-210104",
    'device_code': "MOD05",
    'device_name': "Power(office-2)",
    'zone': "KWHEXP",
    'min_alarm': "0.0",
    'max_alarm': "0.0",
    'name': "Active Energy Export (Into Load)",
    'alarm_name': "NA",
    'unique_tag': "MOD05-KWHEXP"
  },
  "time": time,
  "fields": {
    'value': 35930.41
  }
},
{
  "measurement": "energy",
  "tags": {
    'panel_no': "iam-gw-210104",
    'device_code': "MOD06",
    'device_name': "HVAC",
    'zone': "KWHEXP",
    'min_alarm': "0.0",
    'max_alarm': "0.0",
    'name': "Active Energy Export (Into Load)",
    'alarm_name': "NA",
    'unique_tag': "MOD06-KWHEXP"
  },
  "time": time,
  "fields": {
    'value': 102177.96
  }
}



          ]
    # Set up InfluxDB 2.0 destination client
    dest_client = InfluxDBClient(url=dest_influx2_url, token=dest_influx2_token, org=dest_influx2_org,
                                 username=dest_influx2_username, password=dest_influx2_password,
                                 bucket=dest_influx2_bucket)
    write_data_to_influx2(dest_client, data)
    print("Data transfer completed.")

