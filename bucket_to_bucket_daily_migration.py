from influxdb_client import InfluxDBClient
from influxdb_client import Point
import calendar
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta


source_influx2_url = 'http://192.168.1.11:8087'
source_influx2_org = 'iam'
source_influx2_username = 'root'
source_influx2_password = 'rootrootroot'
source_influx2_bucket = 'lodha-supremus'
source_influx2_token = 'Vu1c6LfOvuVy941gcSYAQP4MaxgqeGg8utt731i3ZtOwqdGFNJQtTgBbWiiPQgt-TBYphj9x_LjgPEW9m6fUog=='


dest_influx2_url = 'http://192.168.1.11:8087'
dest_influx2_org = 'iam'
dest_influx2_username = 'root'
dest_influx2_password = 'rootrootroot'
dest_influx2_bucket = 'data'
dest_influx2_token = 'Vu1c6LfOvuVy941gcSYAQP4MaxgqeGg8utt731i3ZtOwqdGFNJQtTgBbWiiPQgt-TBYphj9x_LjgPEW9m6fUog=='

measurements_to_migrate = ['co2', 'current', 'energy',  'frequency', 'humidity', 'misc',  'power', 'speed', 'temperature', 'voltage']
# measurements_to_migrate = ['energy']


def calculate_dates(year, month, week):
    try:
        first_day_of_month = datetime(year, month, 1)
        last_day_of_month = datetime(year, month, calendar.monthrange(year, month)[1])

        # Calculate the start and end of the week
        start_of_week = first_day_of_month + timedelta(days=(week - 1) * 1)
        end_of_week = min(first_day_of_month + timedelta(days=week * 1 - 1, hours=23, minutes=59, seconds=59), last_day_of_month + timedelta(hours=23, minutes=59, seconds=59))

        return start_of_week, end_of_week
    except Exception as e:
        print(f"Error in calculate_dates: {e}")


def get_data_from_influx2(source_client, measurement, start_time, end_time):
    try:
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        query = f'from(bucket: "{source_influx2_bucket}") |> range(start: {start_time_str}, stop: {end_time_str}) |> filter(fn: (r) => r["_measurement"] == "{measurement}")'
        result = source_client.query_api().query(org=source_influx2_org, query=query)
        return result
    except Exception as e:
        print(f"Error retrieving data for measurement '{measurement}' from InfluxDB 2.0. Error: {str(e)}")
        return []


def write_data_to_influx2(dest_client, measurement, data):
    try:
        write_api = dest_client.write_api(write_options=SYNCHRONOUS)
        dest_client.ready()
        print(f"Connected to InfluxDB 2.0 for measurement '{measurement}' successfully.")

        influx2_data = []

        for table in data:
            for record in table.records:
                try:
                    point = record.values
                    time = point['_time']

                    influx2_point = Point(measurement).time(time)
                    influx2_point.tag('panel_no', point['panel_no'])
                    influx2_point.tag('device_code', point['device_code'])
                    influx2_point.tag('alarm_name', record['alarm_name'])
                    influx2_point.tag('device_name', record['device_name'])
                    influx2_point.tag('max_alarm', record['max_alarm'])
                    influx2_point.tag('min_alarm', record['min_alarm'])
                    influx2_point.tag('name', record['name'])
                    influx2_point.tag('unique_tag', record['unique_tag'])
                    influx2_point.tag('zone', record['zone'])
                    influx2_point.field('value', record['_value'])
                    influx2_data.append(influx2_point)

                except Exception as e:
                    print(f"Error processing data point for measurement '{measurement}'. Error: {str(e)}")
                    print(f"Point data: {point}")

        write_api.write(dest_influx2_bucket, dest_influx2_org, influx2_data)
        print(f"Migration of measurement '{measurement}' successful.")
    except Exception as e:
        print(f"Error connecting to InfluxDB 2.0 for measurement '{measurement}'. Error: {str(e)}")


if __name__ == "__main__":

    source_client = InfluxDBClient(url=source_influx2_url, token=source_influx2_token, org=source_influx2_org,
                                   username=source_influx2_username, password=source_influx2_password,
                                   bucket=source_influx2_bucket)

    dest_client = InfluxDBClient(url=dest_influx2_url, token=dest_influx2_token, org=dest_influx2_org,
                                 username=dest_influx2_username, password=dest_influx2_password,
                                 bucket=dest_influx2_bucket)

    try:
        year = datetime.now().year
        month_list = range(1, 13)
        for measurement in measurements_to_migrate:
            for month in month_list:
                for week in range(1, 32):
                    start_time, end_time = calculate_dates(year, month, week)

                    print(
                        f"[FETCHING DATA 2.0] || FROM TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S')}  |  END TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

                    data_from_influx2 = get_data_from_influx2(source_client, measurement, start_time, end_time)
                    if data_from_influx2 is not None:
                        write_data_to_influx2(dest_client, measurement, data_from_influx2)
                        print("Data transfer completed.")
                    else:
                        print("None data detected")
                        break

    except Exception as e:
        print(e)
    finally:
        source_client.close()
        dest_client.close()
