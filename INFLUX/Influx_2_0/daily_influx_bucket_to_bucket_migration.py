from influxdb_client import InfluxDBClient
from influxdb_client import Point
import calendar
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from loguru import logger


def calculate_dates(year, month, day):
    """
        In this function we calculate dates for daily basis and return start and end dates
    """
    try:
        first_day_of_month = datetime(year, month, day=1)
        last_day_of_month = datetime(year, month, (calendar.monthrange(year, month)[1]))
        start_date = (first_day_of_month + timedelta(days=(day - 1) * 1))
        end_date = min(first_day_of_month + timedelta(days=day * 1 - 1, hours=23, minutes=59, seconds=59),
                          last_day_of_month + timedelta(hours=23, minutes=59, seconds=59))

        return start_date, end_date
    except Exception as e:
        logger.error(f"Error Occurred in calculate_dates: {e}")


def get_data_from_influx2(source_client, measurement, start_time, end_time):
    """
        In this function we get data from server influx bucket and return to caller variable
    """
    try:
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        query = f'from(bucket: "{source_influx2_bucket}") |> range(start: {start_time_str}, stop: {end_time_str}) |> filter(fn: (r) => r["_measurement"] == "{measurement}")'
        result = source_client.query_api().query(org=source_influx2_org, query=query)
        return result
    except Exception as e:
        logger.error(f"Error retrieving data for measurement '{measurement}' from InfluxDB 2.0. Error: {str(e)}")
        return []


def write_data_to_influx2(dest_client, measurement, data):
    """
        In this function we add data to client influx bucket
    """
    try:
        write_api = dest_client.write_api(write_options=SYNCHRONOUS)
        dest_client.ready()
        logger.debug(f"Connected to InfluxDB 2.0 for measurement '{measurement}' successfully.")

        influx2_data = []

        for table in data:
            for record in table.records:
                point = record.values
                try:
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
                    logger.error(f"Error processing data point for measurement '{measurement}'. Error: {str(e)}")
                    logger.debug(f"Point data: {point}")

        write_api.write(dest_influx2_bucket, dest_influx2_org, influx2_data)
        logger.info(f"Migration of measurement '{measurement}' successful.")
    except Exception as e:
        logger.error(f"Error connecting to InfluxDB 2.0 for measurement '{measurement}'. Error: {str(e)}")


if __name__ == "__main__":

    """
        Here we add the Server influx 2.0 details
    """
    source_influx2_url = 'http://192.168.1.11:8086'
    source_influx2_org = 'iam'
    source_influx2_username = 'root'
    source_influx2_password = 'rootrootroot'
    source_influx2_bucket = 'Test'
    source_influx2_token = 'Vu1c6LfOvuVy941gcOwqdGFNJQtTgBbWiiPQgt-TBYphj9x_LjgPEW9m6fUog=='

    """
        Here we add the Client influx 2.0 details
    """
    dest_influx2_url = 'http://10.129.2.23:8086'
    dest_influx2_org = 'iam'
    dest_influx2_username = 'root'
    dest_influx2_password = 'rootrootroot'
    dest_influx2_bucket = 'data'
    dest_influx2_token = '9rcqoz4onukDdzgaUJKwkUmZqlzUm3AbwYhR1kVy-2_wWxihEWeAxV4WZVcYJg=='

    """
        Here we create influx connections for both server and client
    """

    source_client = InfluxDBClient(url=source_influx2_url, token=source_influx2_token, org=source_influx2_org,
                                   username=source_influx2_username, password=source_influx2_password,
                                   bucket=source_influx2_bucket)

    dest_client = InfluxDBClient(url=dest_influx2_url, token=dest_influx2_token, org=dest_influx2_org,
                                 username=dest_influx2_username, password=dest_influx2_password,
                                 bucket=dest_influx2_bucket)

    """
        Here we define Measurements
    """

    measurements_to_migrate = ['co2', 'current', 'energy', 'frequency', 'humidity', 'misc', 'power', 'speed',
                               'temperature', 'voltage']

    try:
        year = datetime.now().year
        month_list = range(1, 13)
        for measurement in measurements_to_migrate:
            for month in month_list:
                for day in range(1, 32):
                    """
                        Here we call calculate_dates function and get start and end dates
                    """
                    start_time, end_time = calculate_dates(year, month, day)

                    logger.info(
                        f"[FETCHING DATA 2.0] || FROM TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S')} | END TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

                    """
                        Here we call get_data_from_influx2 function and get data from server bucket
                    """
                    data_from_influx2 = get_data_from_influx2(source_client, measurement, start_time, end_time)

                    if data_from_influx2 is not None:
                        """
                            Here we call write_data_to_influx2 function and add data to client bucket
                        """
                        write_data_to_influx2(dest_client, measurement, data_from_influx2)
                        logger.info("Data transfer completed.")
                    else:
                        logger.debug("No data detected")
                        break

    except Exception as e:
        logger.error(f"Error Occurred in __main__ Function: {e}")
    finally:
        """
            Finally we close the influx connections
        """
        source_client.close()
        dest_client.close()
