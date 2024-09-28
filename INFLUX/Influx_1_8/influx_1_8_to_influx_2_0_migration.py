from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient as InfluxDB2Client
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
from loguru import logger
import calendar


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


def get_data_from_influx1(source_client, measurement, start_time, end_time):
    """
        In this function we get data from server influx 1.8 and return to caller variable
    """
    try:
        source_client.ping()
        result = source_client.query(
            f'SELECT * FROM "{measurement}" WHERE time >= \'{start_time}\' AND time <= \'{end_time}\' ')
        points = result.get_points()
        return list(points)
    except Exception as e:
        logger.error(f"Error retrieving data for measurement '{measurement}' from InfluxDB 1.8. Error: {str(e)}")
        return []


def write_data_to_influx2(dest_client, measurement, data, dest_influx2_bucket, dest_influx2_org):
    """
        In this function we add data to client influx bucket
    """
    try:
        dest_client.ready()
        write_api = dest_client.write_api(write_options=SYNCHRONOUS)
        influx2_data = []

        for point in data:
            try:
                time = datetime.strptime(point['time'], '%Y-%m-%dT%H:%M:%SZ')

                if measurement == 'pcmcount':
                    influx2_point = {
                        "measurement": measurement,
                        "tags": {
                            'panel_no': point['panel_no'],
                            'device_code': point['device_code'] if point['device_code'] is not None else None,
                            'device_name': point['device_name'] if point['device_name'] is not None else None,
                            'zone': point['zone'] if point['zone'] is not None else None,
                            'min_alarm': float(point['min_alarm']) if point['min_alarm'] is not None else None,
                            'max_alarm': float(point['max_alarm']) if point['max_alarm'] is not None else None,
                            'name': point['name'] if point['name'] is not None else None,
                            'alarm_name': point['alarm_name'] if point['alarm_name'] is not None else None,
                            'alarm_state': point['alarm_state'] if point['alarm_state'] is not None else None,
                            'unique_tag': point['unique_tag'] if point['unique_tag'] is not None else None
                        },
                        "time": time,
                        "fields": {'value': float(point['value'])}
                    }
                    influx2_data.append(influx2_point)
                elif 'panel_no' in point:
                    influx2_point = {
                        "measurement": measurement,
                        "tags": {
                            'panel_no': point['panel_no'],
                            'device_code': point['device_code'] if point['device_code'] is not None else None,
                            'device_name': point['device_name'] if point['device_name'] is not None else None,
                            'zone': point['zone'] if point['zone'] is not None else None,
                            'min_alarm': float(point['min_alarm']) if point['min_alarm'] is not None else None,
                            'max_alarm': float(point['max_alarm']) if point['max_alarm'] is not None else None,
                            'name': point['name'] if point['name'] is not None else None,
                            'alarm_name': point['alarm_name'] if point['alarm_name'] is not None else None,
                            'unique_tag': point['unique_tag'] if point['unique_tag'] is not None else None
                        },
                        "time": time,
                        "fields": {'value': float(point['value'])}
                    }
                    influx2_data.append(influx2_point)
                else:
                    influx2_point = {
                        "measurement": measurement,
                        "tags": {
                            'panel_no': point['a_panel_number'],
                            'device_code': point['device_code'],
                        },
                        "time": time,
                        "fields": {'uptime_': point['uptime_']}
                    }
                    influx2_data.append(influx2_point)
            except Exception as e:
                logger.error(f"Error processing data point for measurement '{measurement}'. Error: {str(e)}")
                logger.debug(f"Point data: {point}")

        write_api.write(dest_influx2_bucket, dest_influx2_org, influx2_data)
    except Exception as e:
        logger.error(f"Error in function write_data_to_influx2 - '{measurement}'. Error: {str(e)}")


if __name__ == "__main__":

    """
        Here we add the Server influx 1.8 details
    """
    influx1_host = '10.129.2.23'
    influx1_port = 8286
    influx1_database = 'data'
    influx1_username = 'admin'
    influx1_password = 'admin123'

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

    source_client = InfluxDBClient(host=influx1_host, port=influx1_port, username=influx1_username,
                                   password=influx1_password, database=influx1_database)

    dest_client = InfluxDB2Client(url=dest_influx2_url, token=dest_influx2_token, org=dest_influx2_org,
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
                        Here we call get_data_from_influx1 function and get data from influx1.8
                    """
                    data_from_influx1 = get_data_from_influx1(source_client, measurement, start_time, end_time)

                    if data_from_influx1 is not None:
                        """
                            Here we call write_data_to_influx2 function and add data to client bucket
                        """
                        write_data_to_influx2(dest_client, measurement, data_from_influx1, dest_influx2_bucket,
                                              dest_influx2_org)
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
