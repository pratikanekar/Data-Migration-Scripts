from influxdb import InfluxDBClient
from influxdb_client import InfluxDBClient as InfluxDB2Client
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
# InfluxDB 1.x connection settings
influx1_host = '10.13.10.102'
influx1_port = 8088
influx1_database = 'data'
influx1_username = 'admin'
influx1_password = 'admin123'

# InfluxDB 2.0 connection settings
influx2_url = 'http://10.11.12.35:8086'
influx2_org = 'iam'
influx2_username = 'root'
influx2_password = 'rootrootroot'
influx2_bucket = 'data'
influx2_token = '1013_Qx0-F__xgNuENCGC1sWwdACgFWjJ6Esjon79vk971ajwxb231oVEgm4hY7_smC_fqCNDfYDNz8qdyE9iQ=='

# measurements_to_migrate = ['co2', 'current', 'energy', 'event', 'frequency', 'humidity', 'misc', 'netd_info', 'power', 'speed',
# 'temperature', 'voltage', 'acmode', 'acspeed', 'acstate', 'acswing', 'operation', 'pcmcount']
measurement = 'co2'


def get_data_from_influx1(measurement, start_time, end_time):
    client = InfluxDBClient(host=influx1_host, port=influx1_port, username=influx1_username, password=influx1_password,
                            database=influx1_database)
    try:
        client.ping()  # Check if the connection is successful
        print(f"Connected to InfluxDB 1.8 for measurement '{measurement}' successfully.")
        result = client.query(
            f'SELECT * FROM "{measurement}" WHERE time >= \'{start_time}\' AND time <= \'{end_time}\' ')  # Use double quotes around the measurement name
        points = result.get_points()
        client.close()
        return list(points)
    except Exception as e:
        print(f"Error retrieving data for measurement '{measurement}' from InfluxDB 1.8. Error: {str(e)}")
        return []


def write_data_to_influx2(measurement, data):
    client = InfluxDB2Client(url=influx2_url, username=influx2_username, password=influx2_password, token=influx2_token,
                             org=influx2_org)
    try:
        client.ready()  # Check if the connection is successful
        print(f"Connected to InfluxDB 2.0 for measurement '{measurement}' successfully.")
        write_api = client.write_api(write_options=SYNCHRONOUS)
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
                print(f"Error processing data point for measurement '{measurement}'. Error: {str(e)}")
                print(f"Point data: {point}")

        write_api.write(influx2_bucket, influx2_org, influx2_data)
        print(f"Migration of measurement '{measurement}' successful.")
    except Exception as e:
        print(f"Error connecting to InfluxDB 2.0 for measurement '{measurement}'. Error: {str(e)}")
    finally:
        client.close()


if __name__ == "__main__":

    data_found_flag = True

    # Decalring Datetime in the Begining
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    # Converting the Datetime to String
    end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')

    print(f"[FETCHING DATA 1.8] || FROM TIME: {start_time}  |  END TIME: {end_time}")

    while data_found_flag:
        data_from_influx1 = get_data_from_influx1(measurement, start_time, end_time)
        if len(data_from_influx1) > 0:
            write_data_to_influx2(measurement, data_from_influx1)
            print("Data transfer completed.")
            print("Changing Date Time.../")

            # De-Crimenting Datetime
            end_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            start_time = end_time - timedelta(days=7)

            # Converting the Datetime to String
            end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')

        else:
            data_found_flag = False
            print(f"No More Data For Measurement: {measurement}")
