from influxdb_client import InfluxDBClient
import pytz
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from loguru import logger


def write_data_to_influx2(dest_client, time, influx2_bucket, influx2_org):
    """
        In this function we write data in influx bucket data is hardcoded data which we add into influx
    """
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

    try:
        write_api = dest_client.write_api(write_options=SYNCHRONOUS)
        dest_client.ready()
        write_api.write(influx2_bucket, influx2_org, data)
    except Exception as e:
        logger.error(f"Error Occurred in write_data_to_influx2: {str(e)}")
    finally:
        dest_client.close()


if __name__ == "__main__":

    influx2_url = 'http://10.129.2.23:8086'
    influx2_org = 'iam'
    influx2_username = 'root'
    influx2_password = 'rootrootroot'
    influx2_bucket = 'data'
    influx2_token = '9rcqoz4onukDdYJYhR1kVy-2_wWxihEWeAxV4WZVcYJg=='

    desired_datetime = datetime(year=2023, month=11, day=1, hour=12, minute=59, second=59)

    """
        Here Converted the desired datetime into UTC and string format
    """
    time = (desired_datetime.replace(tzinfo=pytz.utc)).strftime('%Y-%m-%dT%H:%M:%SZ')
    """
        Set up InfluxDB 2.0 client connection
    """
    try:
        dest_client = InfluxDBClient(url=influx2_url, token=influx2_token, org=influx2_org,
                                     username=influx2_username, password=influx2_password,
                                     bucket=influx2_bucket)
        write_data_to_influx2(dest_client, time, influx2_bucket, influx2_org)
        logger.info("Data transfer completed.")
    except Exception as e:
        logger.error(f"Error Occurred in __main__ function - {e}")
