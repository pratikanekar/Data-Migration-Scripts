from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone
import psycopg2
from loguru import logger
import calendar

# Define InfluxDB connection parameters
INFLUXDB_URL = "http://192.168.1.11:8087"
INFLUXDB_TOKEN = "Vu1c6LfOvuVy941gcSYAQP4MaxgqeGg8utt731i3ZtOwqdGFNJQtTgBbWiiPQgt-TBYphj9x_LjgPEW9m6fUog=="
INFLUXDB_ORG = "iam"
INFLUXDB_username = 'root'
INFLUXDB_password = 'rootrootroot'
INFLUXDB_BUCKET = "lodha-supremus"

# Define PostgreSQL connection parameters
PG_HOST = "localhost"
PG_PORT = "5435"
PG_DATABASE = "central-bms"
PG_USER = "timescaledb"
PG_PASSWORD = "password"


def calculate_dates(month):
    try:

        end_time = datetime.now()

        current_year = end_time.year

        month_number = list(calendar.month_abbr).index(month[:3].capitalize())

        first_day_of_month = datetime(current_year, month_number, 1, 0, 0, 0)
        _, last_day_of_month = calendar.monthrange(current_year, month_number)
        last_day_of_month = datetime(current_year, month_number, last_day_of_month, 23, 59, 59)

        return first_day_of_month, last_day_of_month
    except Exception as e:
        print(f"{e}")


def migrate_data():
    try:
        measurements_to_migrate = ['co2', 'current', 'energy', 'frequency', 'humidity', 'misc', 'power', 'speed',
                                   'temperature', 'voltage']
        influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, username=INFLUXDB_username, password=INFLUXDB_password, bucket=INFLUXDB_BUCKET)
        query_api = influx_client.query_api()
        logger.debug("client connected successfully to influxdb")

        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        pg_cursor = pg_conn.cursor()
        logger.debug("client connected successfully to postgresdb")

        #
        # start_time, end_time = calculate_dates(month="jan")
        #
        # print(f"[FETCHING DATA 2.0] || FROM TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S')}  |  END TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        # end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        #

        # query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: {start_time_str}, stop: {end_time_str}) |> filter(fn: (r) => r["_measurement"] == "energy")'
        for measurement in measurements_to_migrate:
            query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: -1d) |> filter(fn: (r) => r["_measurement"] == "{measurement}")'
            result = query_api.query(query)

            for table in result:
                for record in table.records:
                    pg_cursor.execute(
                        "INSERT INTO o_iot_raw_data (panel_number, o_iot_device_code, o_iot_device_zone_code, min_alarm, max_alarm, o_iot_device_unique_tag, transaction_time, value, created_time, o_iot_device_id, o_iot_device_zone_id, panel_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                        record['panel_no'], record['device_code'], record['zone'], record['min_alarm'], record['max_alarm'],
                        record['unique_tag'], record['_time'], record['_value'], datetime.utcnow(), None, None, None)
                    )

                    pg_conn.commit()
            logger.debug(f"successfully migrated data of mesuremet = {measurement}")
        pg_cursor.close()
        pg_conn.close()
        influx_client.close()

    except Exception as e:
            logger.error(f'{e}')


if __name__ == "__main__":
    migrate_data()
