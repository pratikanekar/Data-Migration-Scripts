from influxdb_client import InfluxDBClient
import calendar
from datetime import datetime
from loguru import logger
import psycopg2


def calculate_dates(month):
    """
        In this function we calculate dates for specific month and return start and end date
    """
    try:
        end_time = datetime.now()
        current_year = end_time.year
        month_number = list(calendar.month_abbr).index(month[:3].capitalize())
        first_day_of_month = datetime(current_year, month_number, 1, 0, 0, 0)
        last_day_of_year, last_day_of_month = calendar.monthrange(current_year, month_number)
        last_day_of_month = datetime(current_year, month_number, last_day_of_month, 23, 59, 59)

        return first_day_of_month, last_day_of_month
    except Exception as e:
        logger.error(f"Error Occurred in calculate_dates: {e}")


def get_data_from_influx2(influx_client, measurement, start_time, end_time, INFLUXDB_ORG, INFLUXDB_BUCKET):
    """
        In this function we get data from server influx bucket and return to caller variable
    """
    try:
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        query = f'from(bucket: "{INFLUXDB_BUCKET}") |> range(start: {start_time_str}, stop: {end_time_str}) |> filter(fn: (r) => r["_measurement"] == "{measurement}")'
        result = influx_client.query_api().query(org=INFLUXDB_ORG, query=query)
        return result
    except Exception as e:
        logger.error(f"Error retrieving data for measurement '{measurement}' from InfluxDB 2.0. Error: {str(e)}")
        return []


def write_data_to_postgres(pg_conn, pg_cursor, data, measurement):
    """
        In this function we add data to postgres database table
    """
    try:
        for table in data:
            for record in table.records:
                pg_cursor.execute(
                    "INSERT INTO o_iot_raw_data (panel_number, o_iot_device_code, o_iot_device_zone_code, min_alarm, max_alarm, o_iot_device_unique_tag, transaction_time, value, created_time, o_iot_device_id, o_iot_device_zone_id, panel_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        record['panel_no'], record['device_code'], record['zone'], record['min_alarm'],
                        record['max_alarm'],
                        record['unique_tag'], record['_time'], record['_value'], datetime.utcnow(), None, None, None)
                )

                pg_conn.commit()
        logger.debug(f"successfully migrated data of measurement = {measurement}")
    except Exception as e:
        logger.error(f"Error in write_data_to_postgres for measurement '{measurement}'. Error: {str(e)}")


if __name__ == "__main__":

    """
        Here we add influx 2.0 details
    """
    INFLUXDB_URL = "http://192.168.1.11:8086"
    INFLUXDB_TOKEN = "Vu1c6Lfi3ZtOwqdGFNJQtTgBbWiiPQgt-TBYphj9x_LjgPEW9m6fUog=="
    INFLUXDB_ORG = "iam"
    INFLUXDB_username = 'root'
    INFLUXDB_password = 'rootrootroot'
    INFLUXDB_BUCKET = "data"

    """
        Here we add postgres database details
    """
    PG_HOST = "localhost"
    PG_PORT = "5435"
    PG_DATABASE = "central-stack"
    PG_USER = "timescaledb"
    PG_PASSWORD = "password"

    """
        Here we create influx connection
    """

    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, username=INFLUXDB_username,
                                   password=INFLUXDB_password, bucket=INFLUXDB_BUCKET)

    pg_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )
    pg_cursor = pg_conn.cursor()
    logger.debug("client connected successfully to postgresdb")

    """
        Here we define Measurements
    """

    measurements_to_migrate = ['co2', 'current', 'energy', 'frequency', 'humidity', 'misc', 'power', 'speed',
                               'temperature', 'voltage']

    try:
        for measurement in measurements_to_migrate:
            """
                Here we call calculate_dates function and get start and end dates
            """
            start_time, end_time = calculate_dates(
                month="sep")  # Here we can specify the month which want to migrate data

            logger.info(
                f"[FETCHING DATA 2.0] || FROM TIME: {start_time.strftime('%Y-%m-%d %H:%M:%S')} | END TIME: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

            """
                Here we call get_data_from_influx2 function and get data from influx
            """
            data_from_influx2 = get_data_from_influx2(influx_client, measurement, start_time, end_time, INFLUXDB_ORG,
                                                      INFLUXDB_BUCKET)

            if data_from_influx2 is not None:
                """
                    Here we call write_data_to_influx2 function and add data to postgres database
                """
                write_data_to_postgres(pg_conn, pg_cursor, data_from_influx2, measurement)
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
        pg_cursor.close()
        pg_conn.close()
        influx_client.close()
