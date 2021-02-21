#!/usr/bin/python
import time
import os
from influxdb import InfluxDBClient
from SolarMax.solarmax import SolarMax
import configparser
import threading
import click
import logging.handlers

log = logging.getLogger("solarmax_logger")

query_dict = {
    'KDY': 'Energy today (kWh)', 'KDL': 'Energy yesterday (Wh)', 'KYR': 'Energy this year (kWh)',
    'KLY': 'Energy last year (kWh)',
    'KMT': 'Energy this month (kWh)', 'KLM': 'Energy last month (kWh)', 'KT0': 'Total Energy(kWh)',
    'IL1': 'AC Current Phase 1 (A)',
    'IL2': 'AC Current Phase 2 (A)', 'IL3': 'AC Current Phase 3 (A)', 'IDC': 'DC Current (A)', 'PAC': 'AC Power (W)',
    'PDC': 'DC Power (W)', 'PRL': 'Relative power (%)', 'TNP': 'Grid period duration',
    'TNF': 'Generated Frequency (Hz)', 'TKK': 'Inverter Temperature (C)', 'UL1': 'AC Voltage Phase 1 (V)',
    'UL2': 'AC Voltage Phase 2 (V)', 'UL3': 'AC Voltage Phase 3 (V)', 'UDC': 'DC Voltage (V)',
    'UD01': 'String 1 Voltage (V)', 'UD02': 'String 2 Voltage (V)', 'UD03': 'String 3 Voltage (V)',
    'ID01': 'String 1 Current (A)', 'ID02': 'String 2 Current (A)', 'ID03': 'String 3 Current (A)',
    'ADR': 'Address', 'TYP': 'Type', 'PIN': 'Installed Power (W)', 'CAC': 'Start Ups (?)', 'KHR': 'Operating Hours',
    'SWV': 'Software Version', 'DDY': 'Date day', 'DMT': 'Date month', 'DYR': 'Date year', 'THR': 'Time hours',
    'TMI': 'Time minutes', 'LAN': 'Language',
    'SAL': 'System Alarms', 'SYS': 'System Status',
    'MAC': 'MAC Address', 'EC00': 'Error Code 0', 'EC01': 'Error Code 1', 'EC02': 'Error Code 2',
    'EC03': 'Error Code 3',
    'EC04': 'Error Code 4', 'EC05': 'Error Code 5', 'EC06': 'Error Code 6', 'EC07': 'Error Code 7',
    'EC08': 'Error Code 8',
    'BDN': 'Build number',
    'DIN': '?',
    'SDAT': 'datetime ?', 'FDAT': 'datetime ?',
    'U_AC': '?', 'F_AC': 'Grid Frequency', 'SE1': '',
    'U_L1L2': 'Phase1 to Phase2 Voltage (V)', 'U_L2L3': 'Phase2 to Phase3 Voltage (V)',
    'U_L3L1': 'Phase3 to Phase1 Voltage (V)'
}


def init_logger(logdir, loglevel):
    log_formatter = logging.Formatter('%(asctime)s - %(name)20s - %(threadName)10s - %(levelname)6s - %(message)s')
    rootlogger = logging.getLogger()
    if not os.path.isdir(logdir):
        os.mkdir(logdir)

    file_handler = logging.handlers.RotatingFileHandler(os.path.join(logdir, "solarmax_logger.log"), maxBytes=1e7,
                                                        backupCount=10)
    file_handler.setLevel(loglevel)
    file_handler.setFormatter(log_formatter)
    rootlogger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)
    console_handler.setFormatter(log_formatter)
    rootlogger.addHandler(console_handler)
    rootlogger.setLevel(loglevel)


def solarmax_logger(inverters, influxdb_host, influxdb_port, database, location, user, password):
    smlist = []
    log.info("Start: connect to inverter, query and push metrics Solarmax")
    for host in inverters.keys():
        sm = SolarMax(host, 12345)  # using port 12345
        if sm.connected():
            sm.use_inverters(inverters[host])
            smlist.append(sm)

    allinverters = []
    for host in inverters.keys():
        allinverters.extend(inverters[host])

    count = 0
    timestamp = 0

    if smlist:
        influxdb = InfluxDBClient(influxdb_host, influxdb_port, user, password, database)
        for sm in smlist:
            for (inverter, inverter_data) in sm.inverters().iteritems():
                try:
                    host_ip = sm.host_ip()
                    measurements = dict()

                    # Pass the parameters you wish to get from the inverter and log.
                    log.info("Sending query to inverter #{}".format(inverter))
                    (inverter, current) = sm.query(inverter, ['PAC', 'UL1', 'TKK', 'KDY', 'KMT', 'KYR', 'KT0'])
                    log.debug("Parsed answer: {}".format(current))

                    if not current:
                        log.info("Query to inverter #{} returned no data. Continue to next inverter...".format(
                            inverter))
                        continue

                    log.info("Successfully retrieved data from inverter {}".format(inverter))

                    # Use system date/Time for logging.
                    if not timestamp:
                        timestamp = int(time.time() * 1000)

                    # Parse the results of sm.query above to human readable variables
                    measurements["power_generation"] = ["watt", float(current['PAC'])]
                    measurements["voltage"] = ["ac_volt", float(current['UL1'])]
                    measurements["inverter_temperature"] = ["celsius", float(current['TKK'])]
                    measurements["energy_generation_today"] = ["kwh", float(current['KDY'])]
                    measurements["energy_generation_this_month"] = ["kwh", float(current['KMT'])]
                    measurements["energy_generation_this_year"] = ["kwh", float(current['KYR'])]
                    measurements["energy_generation_total"] = ["kwh", float(current['KT0'])]

                    # Write data to influxdb
                    log.info("Sending measurements from inverter #{} to database...".format(inverter))
                    for measurement in measurements.keys():
                        influx_data_points = ("{measurement},"
                                              "host_ip={host_ip},"  # tag
                                              "inverter={inverter},"  # tag
                                              "location={location} "  # tag
                                              "{value_name}={measurement_value} "  # field
                                              "{timestamp}".format(measurement=measurement,
                                                                   host_ip=host_ip, inverter=inverter,
                                                                   location=location,
                                                                   value_name=measurements[measurement][0],
                                                                   measurement_value=measurements[measurement][1],
                                                                   timestamp=timestamp))
                        log.debug("Influx data points to send: {}".format(influx_data_points))

                        try:
                            log.debug("Sending measurement {} to database".format(measurement))
                            response = influxdb.write_points(influx_data_points, time_precision='ms', protocol='line')

                        except Exception as e:
                            log.exception("Exception happened sending data from inverter to database: {}".format(e))
                            continue

                        if response is not True:
                            log.error("Something went wrong sending measurement from inverter #{} to database.".format(
                                inverter))
                            continue

                        log.debug("Influx data points successfully sent to #{}".format(inverter))

                    log.info("Measurements from inverter #{} successfully sent to database".format(inverter))

                    count += 1

                except Exception as e:
                    log.exception("Exception with inverter #{}:".format(inverter, e))
                    continue

    if count < len(allinverters):
        log.info("Not all inverters queried ({} < {})".format(count, len(allinverters)))

    log.info("End: connect to inverter, query and push metrics Solarmax")


def sync_loop_solarmax_logger(inverters, influxdb_host, influxdb_port, database, location, user, password):
    log.info("Solarmax logger started")

    last_log = time.time()
    next_run = time.time()

    while True:
        log_now = time.time()
        if (log_now - last_log) > 30:
            last_log = log_now
            log.info("Solarmax logger thread active, next run in {} seconds".format(int(next_run - log_now)))

        if time.time() >= next_run:
            try:
                solarmax_logger(inverters, influxdb_host, influxdb_port, database, location, user, password)

            except Exception as e:
                log.exception("Error query and push Solarmax metrics: {}".format(e))

            finally:
                next_run = time.time() + (60 + 1)
                log.info("Next Solarmax logger run will be in {} seconds".format(int(next_run - time.time())))

        time.sleep(10)


def start_thread_solarmax_logger(inverters, influxdb_host, influxdb_port, database, location, user, password):
    thread = threading.Thread(name="MainThread", target=sync_loop_solarmax_logger,
                              args=[inverters, influxdb_host, influxdb_port, database, location, user, password])
    thread.setDaemon(True)
    thread.start()
    thread.join()


@click.command()
@click.argument("configfile", type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True,
                                              resolve_path=True))
def process(configfile):
    config = configparser.ConfigParser()
    config.read([configfile])
    logdir = config.get("default", "logdir")
    loglevel = config.get("default", "loglevel")
    influxdb_host = config.get("influxdb", "influxdb_host")
    influxdb_port = config.get("influxdb", "influxdb_port")
    database = config.get("influxdb", "database")
    location = config.get("influxdb", "location")
    user = config.get("influxdb", "user")
    password = config.get("influxdb", "password")
    inverters = dict(config.items("inverters"))
    for host, devices in inverters.items():
        devices = map(int, devices.split(','))  # convert devices to integers
        inverters[host] = devices

    init_logger(logdir, loglevel)
    start_thread_solarmax_logger(inverters, influxdb_host, influxdb_port, database, location, user, password)
    exit()


if __name__ == '__main__':
    process()
