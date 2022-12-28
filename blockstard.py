import argparse
import glob
import json
import os
import socket
import threading
import time
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta

import matplotlib

matplotlib.use("agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas
import pandas as pd

UDP_IP = "127.0.0.1"
UDP_PORT = 1433

INDOORS_LOG = "indoors.csv"
WEATHER_LOG = "weather.csv"
PIPES_LOG = "pipes.csv"
INDOORS_GRAPH_PREFIX = "indoors_"
INDOORS_GRAPH_EXT = ".png"

whitelist_ids = []
try:
    lines = open("whitelist_sensor_ids.txt", "r").readlines()
    pairs = list(map(lambda s: s.split(","), lines))
    whitelist_ids = dict(map(lambda s: (int(s[0].strip()), s[1].strip()), pairs))
    print("Whitelisted ids: ", whitelist_ids)
except:
    pass

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.bind((UDP_IP, UDP_PORT))


def parse_syslog(line):
    """Try to extract the payload from a syslog line."""
    line = line.decode("ascii")  # also UTF-8 if BOM
    if line.startswith("<"):
        # fields should be "<PRI>VER", timestamp, hostname, command, pid, mid, sdata, payload
        fields = line.split(None, 7)
        line = fields[-1]
    return line


def rtl_433_probe(args):
    while True:
        line, _addr = sock.recvfrom(1024)

        try:
            line = parse_syslog(line)
            data = json.loads(line)

            date_rec = datetime.strptime(data["time"], "%Y-%m-%d %H:%M:%S")
            sensor_id = data["id"]
            temperature = data["temperature_C"]
            battery_ok = data["battery_ok"]

            if sensor_id in whitelist_ids:
                out_dir = os.path.join(
                    args.db_dir, f"{date_rec.year}_{date_rec.month}_{date_rec.day}"
                )
                out_file = os.path.join(out_dir, "indoors.csv")
                out_line = (
                    ",".join(map(lambda v: str(v), [date_rec, sensor_id, temperature, battery_ok]))
                    + "\n"
                )

                if not os.path.exists(out_dir):
                    os.makedirs(out_dir)
                if not os.path.exists(out_file):
                    with open(out_file, "wt") as fp:
                        fp.write("datetime,sensor_id,temperature,battery_ok\n")
                with open(out_file, "at") as fp:
                    fp.write(out_line)
            else:
                print("sensor not whitelisted", sensor_id)

        except KeyError:
            # The data record returned is not a valid temperature reading
            print("InvalidData")
            pass
        except Exception as err:
            print(err)
            pass


def run(args):
    # with daemon.DaemonContext(files_preserve=[sock]):
    #  detach_process=True
    #  uid
    #  gid
    #  working_directory
    rtl_433_probe(args)


def make_graphs(date_dir):
    log_data = pandas.read_csv(os.path.join(date_dir, INDOORS_LOG))
    unique_ids = log_data["sensor_id"].unique()

    date_of_log = datetime.strptime(os.path.basename(date_dir), "%Y_%m_%d")
    day_start_time = datetime.combine(date_of_log, dtime.min)
    day_end_time = day_start_time + timedelta(days=1)

    for sensor_id in unique_ids:
        graph_file = os.path.join(
            date_dir, INDOORS_GRAPH_PREFIX + str(sensor_id) + INDOORS_GRAPH_EXT
        )
        # Do not regenerate the graph if it exists
        if os.path.exists(graph_file):
            continue

        sensor_values = log_data[log_data["sensor_id"] == sensor_id]
        times = mdates.datestr2num(sensor_values["datetime"])
        temps = sensor_values["temperature"]

        fig, ax = plt.subplots(1)
        fig.autofmt_xdate()
        ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=60))
        ax.set_xlim([day_start_time, day_end_time])
        ax.set_ylim([10, 40])
        ax.set_yticks(range(10, 40, 1), minor=True)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=90)
        plt.grid(True, which="both", axis="both")
        plt.title(day_start_time.strftime("%Y-%m-%d") + " " + whitelist_ids[sensor_id])
        plt.plot(times, temps)

        plt.savefig(graph_file)


def gen_graphs_thread(args):
    midnight = datetime.combine(datetime.today(), dtime.min)
    while True:
        for file in glob.glob(args.db_dir + "/*"):
            # Ignore non-directories
            if not os.path.isdir(file):
                continue
            try:
                date_dir = datetime.strptime(os.path.basename(file), "%Y_%m_%d")
                # If it's a log directory with a date its name, check it's earlier than today
                if date_dir >= midnight:
                    continue
                make_graphs(file)
            except Exception as err:
                # Ignore exceptions (bad date format, bad data, etc)
                raise Exception() from err

        time.sleep(60)


def start_graph_gen(args):
    import functools

    worker_thread = threading.Thread(target=functools.partial(gen_graphs_thread, args))
    worker_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_dir", help="Directory where to store the database", default="./")

    args = parser.parse_args()

    start_graph_gen(args)
    run(args)
