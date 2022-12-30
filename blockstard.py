import argparse
import functools
import glob
import http.server
import json
import os
import socket
import socketserver
import threading
import time
import traceback
from datetime import datetime
from datetime import time as dtime
from datetime import timedelta

import matplotlib
import numpy as np

matplotlib.use("agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

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
                out_dir = os.path.join(args.db_dir, date_rec.strftime("%Y_%m_%d"))
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


def render_graph(day_start_time, day_end_time, sensor_name, times, temps, graph_file, legend=None):
    fig, ax = plt.subplots(1)
    fig.autofmt_xdate()
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=60))
    ax.set_xlim([day_start_time, day_end_time])
    ax.set_ylim([10, 40])
    ax.set_yticks(range(10, 40, 1), minor=True)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=90, ha="center")
    plt.grid(True, which="both", axis="both")
    plt.title(day_start_time.strftime("%Y-%m-%d") + " " + sensor_name)
    if isinstance(times, list) and isinstance(temps, list):
        for ti, te in zip(times, temps):
            plt.plot(ti, te)
    else:
        plt.plot(times, temps)
    if legend:
        plt.legend(legend)

    plt.savefig(graph_file)
    plt.close()


def is_file_older(file, hours=1):
    ti_c = os.path.getctime(file)
    graph_creation_time = datetime.fromtimestamp(ti_c)
    today = datetime.today()
    diff = today - graph_creation_time
    return diff > timedelta(hours=hours)


class CSVLog:
    def __init__(self, csv_file: str):
        with open(csv_file, "rt") as fp:
            lines = fp.readlines()

        self.header = lines[0].split(",")
        self.values = []
        lines.pop(0)
        for l in lines:
            self.values.append(l.split(","))

    def get_column_values(self, colname, dtype):
        idx = self.header.index(colname)
        all_values = []
        for v in self.values:
            all_values.append(v[idx])
        if dtype is str:
            return all_values
        return np.array(all_values, dtype=dtype)


def make_graphs(date_dir, is_today):
    log_data = CSVLog(os.path.join(date_dir, INDOORS_LOG))
    sensor_id_values = log_data.get_column_values("sensor_id", np.int32)
    unique_ids = np.unique(sensor_id_values)

    date_of_log = datetime.strptime(os.path.basename(date_dir), "%Y_%m_%d")
    day_start_time = datetime.combine(date_of_log, dtime.min)
    day_end_time = day_start_time + timedelta(days=1)

    all_times = []
    all_temps = []
    all_titles = []

    use_dir = date_dir if not is_today else os.path.dirname(date_dir)

    for sensor_id in unique_ids:
        if sensor_id not in whitelist_ids:
            continue

        graph_file = os.path.join(
            use_dir, INDOORS_GRAPH_PREFIX + str(sensor_id) + INDOORS_GRAPH_EXT
        )

        sensor_idx = np.where(sensor_id_values == sensor_id)
        times = mdates.datestr2num(log_data.get_column_values("datetime", np.object_)[sensor_idx])
        temps = log_data.get_column_values("temperature", np.float32)[sensor_idx]

        graph_title = str(sensor_id) + ": " + whitelist_ids[sensor_id]
        # Do not regenerate the graph if it exists

        render_this_graph = False
        if not os.path.exists(graph_file):
            # If today's data and the graph does not exist, make it
            # if it's yesterday's data and the graph does not exist make it
            render_this_graph = True
        elif is_today and is_file_older(graph_file):
            # Current day's graph, if it's more than 1 hour old, make it, save to root DB dir
            render_this_graph = True

        if render_this_graph:
            render_graph(day_start_time, day_end_time, graph_title, times, temps, graph_file)

        all_times.append(times)
        all_temps.append(temps)
        all_titles.append(graph_title)

    graph_file = os.path.join(use_dir, INDOORS_GRAPH_PREFIX + "all" + INDOORS_GRAPH_EXT)
    render_this_graph = False
    if not os.path.exists(graph_file):
        render_this_graph = True
    elif is_today and is_file_older(graph_file):
        # Current day's graph, save to root DB dir
        render_this_graph = True

    if render_this_graph:
        render_graph(
            day_start_time, day_end_time, "ALL", all_times, all_temps, graph_file, all_titles
        )


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
                is_today = date_dir >= midnight
                make_graphs(file, is_today)
            except Exception as err:
                # Ignore exceptions (bad date format, bad data, etc)
                traceback.print_exc()
                print(err)

        time.sleep(60)


def start_graph_gen(args):
    worker_thread = threading.Thread(target=functools.partial(gen_graphs_thread, args))
    worker_thread.start()


def httpserver_thread(args):
    PORT = args.http_port
    DIRECTORY = args.db_dir

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            self.directory = DIRECTORY
            super().__init__(*args, **kwargs)

        def end_headers(self):
            self.send_header("Cache-Control", "no-cache, no-store")
            super().end_headers(self)

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print("Server started at localhost:" + str(PORT))
        httpd.serve_forever()


def start_httpserver(args):
    worker_thread = threading.Thread(target=functools.partial(httpserver_thread, args))
    worker_thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_dir", help="Directory where to store the database", default="./")
    parser.add_argument("--http_port", help="HTTP serving port", default=9000)

    args = parser.parse_args()

    start_httpserver(args)
    start_graph_gen(args)
    run(args)
