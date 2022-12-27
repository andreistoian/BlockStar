import socket
from datetime import datetime
import json
import os
import argparse
import glob
import time
import threading

UDP_IP = "127.0.0.1"
UDP_PORT = 1433

whitelist_ids = []
try:
    whitelist_ids = map(
        lambda s: int(s.strip()), open("whitelist_sensor_ids.txt", "r").read().split(",")
    )
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

        except KeyError:
            # The data record returned is not a valid temperature reading
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


def gen_graphs_thread(args):
    print("Doing stuff...")
    while True:
        for file in glob.glob(args.db_dir + "/*"):
            if os.path.isdir(file):
                print(file)
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
