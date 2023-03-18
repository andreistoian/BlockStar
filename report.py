import pandas as pd
import glob
import tqdm
import os
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import make_interp_spline

whitelist_ids = []
try:
    lines = open("whitelist_sensor_ids.txt", "r").readlines()
    pairs = list(map(lambda s: s.split(","), lines))
    whitelist_ids = dict(map(lambda s: (int(s[0].strip()), s[1].strip()), pairs))
    print("Whitelisted ids: ", whitelist_ids)
except:
    pass


def moving_average(a, n=3):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1 :] / n


DATA_FILE = "raw_data.csv"
if os.path.exists(DATA_FILE):
    data = pd.read_csv(DATA_FILE)

    NUM_COLORS = 5
    cm = plt.get_cmap("Paired")
    colors = [cm(1.0 * i / NUM_COLORS) for i in range(NUM_COLORS)]

    data["datetime"] = pd.to_datetime(data["datetime"])

    unique_ids = pd.unique(data["sensor_id"])
    for id in unique_ids:
        id_idx = data["sensor_id"] == id
        temp_id = data[["temperature", "weather_temp"]][id_idx]
        weather_id = data[["datetime", "weather_cloudy", "weather_wind"]][id_idx]

        arr_temp = temp_id.to_numpy()
        # is daytime, is sunny, is windy
        arr_weather = np.vstack(
            (
                weather_id["datetime"].apply(lambda ts: ts.hour > 9 and ts.hour < 17).to_numpy(),
                weather_id["weather_cloudy"].apply(lambda cloudcover: cloudcover <= 2).to_numpy(),
                weather_id["weather_wind"].apply(lambda wind: wind >= 20).to_numpy(),
            )
        ).transpose()

        unique_weather_temp = np.unique(arr_temp[:, 1])
        boxplot_temp = np.zeros((unique_weather_temp.shape[0], 6))
        weather_setting_temp = np.zeros((unique_weather_temp.shape[0], 6))

        for idx, v in enumerate(unique_weather_temp):
            temp_filter = arr_temp[:, 1] == v
            daytime_filter = arr_weather[:, 0] == 1
            sunny_filter = arr_weather[:, 1] == 1
            windy_filter = arr_weather[:, 2] == 1

            values_temp_inside = arr_temp[temp_filter, 0]
            # daytime, no clouds
            values_temp_inside_day_sun = arr_temp[
                np.logical_and(np.logical_and(temp_filter, sunny_filter), daytime_filter), 0
            ]
            # not sunny, windy
            values_temp_inside_day_windy = arr_temp[
                np.logical_and(
                    np.logical_and(temp_filter, np.logical_not(sunny_filter)), windy_filter
                ),
                0,
            ]
            # night or cloudy, not windy
            values_temp_inside_day_still = arr_temp[
                np.logical_and(
                    np.logical_and(
                        temp_filter,
                        np.logical_or(np.logical_not(daytime_filter), np.logical_not(sunny_filter)),
                    ),
                    np.logical_not(windy_filter),
                ),
                0,
            ]

            boxplot_temp[idx, 0] = v
            boxplot_temp[idx, 1:] = np.percentile(values_temp_inside, [5, 25, 50, 75, 95])

            weather_setting_temp[idx, 0] = np.mean(values_temp_inside_day_still)
            weather_setting_temp[idx, 1] = np.std(values_temp_inside_day_still)

            if values_temp_inside_day_windy.size > 0:
                weather_setting_temp[idx, 2] = np.mean(values_temp_inside_day_windy)
                weather_setting_temp[idx, 3] = np.std(values_temp_inside_day_windy)
            else:
                weather_setting_temp[idx, 2] = np.nan
                weather_setting_temp[idx, 3] = np.nan

            if values_temp_inside_day_sun.size > 0:
                weather_setting_temp[idx, 4] = np.mean(values_temp_inside_day_sun)
                weather_setting_temp[idx, 5] = np.std(values_temp_inside_day_sun)
            else:
                weather_setting_temp[idx, 4] = np.nan
                weather_setting_temp[idx, 5] = np.nan

        fig = plt.figure(figsize=(12, 6))
        wnd = 5
        smooth_temp = moving_average(boxplot_temp[:, 3], wnd)
        plt.plot(boxplot_temp[:, 0], boxplot_temp[:, 3], linewidth=0.5)
        plt.plot(boxplot_temp[wnd // 2 : -wnd // 2 + 1, 0], smooth_temp, linewidth=3)
        for idxp, (i1, i2) in enumerate([(1, 5), (2, 4)]):
            fig.axes[0].fill_between(
                boxplot_temp[:, 0],
                y1=boxplot_temp[:, i1],
                y2=boxplot_temp[:, i2],
                alpha=0.40,
                facecolor=colors[idxp],
            )
        plt.title(f"Inside vs outside for {whitelist_ids[id]}")
        plt.ylabel("Inside Temperature")
        plt.xlabel("Outside temperature (Orly)")
        plt.ylim(15, 25)
        minor_ticks = np.arange(15, 25, 0.25)
        fig.axes[0].set_yticks(minor_ticks, minor=True)
        plt.grid(True, "both", color="#CCCCCC", linestyle="--")
        plt.savefig(f"temp_{whitelist_ids[id].replace(' ', '_')}.png")
        plt.show()

        fig = plt.figure(figsize=(12, 6))
        plt.scatter(boxplot_temp[:, 0], weather_setting_temp[:, 0])
        plt.errorbar(boxplot_temp[:, 0], weather_setting_temp[:, 0], yerr=weather_setting_temp[:, 1], fmt="o")
        plt.scatter(boxplot_temp[:, 0], weather_setting_temp[:, 2])
        plt.errorbar(boxplot_temp[:, 0], weather_setting_temp[:, 2], yerr=weather_setting_temp[:, 3], fmt="o")
        plt.scatter(boxplot_temp[:, 0], weather_setting_temp[:, 4])
        plt.errorbar(boxplot_temp[:, 0], weather_setting_temp[:, 4], yerr=weather_setting_temp[:, 5], fmt="o")
        plt.legend(["No sun, no wind", "Windy", "Sunny"])
        plt.title(f"Inside vs outside for {whitelist_ids[id]}")
        plt.ylabel("Inside Temperature")
        plt.xlabel("Outside temperature (Orly)")
        plt.ylim(15, 25)
        minor_ticks = np.arange(15, 25, 0.25)
        fig.axes[0].set_yticks(minor_ticks, minor=True)
        plt.grid(True, "both", color="#CCCCCC", linestyle="--")        
        plt.show()

else:
    all_csv_temp = glob.glob("./db_rpi/database/**/indoors.csv")

    all_temp = None
    all_weather = None
    for fname in all_csv_temp:
        day_data_temps = pd.read_csv(fname)
        try:
            day_data_weather = pd.read_csv(fname.replace("indoors", "weather"))
        except:
            continue

        if all_temp is None:
            all_temp = day_data_temps
            all_weather = day_data_weather
        else:
            all_temp = pd.concat((all_temp, day_data_temps))
            all_weather = pd.concat((all_weather, day_data_weather))

    all_temp["datetime"] = pd.to_datetime(all_temp["datetime"])
    all_weather["datetime"] = pd.to_datetime(all_weather["datetime"])

    all_temp = all_temp.sort_values("datetime")
    all_weather = all_weather.sort_values("datetime")

    weather_temp = [None for _ in range(all_temp.shape[0])]
    weather_wind = [None for _ in range(all_temp.shape[0])]
    weather_cloudy = [None for _ in range(all_temp.shape[0])]

    remove_idx = []
    for idx in tqdm.tqdm(range(all_temp.shape[0])):
        date_reading = all_temp.iloc[idx]["datetime"]
        weather_reading = all_weather["datetime"].searchsorted(date_reading)
        try:
            date_weather = all_weather.iloc[weather_reading]["datetime"]
            weather_temp[idx] = all_weather.iloc[weather_reading]["temperature"]
            weather_wind[idx] = all_weather.iloc[weather_reading]["wind_avg"]
            weather_cloudy[idx] = all_weather.iloc[weather_reading]["cloudcover"]
        except:
            remove_idx.append(int(idx))

    all_temp["weather_temp"] = weather_temp
    all_temp["weather_wind"] = weather_wind
    all_temp["weather_cloudy"] = weather_cloudy

    all_temp = all_temp.drop(all_temp.index[remove_idx])

    all_temp.to_csv(DATA_FILE)
