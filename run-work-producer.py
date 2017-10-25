#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
# Fikadu Getachew <fikadu.getachew@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import json
import os
import sys
import time
import zmq
import copy
import monica_io
import soil_io
import ascii_io
from datetime import date, timedelta
import numpy as np
from collections import defaultdict
from scipy.interpolate import NearestNDInterpolator
from pyproj import Proj, transform


PATHS = {
    "fikadu": {
        "include-file-base-path": "C:/GitHub",
        "local-path-to-archive": "Z:/md/projects/carbiocial/",
        "local-path-to-repository": "C:/GitHub/carbiocial-2017/"
    },
    "stella": {
        "include-file-base-path": "C:/Users/stella/Documents/GitHub",
        "local-path-to-archive": "Z:/projects/carbiocial/",
        "local-path-to-repository": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    },
    "berg-xps15": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "local-path-to-archive": "A:/data/ethiopia/",
        "local-path-to-repository": "C:/Users/berg.ZALF-AD/GitHub/ethiopia-cc-impact/",
        "cluster-path-to-archive": "/archiv-daten/md/data/ethiopia/"
    },
    "berg-lc": {
        "include-file-base-path": "C:/Users/berg.ZALF-AD/GitHub",
        "local-path-to-archive": "A:/data/ethiopia/",
        "local-path-to-repository": "C:/Users/berg.ZALF-AD/GitHub/ethiopia-cc-impact/",
        "cluster-path-to-archive": "/archiv-daten/md/data/ethiopia/"
    }
}


def main():
    "main function"

    config = {
        "port": "6666",
        "server": "cluster3",
        "user": "berg-lc",
        "local-paths": "false"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v 

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])
    
    paths = PATHS[config["user"]]
    use_local_paths = config["local-paths"] == "true"

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    sim["include-file-base-path"] = paths["include-file-base-path"]

    rcps = [
        "baseline",
        "rcp2p6",
        "rcp4p5",
        "rcp6p0",
        "rcp8p5"
    ]

    sorghum_varieties = ["meko", "teshale"]

    def create_climate_interpolator(path_to_climate_dir, scenario, wgs84, utm37n):
        "create interpolation object from some dir with climate data"
        points = []
        values = []
        for filename in os.listdir(path_to_climate_dir + scenario + "/"):
            #if filename[:len(scenario)] != scenario:
            #    continue

            #parse from "baseline_3.25_33.25.csv"
            parts = filename.split("_")
            lat = float(parts[1]) #float(parts[1].strip())
            lon = float(parts[2][:-4]) #float(parts[2][:-4].strip())

            r, h = transform(wgs84, utm37n, lon, lat)
            #xlon, xlat = transform(utm37n, wgs84, r, h)
            points.append([h, r])
            values.append((lat, lon))
            #print "lat:", lat, "lon:", lon, "h:", h, "r:", r, "val:", values[len(values)-1]

        return NearestNDInterpolator(np.array(points), np.array(values))

    wgs84 = Proj(init="epsg:4326")
    utm37n = Proj(init="epsg:20137")
    interpol_climate = create_climate_interpolator(paths["local-path-to-archive"] + "climate/ipsl-cm5a-lr/", "baseline", wgs84, utm37n)


    def create_soil_profiles(path_to_soil_csv):
        "load/create soil profiles"
        with open(path_to_soil_csv) as _:
            profiles = defaultdict(list)
            reader = csv.reader(_, delimiter=",")
            reader.next()
            for line in reader:
                profiles[(float(line[0]), float(line[1]))].append({
                    "Thickness": float(line[2]),
                    "Sand": float(line[3]),
                    "Clay": float(line[4]),
                    "pH": float(line[5]),
                    "FieldCapacity": float(line[6]),
                    "PermanentWiltingPoint": float(line[7]),
                    "SoilBulkDensity": float(line[8]),
                    "SoilOrganicCarbon": float(line[9])
                })
            return profiles

    profiles = create_soil_profiles(paths["local-path-to-archive"] + "soil/soil.csv")

    def create_slope_or_elevation_interpolator(path_to_csv):
        "load slope or elevation data"
        points = []
        values = []
        with open(path_to_csv) as _:
            reader = csv.reader(_, delimiter=",")
            reader.next()
            for line in reader:
                lon = float(line[0])
                lat = float(line[1])
                value = float(line[2])
                r, h = transform(wgs84, utm37n, lon, lat)
                #xlon, xlat = transform(utm37n, wgs84, r, h)
                points.append([h, r])
                values.append(value)
                #print "lat:", lat, "lon:", lon, "h:", h, "r:", r, "val:", values[len(values)-1]

        return NearestNDInterpolator(np.array(points), np.array(values))

    interpol_slope = create_slope_or_elevation_interpolator(paths["local-path-to-archive"] + "slope/slope.csv")
    interpol_elevation = create_slope_or_elevation_interpolator(paths["local-path-to-archive"] + "elevation/elevation.csv")

    def read_onset_dates(path_to_onset_dates_csv):
        "load onset dates"
        with open(path_to_onset_dates_csv) as _:
            onsets = defaultdict(dict)
            reader = csv.reader(_, delimiter=",")
            reader.next()
            for line in reader:
                xxx, slat, slon = line[3].split(" _ ")
                onsets[(float(slat), float(slon))][int(line[0])] = int(line[1])
            return onsets

    env = monica_io.create_env_json_from_json_config({
        "crop": crop,
        "site": site,
        "sim": sim,
        "climate": ""
    })
    templates = {
        "meko": env["cropRotation"].pop("meko"),
        "teshale": env["cropRotation"].pop("teshale"),
        "automatic-sowing": env["cropRotation"].pop("automatic-sowing"),
        "static-sowing": env["cropRotation"].pop("static-sowing"),
        "rest-worksteps": env["cropRotation"].pop("rest-worksteps"),
        "cultivation-method": env["cropRotation"].pop("cultivation-method")
    }
    env["cropRotation"] = []
    

    adaptation_options = []
    for sowing in ["recommended|avg-static-elevation-onsets", "recommended|dynamic-elevation-onsets", "calculated-onsets"]:
        for n_fert in ["recommended", "auto"]:
            for plant_density in ["recommended", "increased"]:
                for cycle_length in ["standard", "longer"]:
                    adaptation_options.append({
                        "sowing": sowing,
                        "fertilizer": n_fert,
                        "plant-density": plant_density,
                        "cycle-length": cycle_length
                    })

    elevation_ranges = {
        "<1600": { 
            "onsets": {"from": date(2017, 6, 10), "to": date(2017, 6, 30)},
            "plant-density": {"from": 8, "to": 13},
            "fertilizer": {"N": 46, "U": 46}
        },
        "=>1600&<=1900": { 
            "onsets": {"from": date(2017, 5, 1), "to": date(2017, 5, 15)},
            "plant-density": {"from": 9, "to": 12},
            "fertilizer": {"N": 50, "U": 75}
        },
        ">1900": { 
            "onsets": {"from": date(2017, 4, 15), "to": date(2017, 5, 10)},
            "plant-density": {"from": 7, "to": 10},
            "fertilizer": {"N": 57, "U": 69}
        },
    }

    def elevation_range(elevation):
        if elevation < 1600:
            return elevation_ranges["<1600"]
        elif elevation <= 1900:
            return elevation_ranges["=>1600&<=1900"]
        else:
            return elevation_ranges[">1900"]

    def avg_static_elevation_onsets(elevation):
        "return avg onsets for given elevation"
        d = elevation_range(elevation)["onsets"]
        avg_doy = (d["from"].timetuple().tm_yday + d["to"].timetuple().tm_yday) // 2
        return (date(2017, 1, 1) + timedelta(days=avg_doy)).strftime("0000-%m-%d")
    


    start_send = time.clock()
    sent_env_count = 0

    for rcp in rcps:

        onsets = read_onset_dates(paths["local-path-to-archive"] + "onset-dates/" + rcp + ".csv")

        for adaptation_option in adaptation_options:
            
            for variety in sorghum_varieties:

                for (lat, lon), profile in profiles.iteritems():

                    sr, sh = transform(wgs84, utm37n, lon, lat)
                    (clat, clon) = interpol_climate(sh, sr)
                    slope = interpol_slope(sh, sr)
                    elevation = interpol_elevation(sh, sr)

                    env["params"]["siteParameters"]["SoilProfileParameters"] = profile
                    env["params"]["siteParameters"]["Latitude"] = lat
                    env["params"]["siteParameters"]["Slope"] = slope
                    env["params"]["siteParameters"]["HeightNN"] = elevation

                    # set fertilization
                    if adaptation_option["fertilizer"] == "recommended":
                        fert = elevation_range(elevation)["fertilizer"]
                        templates["rest-worksteps"][0]["amount"][0] = fert["N"]
                        templates["rest-worksteps"][1]["amount"][0] = float(fert["U"]) / 2
                        templates["rest-worksteps"][2]["amount"][0] = float(fert["U"]) / 2
                    elif adaptation_option["fertilizer"] == "auto":
                        print "to be done"

                    # set plant density
                    plant_density = 0
                    if adaptation_option["plant-density"] == "recommended":
                        pd_range = elevation_range(elevation)["plant-density"]
                        plant_density = (pd_range["from"] + pd_range["to"]) // 2
                    elif adaptation_option["plant-density"] == "increased":
                        print "to be done"

                    # set cycle length
                    if adaptation_option["cycle-length"] == "standard":
                        print "to be done"
                    elif adaptation_option["plant-density"] == "increased":
                        print "to be done"

                    # insert static sowing
                    if adaptation_option["sowing"] == "recommended|avg-static-elevation-onsets":
                        templates["static-sowing"]["crop"] = templates[variety]
                        templates["static-sowing"]["date"] = avg_static_elevation_onsets(elevation)
                        templates["static-sowing"]["PlantDensity"] = plant_density
                        templates["cultivation-method"]["worksteps"] = [templates["static-sowing"]] + templates["rest-worksteps"]
                        env["cropRotation"] = [templates["cultivation-method"]]

                    elif adaptation_option["sowing"] == "recommended|dynamic-elevation-onsets":
                        templates["automatic-sowing"]["crop"] = templates[variety]
                        templates["automatic-sowing"]["earliest-date"] = avg_static_elevation_onsets(elevation)
                        templates["static-sowing"]["PlantDensity"] = plant_density
                        templates["cultivation-method"]["worksteps"] = [templates["automatic-sowing"]] + templates["rest-worksteps"]
                        env["cropRotation"] = [templates["cultivation-method"]]

                    elif adaptation_option["sowing"] == "calculated-onsets":
                        year_to_onset = onsets[(clat, clon)]
                        templates["static-sowing"]["crop"] = templates[variety]
                        templates["static-sowing"]["PlantDensity"] = plant_density
                        templates["cultivation-method"]["worksteps"] = [templates["static-sowing"]] + templates["rest-worksteps"]
                        env["cropRotation"] = []
                        for year in sorted(year_to_onset.keys()):
                            cm = templates["cultivation-method"].deepcopy()
                            onset_date = (date(2017, 1, 1) + timedelta(days=year_to_onset[year])).strftime("0000-%m-%d")
                            cm["worksteps"][0]["date"] = onset_date
                            env["cropRotation"].append(cm)

                    

                    #set climate file - read by the server
                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                    if rcp == "baseline":
                        env["csvViaHeaderOptions"]["start-date"] = "1972-01-01"
                        env["csvViaHeaderOptions"]["end-date"] = "1999-12-31"
                    else:
                        env["csvViaHeaderOptions"]["start-date"] = "2011-01-01"
                        env["csvViaHeaderOptions"]["end-date"] = "2098-12-31"
                    env["pathToClimateCSV"] = paths["local-path-to-archive" if use_local_paths else "cluster-path-to-archive"] \
                    + "climate/ipsl-cm5a-lr/" + rcp + "/" + rcp + "_" + str(clat) + "_" + str(clon) + ".csv"

                    env["customId"] = \
                    variety \
                    + "|" + str(lat) \
                    + "|" + str(lon) \
                    + "|" + str(rcp) \
                    + "|" + str(adaptation_option).replace("|", "/") 

                    socket.send_json(env) 
                    print "sent env ", sent_env_count, " customId: ", env["customId"]
                    sent_env_count += 1

    print "sending", sent_env_count, "envs took", (time.clock() - start_send), "seconds"
    

main()

