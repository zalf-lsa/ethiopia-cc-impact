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
        "include-file-base-path": "C:/Users/fikadu/Documents/GitHub",
        "local-path-to-archive": "Z:/data/ethiopia/",
        "local-path-to-repository": "C:/Users/fikadu/Documents/GitHub/ethiopia-cc-impact/",
        "cluster-path-to-archive": "/archiv-daten/md/data/ethiopia/"
    },
    "stella": {
        "include-file-base-path": "C:/Users/stella/Documents/GitHub",
        "local-path-to-archive": "Z:/data/ethiopia/",
        "local-path-to-repository": "C:/Users/stella/Documents/GitHub/ethiopia-cc-impact/",
        "cluster-path-to-archive": "/archiv-daten/md/data/ethiopia/"
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
        "server": "cluster1",
        "user": "fikadu",
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
        #"baseline",
        "rcp2p6",
        #"rcp4p5",
        #"rcp6p0",
        #"rcp8p5"
    ]

    sorghum_varieties = ["meko"] #, "teshale"]

    wgs84 = Proj(init="epsg:4326")
    utm37n = Proj(init="epsg:20137")

    def create_crop_probability_interpolator(path_to_csv_file, wgs84, utm37n):
        "create interpolation object from some dir with climate data"
        points = []
        values = []

        with open(path_to_csv_file) as _:
            reader = csv.reader(_)
            reader.next()
            for line in reader:
                lon = float(line[5])
                lat = float(line[6])
                prob = float(line[7])

                r, h = transform(wgs84, utm37n, lon, lat)
                #xlon, xlat = transform(utm37n, wgs84, r, h)
                points.append([r, h])
                values.append(prob)

        return NearestNDInterpolator(np.array(points), np.array(values))

    interpol_crop_prob = create_crop_probability_interpolator(paths["local-path-to-archive"] + "Ethiopia_crop_land_prob.csv", wgs84, utm37n)

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
            points.append([r, h])
            values.append((lat, lon))
            #print "lat:", lat, "lon:", lon, "h:", h, "r:", r, "val:", values[len(values)-1]

        return NearestNDInterpolator(np.array(points), np.array(values))

    
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
                points.append([r, h])
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
        "mineral-fertilization":  env["cropRotation"].pop("mineral-fertilization"),
        "NDemand-fertilization":  env["cropRotation"].pop("NDemand-fertilization"),
        "automatic-harvest":  env["cropRotation"].pop("automatic-harvest"),
        "cultivation-method": env["cropRotation"].pop("cultivation-method")
    }
    env["cropRotation"] = []
    

    adaptation_options = []
    for sowing in [#"recommended/dynamic-elevation-onsets", 
                    "calculated-onsets",
                    #"recommended/avg-static-elevation-onsets"
                    ]:
        for n_fert in ["recommended",
                        #"targetN"
                        #"NDemand_20",
                        #"NDemand_30",
                        #"NDemand_40",
                        #"NDemand_50",
                        #"NDemand_60",
                        #"NDemand_70",
                        #"NDemand_80",
                        #"NDemand_90",
                        #"NDemand_100",
                        ]:#, "auto"]:
            for cycle_length in ["standard"]:#, "longer"]:
                adaptation_options.append({
                    "sowing": sowing,
                    "fertilizer": n_fert,
                    "cycle-length": cycle_length
                })

    elevation_ranges = {
        "<1600": { 
            "onsets": {"from": date(2017, 6, 10), "to": date(2017, 6, 30)},
            "plant-density": {"from": 8, "to": 13},
            "fertilizer": {"N": 46},
            "target_soilN": 60
        },
        "=>1600&<=1900": { 
            "onsets": {"from": date(2017, 5, 1), "to": date(2017, 5, 15)},
            "plant-density": {"from": 9, "to": 12},
            "fertilizer": {"N": 50},
            "target_soilN": 60
        },
        ">1900": { 
            "onsets": {"from": date(2017, 4, 15), "to": date(2017, 5, 10)},
            "plant-density": {"from": 7, "to": 10},
            "fertilizer": {"N": 57},
            "target_soilN": 70
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
        return (date(2017, 1, 1) + timedelta(days=avg_doy-1)).strftime("0000-%m-%d")
    


    start_send = time.clock()
    sent_env_count = 0

    for rcp in rcps:

        onsets = read_onset_dates(paths["local-path-to-archive"] + "onset-dates/" + rcp + ".csv")

        for adaptation_option in adaptation_options:
            
            for variety in sorghum_varieties:

                for (lat, lon), profile in profiles.iteritems():

                    sr, sh = transform(wgs84, utm37n, lon, lat)
                    prob = interpol_crop_prob(sr, sh)
                    if prob < 40:
                        continue

                    (clat, clon) = interpol_climate(sr, sh)
                    slope = interpol_slope(sr, sh)
                    elevation = interpol_elevation(sr, sh)

                    env["params"]["siteParameters"]["SoilProfileParameters"] = profile
                    env["params"]["siteParameters"]["Latitude"] = lat
                    env["params"]["siteParameters"]["Slope"] = slope
                    env["params"]["siteParameters"]["HeightNN"] = elevation

                    # set fertilization
                    fertilizations = []
                    if adaptation_option["fertilizer"] == "recommended":
                        fert = elevation_range(elevation)["fertilizer"]
                        templates["mineral-fertilization"][0]["amount"][0] = float(fert["N"]) /2
                        templates["mineral-fertilization"][1]["amount"][0] = float(fert["N"]) /2
                        fertilizations = templates["mineral-fertilization"]
                    elif "NDemand" in adaptation_option["fertilizer"]:
                        #this strategy should be used to design adaptation options
                        Ndem = float(adaptation_option["fertilizer"].split("_")[1])
                        templates["NDemand-fertilization"][0]["N-demand"][0] = Ndem
                        templates["NDemand-fertilization"][1]["N-demand"][0] = Ndem
                        fertilizations = templates["NDemand-fertilization"]
                    elif "targetN" in adaptation_option["fertilizer"]:
                        target_Ndem = elevation_range(elevation)["target_soilN"]
                        templates["NDemand-fertilization"][0]["N-demand"][0] = target_Ndem
                        templates["NDemand-fertilization"][1]["N-demand"][0] = target_Ndem
                        fertilizations = templates["NDemand-fertilization"]

                    # set cycle length
                    if adaptation_option["cycle-length"] == "standard":
                        print "cycle length standard: to be done"
                    elif adaptation_option["plant-density"] == "increased":
                        print "cycle length increased: to be done"

                    # insert static sowing
                    if adaptation_option["sowing"] == "recommended/avg-static-elevation-onsets":
                        templates["static-sowing"]["crop"] = templates[variety]
                        templates["static-sowing"]["date"] = avg_static_elevation_onsets(elevation)
                        templates["cultivation-method"]["worksteps"] = [templates["static-sowing"]] + fertilizations + [templates["automatic-harvest"]]
                        env["cropRotation"] = [templates["cultivation-method"]]

                    elif adaptation_option["sowing"] == "recommended/dynamic-elevation-onsets":
                        templates["automatic-sowing"]["crop"] = templates[variety]
                        dates = elevation_range(elevation)["onsets"]
                        templates["automatic-sowing"]["earliest-date"] = dates["from"].strftime("0000-%m-%d")
                        templates["automatic-sowing"]["latest-date"] = dates["to"].strftime("0000-%m-%d")
                        templates["cultivation-method"]["worksteps"] = [templates["automatic-sowing"]] + fertilizations + [templates["automatic-harvest"]]
                        env["cropRotation"] = [templates["cultivation-method"]]

                    elif adaptation_option["sowing"] == "calculated-onsets":
                        year_to_onset = onsets[(clat, clon)]
                        templates["static-sowing"]["crop"] = templates[variety]
                        templates["cultivation-method"]["worksteps"] = [templates["static-sowing"]] + fertilizations + [templates["automatic-harvest"]]
                        env["cropRotation"] = []
                        for year in sorted(year_to_onset.keys()):
                            cm = copy.deepcopy(templates["cultivation-method"])
                            onset_date = (date(2017, 1, 1) + timedelta(days=year_to_onset[year]-1)).strftime("0000-%m-%d")
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
                    + "|" + adaptation_option["sowing"] \
                    + "|" + adaptation_option["fertilizer"] \
                    + "|" + adaptation_option["cycle-length"] \
                    + "|" + str(elevation) \

                    socket.send_json(env) 
                    print "sent env ", sent_env_count, " customId: ", env["customId"]
                    sent_env_count += 1

    print "sending", sent_env_count, "envs took", (time.clock() - start_send), "seconds"
    

main()

