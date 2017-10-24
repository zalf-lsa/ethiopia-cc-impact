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

    def create_interpolator(path_to_climate_dir, scenario, wgs84, utm37n):
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
    interpol = create_interpolator(paths["local-path-to-archive"] + "climate/ipsl-cm5a-lr/", "baseline", wgs84, utm37n)


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
           
    envs = []

    # pre-build envs for the two possible crops
    crop["cropRotation"][0]["worksteps"][0]["crop"][2] = "Meko"
    envs.append(monica_io.create_env_json_from_json_config({
        "crop": crop,
        "site": site,
        "sim": sim,
        "climate": ""
    }))

    crop["cropRotation"][0]["worksteps"][0]["crop"][2] = "Teshale"
    envs.append(monica_io.create_env_json_from_json_config({
        "crop": crop,
        "site": site,
        "sim": sim,
        "climate": ""
    }))

    start_send = time.clock()
    sent_env_count = 0

    rcp = rcps[0]

    for (lat, lon), profile in profiles.iteritems():

        sr, sh = transform(wgs84, utm37n, lon, lat)
        (clat, clon) = interpol(sh, sr)

        for env in envs:
            env["params"]["siteParameters"]["SoilProfileParameters"] = profile
            env["params"]["siteParameters"]["Latitude"] = lat

            #set climate file - read by the server
            env["csvViaHeaderOptions"] = sim["climate.csv-options"]
            #env["csvViaHeaderOptions"]["start-date"] = sim["start-date"].replace("1981", str(p["start_year"]))
            #env["csvViaHeaderOptions"]["end-date"] = sim["end-date"].replace("2012", str(p["end_year"]))
            #note that the climate file content is csv like, despite the extension .asc
            env["pathToClimateCSV"] = paths["local-path-to-archive" if use_local_paths else "cluster-path-to-archive"] \
            + "climate/ipsl-cm5a-lr/" + rcp + "/" + rcp + "_" + str(clat) + "_" + str(clon) + ".csv"

            env["customId"] = \
            env["cropRotation"][0]["worksteps"][0]["crop"]["cropParams"]["cultivar"]["CultivarName"] \
            + "|" + str(lat) \
            + "|" + str(lon) 

            socket.send_json(env) 
            print "sent env ", sent_env_count, " customId: ", env["customId"]
            sent_env_count += 1

    print "sending", sent_env_count, "envs took", (time.clock() - start_send), "seconds"
    

main()

