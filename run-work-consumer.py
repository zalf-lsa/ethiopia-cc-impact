#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
# Tommaso Stella <tommaso.stella@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import sys

#import json
import csv
import types
import os
from datetime import datetime, date, timedelta
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
import re
import numpy as np

PATHS = {
    "fikadu": {
        "local-path-to-output-dir": "out/"
    },

    "stella": {
        "local-path-to-output-dir": "out/"
    },

    "berg-xps15": {
        "local-path-to-output-dir": "out/"
    },
    "berg-lc": {
        "local-path-to-output-dir": "out/"
    }
}

def create_output(result, lat, lon):
    "create output structure for single run"

    year_to_vals = defaultdict(dict)
    out = []
    if len(result.get("data", [])) > 0 and len(result["data"][0].get("results", [])) > 0:

        for data in result.get("data", []):
            results = data.get("results", [])
            oids = data.get("outputIds", [])

            #skip empty results, e.g. when event condition haven't been met
            if len(results) == 0:
                continue

            assert len(oids) == len(results)
            for kkk in range(0, len(results[0])):
                vals = {}

                for iii in range(0, len(oids)):
                    oid = oids[iii]
                    val = results[iii][kkk]

                    name = oid["name"] if len(oid["displayName"]) == 0 else oid["displayName"]

                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[name] = val_
                    else:
                        vals[name] = val

                year_to_vals[vals["Year"]].update(vals)

    for year in sorted(year_to_vals):
        vals = year_to_vals[year]

        out.append([
            lat,
            lon,
            vals.get("Year", "NA"),
            vals.get("yield", "NA"),
            vals.get("applied-N", "NA"),
            vals.get("N-leaching", "NA"),
            vals.get("N-uptake", "NA"),
            vals.get("cycle-length", "NA"),
            vals.get("TraDef1", "NA"),
            vals.get("TraDef2", "NA"),
            vals.get("TraDef3", "NA"),
            vals.get("TraDef4", "NA"),
            vals.get("TraDef5", "NA"),
            vals.get("TraDef6", "NA"),
            vals.get("TraDef7", "NA")
        ])

    return out

def write_data(path_to_out_dir, rows, cultivar, rcp, sowing, fertilizer, plant_density, cycle_length):
    "write data"

    sowing_shortcut = "-".join(map(lambda x: x[:3], sowing.split("/")))
    path_to_file = path_to_out_dir + cultivar + "_" + rcp + "_s-" + sowing_shortcut \
    + "_f-" + fertilizer[:3] + "_p-" + plant_density[:3] + "_c-" + cycle_length[:3] + ".csv"

    if not os.path.isfile(path_to_file):
        with open(path_to_file, "w") as _:
            _.write("year, yield, applied-N, N-leaching, N-uptake, cycle-length, TraDef1, TraDef2, TraDef3, TraDef4, TraDef5, TraDef6, TraDef7\n")

    with open(path_to_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row in rows:
            writer.writerow(row)


def main():
    "collect data from workers"

    config = {
        "port": "7777",
        "server": "cluster3", #"10.10.26.34", #"cluster3",
        "user": "berg-lc"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v 

    paths = PATHS[config["user"]]

    write_normal_output_files = False
    
    received_envs_count = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])    
    socket.RCVTIMEO = 10000
    leave = False
    
    while not leave:
        try:
            result = socket.recv_json(encoding="latin-1")
        except:
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            cultivar = ci_parts[0]
            lat = ci_parts[1]
            lon = ci_parts[2]
            rcp = ci_parts[3]
            sowing = ci_parts[4]
            fertilizer = ci_parts[5]
            plant_density = ci_parts[6]
            cycle_length = ci_parts[7]

            print "received work result", received_envs_count, "customId:", result.get("customId", "")

            out = create_output(result, lat, lon)
            write_data(paths["local-path-to-output-dir"], out, cultivar, rcp, sowing, fertilizer, plant_density, cycle_length)

            received_envs_count = received_envs_count + 1
        
        elif write_normal_output_files:
            print "received work result ", received_envs_count, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            cultivar = ci_parts[0]
            lat = ci_parts[1]
            lon = ci_parts[2]

            #with open("out/out-" + str(i) + ".csv", 'wb') as _:
            with open("out/" + cultivar + "_lat_" + lat + "_lon_" + lon + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")
                
                for data_ in result.get("data", []):
                    results = data_.get("results", [])
                    orig_spec = data_.get("origSpec", "")
                    output_ids = data_.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec.replace("\"", "")])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                    include_header_row=True,
                                                                    include_units_row=True,
                                                                    include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            received_envs_count = received_envs_count + 1

main()


