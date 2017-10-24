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

USER = "berg-xps15"

PATHS = {
    "hampf": {
        "INCLUDE_FILE_BASE_PATH": "C:/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/md/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },

    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },

    "berg-xps15": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "out/"
    },
    "berg-lc": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD.000/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD.000/Documents/GitHub/carbiocial-2017/",
        "LOCAL_PATH_TO_OUTPUT_DIR": "G:/carbiocial-2017-out/"
    }
}

def create_output(result):
    "create output structure for single run"

    cm_count_to_crop_to_vals = defaultdict(lambda: defaultdict(dict))
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

                if "CM-count" not in vals or "Crop" not in vals:
                    print "Missing CM-count or Crop in result section. Skipping results section."
                    continue

                cm_count_to_crop_to_vals[vals["CM-count"]][vals["Crop"]].update(vals)

    return cm_count_to_crop_to_vals

def create_daily_avg_output_2(result, col):
    "create output structure for single run"
    
    # store for globrads
    crop_to_sowing_doys = defaultdict(list)
    crop_to_avg_sowing_doy = {}
    crop_to_harvest_doys = defaultdict(list)
    crop_to_avg_harvest_doy = {}
    crop_to_list_of_globrads_avg = defaultdict(lambda: defaultdict(list))

    # store for LAIs
    cm_count_to_sowing_doy = {}
    cm_count_to_harvest_doy = {}
    crop_to_list_of_LAIs = defaultdict(lambda: defaultdict(list))
    crop_to_list_of_globrads = defaultdict(lambda: defaultdict(list))


    if len(result.get("data", [])) > 0 and len(result["data"][0].get("results", [])) > 0:

        prev_orig_spec = None
        for data in result.get("data", []):

            results = data.get("results", [])
            orig_spec = data.get("origSpec", "")
            oids = data.get("outputIds", [])

            # after switch from crop section to daily section, calc the average doys for globrad
            if prev_orig_spec != None and prev_orig_spec != orig_spec:
                for crop, sdoys in crop_to_sowing_doys.iteritems():
                    crop_to_avg_sowing_doy[crop] = sum(sdoys) / len(sdoys)
                for crop, sdoys in crop_to_harvest_doys.iteritems():
                    crop_to_avg_harvest_doy[crop] = sum(sdoys) / len(sdoys)

            prev_orig_spec = orig_spec

            #skip empty results, e.g. when event condition haven't been met
            if len(results) == 0:
                continue

            prev_cm_count = None
            next_sowing_year = {}
            next_harvest_year = {}
            count_avg = 0
            count = 0

            # iterate over whole section, either all croping seasons or all daily results
            assert len(oids) == len(results)
            for kkk in range(0, len(results[0])):
                vals = {}

                # create one row of information
                for iii in range(0, len(oids)):
                    oid = oids[iii]
                    val = results[iii][kkk]

                    name = oid["name"] if len(oid["displayName"]) == 0 else oid["displayName"]

                    if isinstance(val, types.ListType):
                        for val_ in val:
                            vals[name] = val_
                    else:
                        vals[name] = val

                if orig_spec == '"crop"':
                    s_year = vals["s-year"]
    	            h_year = vals["h-year"]

                    crop = vals["Crop"]

                    next_sowing_year[crop] = next_sowing_year[crop] + 1 if crop in next_sowing_year else vals["s-year"]
                    next_harvest_year[crop] = next_harvest_year[crop] + 1 if crop in next_harvest_year else vals["h-year"]

                    s_delta = (s_year - next_sowing_year[crop]) * 365 
                    h_delta = (h_year - next_harvest_year[crop]) * 365

                    crop_to_sowing_doys[crop].append(vals["sowing"] + s_delta)
                    crop_to_harvest_doys[crop].append(vals["harvest"] + h_delta)
                    cm_count_to_sowing_doy[vals["CM-count"]] = vals["sowing"]
                    cm_count_to_harvest_doy[vals["CM-count"]] = vals["harvest"]
                    
                elif orig_spec == '"daily"':
                    doy = vals["DOY"]
                    crop = vals["Crop"]
                    cm_count = vals["CM-count"]

                    days_in_year = date(vals["Year"], 12, 31).timetuple().tm_yday

                    if len(crop) > 0:
                        # collect globrads avg
                        avg_sowing_doy = crop_to_avg_sowing_doy[crop]
                        if avg_sowing_doy < 1:
                            avg_sowing_doy += 365
                        avg_harvest_doy = crop_to_avg_harvest_doy[crop]
                        if avg_harvest_doy > days_in_year:
                            avg_havest_doy -= 365

                        if avg_sowing_doy <= doy and doy <= avg_harvest_doy:
                            if prev_cm_count > 0 and prev_cm_count != cm_count:
                                count_avg = 0
                            crop_to_list_of_globrads_avg[crop][count_avg].append(vals["Globrad"])
                            count_avg += 1

                        # collect LAIs and globrads
                        sowing_doy = cm_count_to_sowing_doy[cm_count]
                        harvest_doy = cm_count_to_harvest_doy[cm_count]

                        if sowing_doy <= doy and doy <= harvest_doy:
                            if prev_cm_count > 0 and prev_cm_count != cm_count:
                                count = 0
                            crop_to_list_of_LAIs[crop][count].append(vals["LAI"])
                            crop_to_list_of_globrads[crop][count].append(vals["Globrad"])
                            count += 1

                    prev_cm_count = cm_count

                if "CM-count" not in vals or "Crop" not in vals:
                    print "Missing CM-count or Crop in result section. Skipping results section."
                    continue

            
        # average globrads and LAIs
        out = ""
        count = 0
        for crop in sorted(crop_to_list_of_globrads.keys()):
            list_of_globrads_avg = crop_to_list_of_globrads_avg[crop]
            list_of_LAIs = crop_to_list_of_LAIs[crop]
            list_of_globrads = crop_to_list_of_globrads[crop]

            for das in xrange(max(map(len, [list_of_globrads_avg, list_of_LAIs, list_of_globrads]))):
                avg_globrad_avg = -9999
                if das < len(list_of_globrads_avg) and len(list_of_globrads_avg[das]) > 0:
                    avg_globrad_avg = sum(list_of_globrads_avg[das]) / len(list_of_globrads_avg[das])
                avg_lai = -9999
                if das < len(list_of_LAIs) and len(list_of_LAIs[das]) > 0:
                    avg_lai = sum(list_of_LAIs[das]) / len(list_of_LAIs[das])
                avg_globrad = -9999
                if das < len(list_of_globrads) and len(list_of_globrads[das]) > 0:
                    avg_globrad = sum(list_of_globrads[das]) / len(list_of_globrads[das])
                
                out += str(col) + "," + str(crop) + "," + str(das) + "," + str(avg_globrad_avg) + "," + str(avg_globrad) + "," + str(avg_lai) + "\n"

    return out


def create_daily_avg_output(result, col):
    "create daily average LAI and global radiation output"

    glob_rad = defaultdict(lambda: defaultdict(list)) #crop, day, list of values in the period
    LAI = defaultdict(lambda: defaultdict(list))
    days_after_sowing = defaultdict()    

    for data_ in result.get("data", []):
        results = data_.get("results", [])
        orig_spec = data_.get("origSpec", "")
        output_ids = data_.get("outputIds", [])

        if orig_spec == unicode('"crop"'):
            crops = set(results[0])

        elif orig_spec == unicode('"daily"'):
            for index in range(len(results[0])):
                #store GlobRad and LAI from sowing to harvest (specific for each year)
                if results[0][index] == unicode(''):
                    for cp in crops:
                        days_after_sowing[cp] = 0
                else: 
                    cp = results[0][index]
                    days_after_sowing[cp] += 1
                    LAI[cp][days_after_sowing[cp]].append(results[1][index])
                    glob_rad[cp][days_after_sowing[cp]].append(results[2][index])

    avg_rows = ""
    for cp in crops:
        for das in sorted(glob_rad[cp].keys()):
            avg_rad = sum(glob_rad[cp][das]) / len(glob_rad[cp][das]) #np.array(glob_rad[cp][das]).mean()
            avg_LAI = sum(LAI[cp][das]) / len(LAI[cp][das]) #np.array(LAI[cp][das]).mean()
            avg_rows += str(col) + "," + str(cp) + "," + str(das) + "," + str(avg_rad) + "," + str(avg_LAI) + "\n"

    return avg_rows



def create_template_grid(path_to_file, n_rows, n_cols):
    "0=no data, 1=data"

    with open(path_to_file) as file_:
        for header in range(0, 6):
            file_.next()

        out = np.full((n_rows, n_cols), 0, dtype=np.int8)

        row = 0
        for line in file_:
            col = 0
            for val in line.split(" "):
                out[row, col] = 0 if int(val) == -9999 else 1
                col += 1
            row += 1

        return out


HEADER = """ncols         1928
nrows         2544
xllcorner     -9345.000000
yllcorner     8000665.000000
cellsize      900
NODATA_value  -9999
"""

def write_row_to_grids(row_col_data, row, insert_nodata_rows_count, template_grid, rotation, period):
    "write grids row by row"

    row_template = template_grid[row]
    rows, cols = template_grid.shape

    make_dict_dict_nparr = lambda: defaultdict(lambda: defaultdict(lambda: np.full((cols,), -9999, dtype=np.float)))

    output_grids = {
        "sowing": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "harvest": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        #"Year": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "s-year": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "h-year": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "Yield": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 2},
        "NDefavg": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDefavg": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "anthesis": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "matur": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        #"Nstress1": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef1": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        #"Nstress2": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef2": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        #"Nstress3": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef3": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        #"Nstress4": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef4": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        #"Nstress5": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef5": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        #"Nstress6": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "TraDef6": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "NFert": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "NLeach": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "PercolationRate": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "Nmin": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "SumNUp": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "length": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
        "avg-precip": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 4},
        "avg-tavg": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 1},
        "avg-tmax": {"data" : make_dict_dict_nparr(), "cast-to": "float", "digits": 1},
        "Tmax>=40": {"data" : make_dict_dict_nparr(), "cast-to": "int", "digits": 0},
    }

    # skip this part if we write just a nodata line
    if row in row_col_data:
        for col in xrange(0, cols):
            if row_template[col] == 1:
                if col in row_col_data[row]:
                    for cm_count, crop_to_data in row_col_data[row][col].iteritems():
                        for crop, data in crop_to_data.iteritems():
                            for key, val in output_grids.iteritems():
                                val["data"][cm_count][crop][col] = data.get(key, -9999)

    for key, y2c2d_ in output_grids.iteritems():
        
        key = key.replace(">=", "gt")

        y2c2d = y2c2d_["data"]
        cast_to = y2c2d_["cast-to"]
        digits = y2c2d_["digits"]
        if cast_to == "int":
            mold = lambda x: str(int(x))
        else:
            mold = lambda x: str(round(x, digits))

        for cm_count, c2d in y2c2d.iteritems():

            for crop, row_arr in c2d.iteritems():
            
                crop = crop.replace("/", "").replace(" ", "")
                path_to_file = PATHS[USER]["LOCAL_PATH_TO_OUTPUT_DIR"] + period + "/" + crop + "_in_" + rotation + "_" + key + "_" + str(cm_count) + ".asc"

                if not os.path.isfile(path_to_file):
                    with open(path_to_file, "w") as _:
                        _.write(HEADER)

                with open(path_to_file, "a") as _:

                    if insert_nodata_rows_count > 0:
                        for i in xrange(0, insert_nodata_rows_count):
                            rowstr = " ".join(map(lambda x: "-9999", row_template))
                            _.write(rowstr +  "\n")

                    rowstr = " ".join(map(lambda x: "-9999" if int(x) == -9999 else mold(x), row_arr))
                    _.write(rowstr +  "\n")
    
    if row in row_col_data:
        del row_col_data[row]


def write_grid_row_to_avg_file(row_col_data, row, rotation, period):
    "write grids row by row"

    if row in row_col_data:
        path_to_file = PATHS[USER]["LOCAL_PATH_TO_OUTPUT_DIR"] + period + "/" + str(row) + "_" + rotation + ".csv"

        if not os.path.isfile(path_to_file):
            with open(path_to_file, "w") as _:
                _.write("col, crop, das, Globrad_avg, Globrad, LAI\n")

        with open(path_to_file, "a") as _:
            for col, col_str in row_col_data[row].iteritems():
                _.write(col_str)

    if row in row_col_data:
        del row_col_data[row]



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

    write_normal_output_files = True
    
    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://" + config["server"] + ":" + config["port"])    
    socket.RCVTIMEO = 10000
    leave = False
    
    n_rows = 2544
    n_cols = 1928

    if not write_normal_output_files:  
        print("loading template for output...")
        template_grid = create_template_grid(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc", n_rows, n_cols)
        datacells_per_row = np.sum(template_grid, axis=1) #.tolist()
        print("load complete")

        period_to_rotation_to_data = defaultdict(lambda: defaultdict(lambda: {
            "row-col-data": defaultdict(dict),
            "datacell-count": datacells_per_row.copy(), 
            "insert-nodata-rows-count": 0,
            "next-row": int(config["start-row"])
        }))

        debug_file = open("debug.out", "w")

    while not leave:
        try:
            result = socket.recv_json(encoding="latin-1")
        except:
            #print "no activity on socket for ", (socket.RCVTIMEO / 1000.0), "s, trying to write final data"
            #for period, rtd in period_to_rotation_to_data.iteritems():
            #    print "period:", period
            #    for rotation, data in rtd.iteritems():
            #        print "rotation:", rotation
            #        while data["next-row"] in data["row-col-data"]:# and data["datacell-count"][data["next-row"]] == 0:
            #            print "row:", data["next-row"]
            #            write_row_to_grids(data["row-col-data"], data["next-row"], data["insert-nodata-rows-count"], template_grid, rotation, period)
            #            data["insert-nodata-rows-count"] = 0 # should have written the nodata rows for this period and 
            #            data["next-row"] += 1 # move to next row (to be written)
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            period = ci_parts[0]
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            rotation = ci_parts[3]

            data = period_to_rotation_to_data[period][rotation]
            debug_msg = "received work result " + str(i) + " customId: " + result.get("customId", "") \
            + " next row: " + str(data["next-row"]) + " cols@row to go: " + str(data["datacell-count"][row]) + "@" + str(row) #\
            #+ " rows unwritten: " + str(data["row-col-data"].keys()) 
            print debug_msg
            debug_file.write(debug_msg + "\n")

            #data["row-col-data"][row][col] = create_output(result)
            data["row-col-data"][row][col] = create_daily_avg_output(result, col)
            data["datacell-count"][row] -= 1

            while (data["next-row"] < n_rows and datacells_per_row[data["next-row"]] == 0) \
            or (data["next-row"] in data["row-col-data"] and data["datacell-count"][data["next-row"]] == 0):
                # if rows have been initially completely nodata, remember to write these rows before the next row with some data
                if datacells_per_row[data["next-row"]] == 0:
                    data["insert-nodata-rows-count"] += 1
                else:
                    #write_row_to_grids(data["row-col-data"], data["next-row"], data["insert-nodata-rows-count"], template_grid, rotation, period)
                    write_grid_row_to_avg_file(data["row-col-data"], data["next-row"], rotation, period)
                    debug_msg = "wrote " + rotation + " row: "  + str(data["next-row"]) + " next-row: " + str(data["next-row"]+1) + " rows unwritten: " + str(data["row-col-data"].keys())
                    print debug_msg
                    debug_file.write(debug_msg + "\n")
                    data["insert-nodata-rows-count"] = 0 # should have written the nodata rows for this period and 
                
                data["next-row"] += 1 # move to next row (to be written)

            i = i + 1
        
        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

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


            i = i + 1

    debug_file.close()

main()


