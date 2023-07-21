Real-time poll the OARnet statistics from https://gateway.oar.net/stats/api to update
a historical record of traffic at each of the selected interfaces.
Provide alerts of significant dropped traffic via email.
"""

import csv
from datetime import datetime
import json
from argparse import ArgumentParser
import os.path
import platform
from pytz import timezone
import requests
import statistics
import sys
sys.path.append(r'C:\Users\mgeist\Desktop\oarnet-2.0.4\OARNET-Geist-Original')
from time import sleep, time
from typing import Union
import cred, logErr, printIf

# Column index of each series in a record
epoch = 0					# time in epoch seconds
Client2Oar = 1				# Client to OARnet
Oar2Client = 2				# OARnet to Client
Client2OarDrops = 3			# Client to OARnet drops
Oar2ClientDrops = 4			# OARnet to Client drops
Subscription = 5			# The line for the subscription
Burst = 6					# The line for the burst
csvHeaders = ["Date", "Client2Oar", "Oar2Client", "Client2OarDrops",
            "Oar2ClientDrops", "Subscription", "Burst"]
attrIndex = {csvHeaders[i]: i for i in range(len(csvHeaders))}  # map name to index
attrs = {csvHeaders[i] for i in (Client2Oar, Oar2Client)} 	# for statistics

histLen = 3					# number of [weeks of] history values to keep in list
devsLen = 4					# number of consecutive samples to consider
hist = {}			        # {service:{attr:{weekMinute:[oldestVal, ..., newestVal]}}}
histFile = {} 			    # {service:csv_file, ...}
histReader = {} 		    # {service:csvReader, ...}
fmt = '%c'					# output date format
home_zone = timezone('US/Eastern')


def strftime(t: float, fmt: str = '%c') -> str:
    """Convert float timestamp to date-time string localized to hone_zone

    :param t:       epoch seconds
    :param fmt:     strftime format string
    :return:        t formatted as date-time string
    """
    return datetime.fromtimestamp(t, home_zone).strftime(fmt)


def strptime(date_str: str, fmt: str = '%c') -> float:
    """Convert date-time string, localized to home_zone, to epoch seconds

    :param date_str:    date-time string in hone_zone
    :param fmt:         format of date_str
    :return:            epoch seconds
    """
    return home_zone.localize(datetime.strptime(date_str, fmt)).timestamp()


def diag_str(r: requests.Response) -> str:
    """Print diagnostic details of a Requests Response.

    Parameters:
        r (Response):	the Response returned from requests.*
    """

    s = f"url={r.url}"
    s += f"Status_code={r.status_code}"
    s += f"headers={str(r.headers)}"
    try:
        s += f"json={str(r.json())[:1000]}"
    except json.decoder.JSONDecodeError:
        s += f"text={r.text[:1000]}"
    return s


def file_stats(devices: list, member: str, service: str, time_frame: int,
            sampling: int, consolidation: str) -> tuple:
    """Emulate oarNet_stats with input from existing csv file

    Parameters:
        devices (list):			list of device name strings
        member (str):			name of the OARnet member
        service (str):			one of "INTERNET", "I2", or "ONNET"
        time_frame (int):	minutes of data to return. {1440, 10080, or 40320}. i.e. Day, Week, or 4 weeks
        sampling (int):		sample period in seconds. {3600, or 300 if time_frame in {1440,10080}}
        consolidation (str): 	{"avg", or "max" if sampling==3600}
    Returns:					a dict of samples, or None iff error
        {x: [y1, ..., yn]}, errString
    """
    if histFile[service] is None: 	# File already at EOF and closed?
        return None, ''				# Yes, return EOF indication
    rec_cnt = 0						# max # records to return, or 0 for all
    result = {}					# build result here
    for rec in histReader[service]:
        rec[epoch] = strptime(rec[0])  # date text --> epoch seconds
        for i in range(1, len(rec)):  # for each data value
            x = float(rec[i])		# convert string to float ...
            if x == int(x):
                x = int(x)			# ... or int if integral
            rec[i] = x
        result[rec[epoch]] = rec[1:]
        rec_cnt -= 1
        if rec_cnt == 0:			# reached maximum number of records
            break					# break out of loop to return these
    else:							# reached EOF
        histFile[service].close() 	# close the input file
        histFile[service] = None 	# and remember that it is closed
        if len(result) > 0:			# Any accumulated records to return?
            return result, ''		# Yes. Return them
        return None, ''				# No. Return EOF
    return result, ''				# normal return of maxRec>0 records


def oarNet_stats(devices: list, member: str, service: str, time_frame: int,
                sampling: int, consolidation: str) -> tuple:
    """Collect statistics time series from the OARnet status frame API

    Parameters:
        devices (list):			list of device name strings
        member (str):			name of the OARnet member
        service (str):			one of "INTERNET", "I2", or "ONNET"
        time_frame (int):	minutes of data to return. {1440, 10080, or 40320}. i.e. Day, Week, or 4 weeks
        sampling (int):		sampling rate in seconds. {3600, or 300 if time_frame in {1440,10080)}
        consolidation (str): 	one of {"avg", or "max" if sampling == 3600}
    Returns:					a dict of samples or None iff error, error string
        {x: [y1, ..., yn]}, err_string
    """
    # construct the URL from the parameters
    url = '/'.join([r'https://gateway.oar.net/stats/api', '+'.join(devices), member,
                    service, str(time_frame), str(1), str(sampling), consolidation])
    headers = {'Authorization': 'token ' + OARnet_token}
    # headers={'Authorization': 'token 6d08dff966438adfac62df59374bcfc4a99ba50c'}
    try:
        results = requests.get(url, headers=headers)
    except requests.RequestException:
        return None, f"{sys.exc_info()[0]} {sys.exc_info()[1]}\n"  # report error
    # parse through the json response
    try:
        response = results.json()
        data = response['data']
    except requests.RequestException:  # No json in response
        return None, f"Response  is not JSON"
    except KeyError: 					# No data in response
        return None, f'No "data" in response\n' + diag_str(results)

    err_s = ''						# accumulate errors here for return to caller
    # Enter data into a dictionary of {x: [y1, ..., yn]}
    result = {}
    num_series = len(data)				# number of [{x,y},...] series
    # Each [y1, ..., yn] is initialized to [None, ..., None]
    init_val = list(None for i in range(num_series))

    for i in range(num_series):			# for each series ...
        try:
            vals = data[i]['values'] 	# ... in the response, a list
        except KeyError:
            err_s += f'No {service} "values" list for series[{i}]={str(data[i])[:100]}\n'
            continue
        for pair in vals:				# for each {x: val, y: val} in a series
            try:
                x = float(pair['x']) 	# return time as float
            except ValueError:
                err_s += f'non-numeric "x" (service) value in series(i) value={str(pair)[0:100]} dropped.\n'
                continue
            try:
                y = int_or_float(pair['y'])
            except ValueError:
                err_s += f'non-numeric "y" {service} value in series[{i}] value={str(pair)[0:100]} dropped.\n'
                continue
            if x not in result:			# if no sample is started yet for this x ...
                result[x] = init_val[:] 	# ... create it with all y=None
            result[x][i] = y			# fill-in y[i] for this element
    return result, err_s


def int_or_float(s: str):
    """Converts string to int or float, with possible ValueError."""
    x = float(s)  # convert string to float ...
    if x == int(x):
        x = int(x)  # ... or int iff no decimal
    return x


def statChange(service: str, rec: list, attr: str) -> Union[tuple, None]:
    """Calculate attribute value's std deviations from mean

    Parameters:
        service (str):		name of the service
        rec (list):			record
        attr (str):			attribute name of value in rec
    Returns:
        dev (float):		(value - mean)/stdDeviation
        txt (list):			[description, ...]
    """
    global hist
    scale = 1000000
    this_attr = hist[service][attr]
    week_minutes = int((rec[0]+30) % (7*24*60*60))/60 	# rounded minute of the week
    try:
        vals = this_attr[week_minutes] 	# list of recent values
    except KeyError:
        vals = this_attr[week_minutes] = []  # initialize to none
    y = rec[attrIndex[attr]]
    if len(vals) >= histLen:			# histLen saved values for this weekTime?
        mean = statistics.mean(vals) 	# Yes, analyse for changes
        # Anything that can happen, will. Disbelieve unreasonably low stdev
        stdev = max(0.2*mean, statistics.stdev(vals, mean))
        dev = (y-mean)/stdev
        del vals[0]						# remove oldest
        vals.append(y)					# append new
        devs = this_attr['devs'] 		# newest std deviations for this service attr
        txts = this_attr['txt']			# and associated text messages
        txt = f"{int(y/scale):,}={int(mean/scale):,}{dev:+5.1F}*{int(stdev/scale):,}"
        if len(devs) >= devsLen:		# sufficient consecutive samples?
            del devs[0]					# remove oldest
            devs.append(dev)			# append new
            del txts[0]					# remove oldest
            txts.append(txt)
            avg = statistics.mean(devs)
            if abs(avg) > args.deviations:
                t = txts[:]				# copy of txts
                devs.clear()			# prevent repeat reporting of these
                txts.clear()			# prevent repeat reporting of these
                return avg, t			# reportable change
        else:							# No, just append new deviation and message
            devs.append(dev)
            txts.append(txt)
        return None						# no reportable change
    else:
        vals.append(y)
        return None						# insufficient data for meaningful statistics


parser = ArgumentParser(description='''Periodically retrieve oarnet statistics.
Append new samples for each service to a csv file at pathname/service.csv''')
parser.add_argument('pathname')
parser.add_argument('--consolidation', action='store', default='avg',
                    choices=['avg', 'max'],
    help='Aggregation function. {avg or max}. Max may be used only with sampling==3600. Default=avg')
parser.add_argument('--deviations', action='store', type=float, default=None,
    help='Std deviations difference to trigger warning. e.g. 10. Default=None')
parser.add_argument('--devices', action='append',
    help='one or more devices. Default=yntww-r9.bb.oar.net akrnq-r5.core.oar.net')
parser.add_argument('--drops', action='store', type=int, default=50000,
    help='Email when bytes dropped per sample exceeds this value. Default=50000')
parser.add_argument('--emails', action='append',
    help='one or more email addresses to receive error messages. Default=user@machineDomain')
parser.add_argument('--history', action='store_true', default=False,
    help='Read and analyse the files at pathname, w/o output to files')
parser.add_argument('--member', action='store', default='KENT',
    help='Oarnet member name. Default=KENT')
parser.add_argument('--refresh', action='store', type=int, default=15,
    help='Minutes between GETs to refresh data. 0 for single poll. Default=15')
parser.add_argument('--sampling', action='store', type=int, default=300,
                    choices=[300, 3600],
    help='OARnet sample period in seconds. {300 (allowed only if time_frame<=10080) or 3600}. Default=300')
parser.add_argument('--services', action='append',
                    choices=['INTERNET', 'I2', 'ONNET'],
    help='one or more services to poll. Default=INTERNET I2 ONNET')
parser.add_argument('--time_frame', action='store', type=int, default=10080,
                    choices=[1440, 10080, 40320],
    help='Initial window minutes {1440, 10080, or 40320} to retrieve. I.e. day, week, or 4 weeks. Default=1080.')
parser.add_argument('--verbose', action='count', default=False,
                    help="increase diagnostic messages")
args = parser.parse_args()
# validate options values
if not args.devices:
    args.devices = ['yntww-r9.bb.oar.net', 'akrnq-r5.core.oar.net']
if not args.emails:
    args.emails = ['user@machineDomain']
print(f"logErr(...) will send email to {args.emails}")
logErr.logSubject = 'Oarnet statistics'
logErr.logToAddr = args.emails
if not args.services:
    args.services = ['INTERNET', 'I2', 'ONNET']
for service in args.services:
    hist[service] = {} 			        # {attr:{'devs':[oldestDev, ..., newestDev],
    # 'txt':[oldestTxt, ..., newestTxt], weekMinute:[oldestVal, ..., newestVal]}}
    for attr in attrs:				    # the "Client2Oar" and "Oar2Client" columns
        hist[service][attr] = {}        # {'devs':[oldestDev, ..., newestDev],
        # 'txt':[oldestTxt, ..., newestTxt], weekMinute:[oldestVal, ..., newestVal]}
        hist[service][attr]['devs'] = list()  # [oldestDev, ..., newestDev]
        hist[service][attr]['txt'] = list()  # [oldestTxt, ..., newestTxt]
if args.consolidation == 'max' and not args.sampling == 3600:
    print(f"--consolidation=max is valid only with --sampling=3600")
    sys.exit(1)
if args.sampling == 300 and args.time_frame > 10080:
    print(f"--sampling=300 is valid only when --time_frame is 1440 or 10080")
    sys.exit(1)

try:
    OARnet_token = cred.API_TOKEN
except KeyError:
    print(f"Failed to obtain credentials for {args.member}")
    sys.exit(1)
#OARnet_token = cred[1]			# credentials password is the Authorization token

if args.verbose:						# print options summary
    print(f"Retrieving {args.consolidation} values for {args.member}'s "
          + f"{str(args.devices)} devices.")
    print(f"Data for {str(args.services)} will be retrieved",
        "once." if args.refresh == 0 else f"every {args.refresh} minutes.")

getStats = file_stats if args.history else oarNet_stats  # input callable
learningStart = time() - (histLen+1)*7*24*60*60  # time for learning to start
# initialize prevMax[service] = max(timestamp) from records in pathname/service.csv
prevMax = {}
for service in args.services: 		    # for each service
    prevMax[service] = 0.0				# actual time, x, is always >0
    file_name = os.path.join(args.pathname, service) + '.csv'
    try:
        histFile[service] = csv_file = open(file_name, 'r', newline='')
        histReader[service] = csvReader = csv.reader(csv_file)
        first = True
        for rec in csvReader:			# read the csv file ...
            if first:					# ... except for header record
                first = False
                if args.history: 	    # input will be from existing file?
                    break				# only read the header, and leave file open
                continue
            try:
                recX = strptime(rec[epoch], '%c')
            except ValueError:
                logErr(f"Bad date {rec[epoch]} in {file_name} record={rec}. Record ignored.")
                continue
            prevMax[service] = max(prevMax[service], recX)
            if args.deviations is not None and recX > learningStart:  # within N week startup?
                for attr in attrs: 		# Yes, for each attribute
                    statChange(service, rec, attr)  # learn past statistics
        else:							# have read the entire file
            csv_file.close()
            print(args.verbose, f"Maximum date in {file_name} is "
                    + f"{prevMax[service]}={strftime(prevMax[service], fmt)}")
        # if options.history, the file is left open after the header for later reading
    except FileNotFoundError:
        histFile[service] = None 		# indicate at EOF
        print(args.verbose, f"No existing {file_name} file.",
            '' if args.history else 'Creating a new file.')
        try:
            csv_file = open(file_name, 'w', newline='')
        except FileNotFoundError:
            logErr(f"Could not create new file: {file_name}")
            sys.exit(1)
        csvWriter = csv.writer(csv_file)
        csvWriter.writerow(csvHeaders) 	# Write header record
        csv_file.close()

# forever (unless refresh==0 --> onetime, or history)
#   obtain a time_frame window of samples of each series
#   remove data times that were previously processed
#   append new samples to csv file
allEOF = False
while not allEOF:
    if args.refresh != 0 and not args.history:  # normal real-time processing?
        t = 60*args.refresh - (time() % (60*args.refresh))  # Yes
        sleep(10)#max(0.0, t))  			# sleep until refresh multiple
    allEOF = args.history			    # allEOF iff no service so far has records
    alerts = ''							# Initially no alerts to post
    totBytesDropped = 0					# total bytes dropped in this time window
    prefix_fmt = '{0} {1:'+f"{max(len(s) for s in args.services)+1}"+'}'
    for service in args.services: 	    # for each service:
        # retrieve a window of the 6 series for that service
        report, errString = getStats(args.devices, args.member, service,
                                     args.time_frame, args.sampling, args.consolidation)
        alerts += errString
        if report is None:				# error from retrieval?
            if len(errString) == 0: 	# nothing specific to report?
                alerts += 'No records returned from API'  # say something later
                continue                # nothing to process. wait until next sample time
            else:                       # specific error getting the report
                break					# Report and try again later.
        allEOF = args.history		    # at least one service had records
        # convert dict {x:rec, ...} to list [[x]+rec, ...] sorted by x
        recs = list([x]+report[x] for x in report)
        recs.sort()
        recsLen = len(recs)				# number of records input

        # delete records that were previously output
        i = 0
        for rec in recs:
            if len(rec) != len(csvHeaders):  # bad record?
                alerts += f"bad record {rec} ignored\n"
            elif rec[epoch] > prevMax[service]:  # unseen record?
                break					# Yes. First of new records
            i += 1
        recs = recs[i:]					# trim away previously written records
        print(args.verbose, f"{len(recs)} new {service} records from {recsLen}")

        # test each new record for alert conditions
        if not args.history:			# Writing an output file?
            file_name = os.path.join(args.pathname, service) + '.csv' 	# Yes.
        else:							# No. write to bit bucket
            file_name = r'\Device\Null' if platform.system() == 'Windows' \
                else '/dev/null'
        csv_file = open(file_name, 'a', newline='') 	# Open file for append
        csvWriter = csv.writer(csv_file)
        for rec in recs:
            if int((rec[epoch]-prevMax[service]+30)/60) != int(args.sampling/60) \
                    and prevMax[service] > 0:
                alerts += f"{strftime(rec[epoch], fmt)} {service:9} missing"\
                    + f" sample(s) since {strftime(prevMax[service], fmt)}\n"
            prevMax[service] = max(prevMax[service], rec[epoch])
            prefix = prefix_fmt.format(strftime(rec[epoch], fmt), service)
            try:
                s = []                  # list of drops
                # Documented as Burst - Drops, but data matches Drops - Burst
                for index in (Client2OarDrops, Oar2ClientDrops):
                    drops = rec[index] - rec[Burst]
                    if drops > 0:		# some bytes dropped?
                        totBytesDropped += drops
                        s.append(f"{drops:12,d} bytes of {csvHeaders[index]}")
                if len(s) > 0:			# drop(s) to report?
                    alerts += prefix + '  '.join(s) + '\n'
                if args.deviations is not None:  # Discontinuity?
                    for attr in attrs: 	# Predict value and notify
                        c = statChange(service, rec, attr)
                        if c is not None:  # discontinuity in attribute values?
                            totBytesDropped = 100000000000 	# Force output of warning
                            alerts += prefix + f"{attr}={c[0]:+6.1F}=[" + ', '.join(c[1]) + ']\n'
                rec[epoch] = strftime(rec[epoch], fmt)  # convert epoch to date string
                csvWriter.writerow(rec)  # append new record to output file
            except Exception:	    	# any error while processing the record
                alerts += f"error processing record {rec}. Record ignored"
        csv_file.close()				# close output file for this service

    args.time_frame = 1440			    # after 1st, use the minimum window size
    if len(alerts) > 0 and totBytesDropped > args.drops:  # Issues to report?
        logErr(alerts)
    if args.refresh == 0:			    # one poll only?
        break
for service in args.services: 		    # For each service
    if histFile[service] is not None:
        histFile[service].close() 		# Close the input file
        histFile[service] = None
