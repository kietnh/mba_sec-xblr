# Copyright 2014 Altova GmbH
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import feedparser
import os.path
import sys, getopt
import time
import socket
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import xml.etree.ElementTree as ET
from config import Config

from sqlalchemy import Table, Column, Integer, String, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

engine = create_engine(Config.DATABASE_URI, client_encoding='utf8')
if not database_exists(engine.url):
    create_database(engine.url)

class Item():
    @staticmethod
    def define(metadata):
        return Table('item', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('xblr', String),
                     Column('calculation', String),
                     Column('lab', String),
                     Column('presentation', String),
                     Column('reference', String),
                     Column('date', String))


metadata = MetaData()
item_table = Item.define(metadata)
metadata.create_all(engine)


def downloadfile(sourceurl, targetfname):
    mem_file = ""
    good_read = False
    xbrlfile = None
    if os.path.isfile(targetfname):
        print("Local copy already exists")
        return True
    else:
        print("Downloading:", sourceurl)
        try:
            xbrlfile = urlopen(sourceurl)
            try:
                mem_file = xbrlfile.read()
                good_read = True
            finally:
                xbrlfile.close()
        except HTTPError as e:
            print("HTTP Error:", e.code)
        except URLError as e:
            print("URL Error:", e.reason)
        except TimeoutError as e:
            print("Timeout Error:", e.reason)
        except socket.timeout:
            print("Socket Timeout Error")
        if good_read:
            output = open(targetfname, 'wb')
            output.write(mem_file)
            output.close()
        return good_read


def downloadfileAsString(sourceurl):
    mem_file = ""
    xbrlfile = None
    print("Downloading:", sourceurl)
    try:
        xbrlfile = urlopen(sourceurl)
        try:
            mem_file = xbrlfile.read().decode('utf-8')
        finally:
            xbrlfile.close()
    except HTTPError as e:
        print("HTTP Error:", e.code)
    except URLError as e:
        print("URL Error:", e.reason)
    except TimeoutError as e:
        print("Timeout Error:", e.reason)
    except socket.timeout:
        print("Socket Timeout Error")

    return mem_file


def SECdownload(year, month):
    connect = engine.connect()

    root = None
    feedFile = None
    feedData = None
    good_read = False
    itemIndex = 0
    output = ""
    edgarFilingsFeed = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-' + str(year) + '-' + str(month).zfill(
        2) + '.xml'
    print(edgarFilingsFeed)
    if not os.path.exists("sec/" + str(year)):
        os.makedirs("sec/" + str(year))
    if not os.path.exists("sec/" + str(year) + '/' + str(month).zfill(2)):
        os.makedirs("sec/" + str(year) + '/' + str(month).zfill(2))
    target_dir = "sec/" + str(year) + '/' + str(month).zfill(2) + '/'
    try:
        feedFile = urlopen(edgarFilingsFeed)
        try:
            feedData = feedFile.read()
            good_read = True
        finally:
            feedFile.close()
    except HTTPError as e:
        print("HTTP Error:", e.code)
    except URLError as e:
        print("URL Error:", e.reason)
    except TimeoutError as e:
        print("Timeout Error:", e.reason)
    except socket.timeout:
        print("Socket Timeout Error")
    if not good_read:
        print("Unable to download RSS feed document for the month:", year, month)
        return
    # we have to unfortunately use both feedparser (for normal cases) and ET for old-style RSS feeds,
    # because feedparser cannot handle the case where multiple xbrlFiles are referenced without enclosure
    try:
        root = ET.fromstring(feedData)
    except ET.ParseError as perr:
        print("XML Parser Error:", perr)
    feed = feedparser.parse(feedData)
    try:
        print(feed["channel"]["title"])
    except KeyError as e:
        print("Key Error:", e)
    # Process RSS feed and walk through all items contained


    for item in feed.entries:
        items = []

        try:
            # Identify ZIP file enclosure, if available
            enclosures = [l for l in item["links"] if l["rel"] == "enclosure"]

            if (len(enclosures) > 0):
                # ZIP file enclosure exists, so we can just download the ZIP file
                enclosure = enclosures[0]
                sourceurl = enclosure["href"]
                cik = item["edgar_ciknumber"]
                targetfname = target_dir + cik + '-' + sourceurl.split('/')[-1]
                retry_counter = 3
                while retry_counter > 0:
                    good_read = downloadfile(sourceurl, targetfname)
                    if good_read:
                        break
                    else:
                        print("Retrying:", retry_counter)
                        retry_counter -= 1
            else:
                # We need to manually download all XBRL files here and ZIP them ourselves...
                linkname = item["link"].split('/')[-1]
                linkbase = os.path.splitext(linkname)[0]
                cik = item["edgar_ciknumber"]
                zipfname = target_dir + cik + '-' + linkbase + "-xbrl.zip"

                # print ("linkname is %s" % linkname)
                # print ("linkbase is %s" % linkbase)
                # print ("cik is %s" % cik)
                # print ("zipfname is %s" % zipfname)


                edgarNamespace = {'edgar': 'http://www.sec.gov/Archives/edgar'}
                currentItem = list(root.iter("item"))[itemIndex]
                xbrlFiling = currentItem.find("edgar:xbrlFiling", edgarNamespace)
                xbrlFilesItem = xbrlFiling.find("edgar:xbrlFiles", edgarNamespace)
                xbrlFiles = xbrlFilesItem.findall("edgar:xbrlFile", edgarNamespace)

                date = str(month) + "/" + str(year)
                calculation = ""
                lab = ""
                presentation = ""
                reference = ""
                xblr = ""

                for xf in xbrlFiles:
                    xfurl = xf.get("{http://www.sec.gov/Archives/edgar}url")

                    if xfurl.endswith((".xml")):
                        filename = xfurl.split('/')[-1]
                        filename_without_extension = filename.split('.')[0]

                        retry_counter = 3
                        while retry_counter > 0:
                            output = downloadfileAsString(xfurl)
                            if output != "":
                                break
                            else:
                                print("Retrying:", retry_counter)
                                retry_counter -= 1

                        if (filename_without_extension.endswith("_cal")):
                            calculation = output
                        elif (filename_without_extension.endswith("_def")):
                            reference = output
                        elif (filename_without_extension.endswith("_lab")):
                            lab = output
                        elif (filename_without_extension.endswith("_pre")):
                            presentation = output
                        else:
                            xblr = output

                    # print ("cal = %s" % calculation)
                    # print ("xblr = %s" % xblr)
                    # print ("presentation = %s" % presentation)

                    ins = item_table.insert().values(xblr = xblr, calculation = calculation, lab = lab, presentation = presentation, reference = reference, date = date)
                    connect.execute(ins)


        except KeyError as e:
            print("Key Error:", e)
        finally:
            print("----------")
        itemIndex += 1

    connect.close()


def main(argv):
    year = 2013
    month = 1
    from_year = 1999
    to_year = 1999
    year_range = False
    if not os.path.exists("sec"):
        os.makedirs("sec")
    socket.setdefaulttimeout(10)
    start_time = time.time()
    try:
        opts, args = getopt.getopt(argv, "hy:m:f:t:", ["year=", "month=", "from=", "to="])
    except getopt.GetoptError:
        print('loadSECfilings -y <year> -m <month> | -f <from_year> -t <to_year>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('loadSECfilings -y <year> -m <month> | -f <from_year> -t <to_year>')
            sys.exit()
        elif opt in ("-y", "--year"):
            year = int(arg)
        elif opt in ("-m", "--month"):
            month = int(arg)
        elif opt in ("-f", "--from"):
            from_year = int(arg)
            year_range = True
        elif opt in ("-t", "--to"):
            to_year = int(arg)
            year_range = True
    if year_range:
        if from_year == 1999:
            from_year = to_year
        if to_year == 1999:
            to_year = from_year
        for year in range(from_year, to_year + 1):
            for month in range(1, 12 + 1):
                SECdownload(year, month)
    else:
        SECdownload(year, month)
    end_time = time.time()
    print("Elapsed time:", end_time - start_time, "seconds")


if __name__ == "__main__":
    main(sys.argv[1:])
