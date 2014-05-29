#! /usr/bin/python
import requests
import csv
import yql
import pdb
from datetime import datetime
from decimal import Decimal
import time
import re
import sys
import os
import multiprocessing
from BeautifulSoup import BeautifulSoup
import StringIO

stock_keys = [
    "Ticker",
    "Company",
    "MarketCap",
    "PE",
    "PS",
    "PB",
    "PFreeCashFlow",
    "DividendYield",
    "PerformanceHalfYear",
    "Price",
    "BB",
    "EVEBITDA",
    "BBY",
    "SHY",
    "PERank",
    "PSRank",
    "PBRank",
    "PFCFRank",
    "SHYRank",
    "EVEBITDARank",
    "Rank",
    "OVRRank"
]

def generate_snapshot_to_csv():
    data = {}
    generate_snapshot(data)
    to_csv(data)


def generate_snapshot(data):
    print "Creating new snapshot"
    import_finviz(data)
    import_evebitda(data)
    import_buyback_yield(data, True)
    compute_rank(data)
    return data

def import_finviz(processed_data):
    print "Importing data from finviz"
    # not using f=cap_smallover since it filters market caps over 300M instead of 200M
    #r = requests.get('http://finviz.com/export.ashx?v=152', cookies={"screenerUrl": "screener.ashx?v=152&f=cap_smallover&ft=4", "customTable": "0,1,2,6,7,10,11,13,14,45,65"})
    r = requests.get('http://finviz.com/export.ashx?v=152', cookies={"screenerUrl": "screener.ashx?v=152&ft=4", "customTable": "0,1,2,6,7,10,11,13,14,45,65"})
    data = csv_to_dicts(r.text)
    tickers = []
    for row in data:
        try:
            stock = {}
            if row["Ticker"]:
                stock["Ticker"] = row["Ticker"]
            print stock["Ticker"]
            if "Importing " + row["Company"]:
                stock["Company"] = row["Company"]
            # Ignore companies with market cap below 200M
            if not "Market Cap" in row or row["Market Cap"] == "":
                continue
            market_cap = Decimal(row["Market Cap"])
            if market_cap < 200:
                print "Market Cap too small: "+ row["Market Cap"]
                continue
            stock["MarketCap"] = row["Market Cap"]
            if row["P/E"]:
                stock["PE"] = row["P/E"]
            if row["P/S"]:
                stock["PS"] = row["P/S"]
            if row["P/B"]:
                stock["PB"] = row["P/B"]
            if row["P/Free Cash Flow"]:
                stock["PFreeCashFlow"] = row["P/Free Cash Flow"]
            if row["Dividend Yield"]:
                stock["DividendYield"] = row["Dividend Yield"][:-1]
            if row["Performance (Half Year)"]:
                stock["PerformanceHalfYear"] = row["Performance (Half Year)"][:-1]
            if row["Price"]:
                stock["Price"] = row["Price"]
            processed_data[stock["Ticker"]] = stock
        except Exception as e:
            print e
            #pdb.set_trace()
    print "Finviz data imported"

def import_evebitda(data):
    print "Importing EV/EBITDA"
    y = yql.Public()
    step=100
    tickers = data.keys()
    for i in range(0,len(tickers),step):
        print "From " + tickers[i] + " to " + tickers[min(i+step,len(tickers))-1]
        nquery = 'select symbol, EnterpriseValueEBITDA.content from yahoo.finance.keystats where symbol in ({0})'.format('"'+('","'.join(tickers[i:i+step-1])+'"'))
        ebitdas = y.execute(nquery, env="http://www.datatables.org/alltables.env")
        if ebitdas.results:
            for row in ebitdas.results["stats"]:
                print row["symbol"]
                if "EnterpriseValueEBITDA" in row and row["EnterpriseValueEBITDA"] and row["EnterpriseValueEBITDA"] != "N/A":
                    print "EVEBITDA: " + row["EnterpriseValueEBITDA"]
                    data[row["symbol"]]["EVEBITDA"] = row["EnterpriseValueEBITDA"]
        else:
            pass
            print "No results"
    print "EV/EBITDA imported"

def import_single_buyback_yield(stock):
    done = False
    while not done:
        try:
            print stock["Ticker"]
            if not stock["MarketCap"]: break
            query = "http://finance.yahoo.com/q/cf?s="+stock["Ticker"]+"&ql=1"
            print query
            r = requests.get(query, timeout=5)
            html = r.text
            # Repair html
            html = html.replace('<div id="yucs-contextual_shortcuts"data-property="finance"data-languagetag="en-us"data-status="active"data-spaceid=""data-cobrand="standard">', '<div id="yucs-contextual_shortcuts" data-property="finance" data-languagetag="en-us" data-status="active" data-spaceid="" data-cobrand="standard">')
            html = re.sub(r'(?<!\=)"">', '">', html)
            soup = BeautifulSoup(html)
            #with open("html.html", "w") as f:
            #    f.write(html)
            #with open("file.html", "w") as f:
            #    f.write(soup.prettify())
            table = soup.find("table", {"class": "yfnc_tabledata1"})
            if not table: break
            table = table.find("table")
            if not table: break
            sale = 0
            for tr in table.findAll("tr"):
                title = tr.td.renderContents().strip()
                if title == "Sale Purchase of Stock":
                    for td in tr.findAll("td")[1:]:
                        val = td.renderContents().strip()
                        val = val.replace("(", "-")
                        val = val.replace(",", "")
                        val = val.replace(")", "")
                        val = val.replace("&nbsp;", "")
                        val = val.replace("\n", "")
                        val = val.replace("\t", "")
                        val = val.replace("\\n", "")
                        val = val.replace(" ", "")
                        if val == "-": continue
                        sale += int(val)*1000
            stock["BB"] = -sale
            print "BB: "+str(stock["BB"])
            done = True
            #print "done!"
        except Exception as e:
            print e
            print "Trying again in 1 sec"
            time.sleep(1)

def import_buyback_yield(data, parallel=False):
    print "Importing Buyback Yield"
    if parallel:
        pool = multiprocessing.Pool(4)
        pool.map(import_single_buyback_yield, data.values())
    else:
        for stock in data:
            stock = data[stock]
            import_single_buyback_yield(stock)
    print "Completed Buyback Yield"

def compute_rank(data, step=0):
    if step == 0:
        compute_perank(data)
    if step <=1:
        compute_psrank(data)
    if step <=2:
        compute_pbrank(data)
    if step <=3:
        compute_pfcfrank(data)
    if step <=4:
        compute_bby(data)
    if step <=5:
        compute_shy(data)
    if step <=6:
        compute_shyrank(data)
    if step <=7:
        compute_evebitdarank(data)
    if step <=8:
        set_mediums(data)
    if step <=9:
        compute_stockrank(data)
    if step <=10:
        compute_overallrank(data)
    print "Done"

def compute_somerank(data, key, origkey=None, reverse=True, filterpositive=False):
    print "Computing " + key + " rank"
    if not origkey:
        origkey = key
    i = 0
    value = None
    stocks = sorted([stock for stock in data.values() if origkey in stock and (not filterpositive or stock[origkey] >= 0)], key=lambda k: k[origkey], reverse=reverse)
    amt = len(stocks)
    for stock in stocks:
        print stock["Ticker"]
        if stock[origkey] != value:
            last_rank = i
            value = stock[origkey]
        stock[key+"Rank"] = Decimal(last_rank)/amt*100
        print key+"Rank: " + str(stock[key+"Rank"])
        i +=1
    print "Computed " + key + " Rank"

def compute_perank(data):
    compute_somerank(data, "PE")

def compute_psrank(data):
    compute_somerank(data, "PS")

def compute_pbrank(data):
    compute_somerank(data, "PB")

def compute_pfcfrank(data):
    compute_somerank(data, "PFCF", "PFreeCashFlow")

def compute_bby(data):
    print "Computing BBY"
    for stock in [stock for stock in data.values() if "BB" in stock and "MarketCap" in stock]:
        print stock["Ticker"]
        stock["BBY"] = -Decimal(stock["BB"])/(Decimal(stock["MarketCap"])*1000000)*100
        print "BBY: " + str(stock["BBY"])
    print "Done computing BBY"

def compute_shy(data):
    print "Computing SHY"
    for stock in data.values():
        print stock["Ticker"]
        stock["SHY"] = 0
        if "DividendYield" in stock:
            stock["SHY"] += Decimal(stock["DividendYield"])
        if "BBY" in stock:
            stock["SHY"] += stock["BBY"]
        print "SHY: " + str(stock["SHY"])
    print "Done computing SHY"

def compute_shyrank(data):
    compute_somerank(data, "SHY", reverse=False)

def compute_evebitdarank(data):
    compute_somerank(data, "EVEBITDA", filterpositive=True)

def set_mediums(data):
    print "Setting Mediums"
    for stock in data.values():
        for key in ["PE", "PS", "PB", "PFCF", "EVEBITDA"]:
            if not key + "Rank" in stock:
                stock[key + "Rank"] = 50
            if "EVEBITDA" in stock and stock["EVEBITDA"] < 0:
                stock["EVEBITDARank"] = 50
    print "Done setting Mediums"

def compute_stockrank(data):
    print "Computing stock rank"
    for stock in data.values():
        print stock["Ticker"]
        stock["Rank"] = stock["PERank"]+stock["PSRank"]+stock["PBRank"]+stock["PFCFRank"]+stock["SHYRank"]+stock["EVEBITDARank"]
        print "Rank: " + str(stock["Rank"])

def compute_overallrank(data):
    print "Computing Overall rank"
    compute_somerank(data, "OVR", origkey="Rank", reverse=False)

def to_csv(data):
    date = datetime.now()
    datestr = date.strftime('%y-%m-%d--%H:%M')
    with open("snapshot-"+datestr+".csv", "wb") as f:
        w = csv.DictWriter(f, stock_keys)
        w.writer.writerow(stock_keys)
        w.writerows(data.values())

def csv_to_dicts(scsv):
    scsv = scsv.encode('ascii', 'ignore')
    reader = csv.reader(StringIO.StringIO(scsv))
    header = []
    res = []
    for row in reader:
        if header:
            data = {}
            for i,val in enumerate(row):
                data[header[i]] = val
            res.append(data)
        else:
            header = row
    return res[:50]

if __name__ == '__main__':
    generate_snapshot_to_csv()
