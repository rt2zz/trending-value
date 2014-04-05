This script scrapes the web and computes the trending value portfolio.

Tested with Python 2.6.6

Install
=======
Install python
Install pip
With pip install:
    requests, yql, BeautifulSoup

On ubuntu/debian linux:
===============================

sudo apt-get install python python-pip
sudo pip install requests yql BeautifulSoup

Execute:
========

Execute the script with the following command:

python trendingvalue.py

Generation takes 1-2 hours.
If it runs successfully it will generate a csv file. To complete the portfolio you need to pick the stocks with OVR rank 90 or higher, then reorder them by momentum. Th 25 or 50 stocks on top are your portfolio.
