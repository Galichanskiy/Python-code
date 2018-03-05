#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: RGalichanskiy
"""

import asyncio
from aiohttp import ClientSession
import time
from lxml.html import fromstring
from collections import namedtuple
from openpyxl import Workbook 
import re


# creating custom named tuple for caching the results
coin = namedtuple('coin', 'Numb Name Symbol MarketCap Price CirculatingSupply Volume24h c1h c24h c7d')
        
# creating xlsx-book
wb = Workbook()

# Creating url variable 
url = 'https://coinmarketcap.com/all/views/all/'
       
# request-loop
async def fetch(client, url):
    async with client.get(url) as response:
        assert response.status == 200
        return await response.read()

# substructing values for excel
async def substruct(content):
    tds = [fieldFilter(_.text_content().strip()) for _ in content]
    coins_item = coin(*tds)
    ws.append(coins_item)
    await asyncio.sleep(0)
    return ws

# filter
pattern_price = re.compile(r'([\d.,]+)')
pattern_percent = re.compile(r'([\d.,-]+)')

def fieldFilter(field):
    if '?' in field:
        field = 0.0
    elif 'Vol' in field:
        field = 0
    elif '...' in field:
        field = field
    elif '%' not in field:
        amount = re.findall(pattern_price, field)
        if amount:
            if ',' in amount[0]:
                field = float(amount[0].replace(',', ''))
            elif amount[0] == '.':
                field = field
            else:
                field = float(amount[0])
        else:
            field = field
    elif '%' in field:
        amount = re.findall(pattern_percent, field)
        if amount:
            field = float(amount[0])
        else:
            field = 0.0
    return field


# main loop
async def pars_response(url):
    tasks = []
    async with ClientSession() as client:
        response = await fetch(client, url)
        html = fromstring(response)
        trs = html.xpath('//tbody/tr')
        tr = [substruct(content) for content in trs]
        tasks.extend(tr)
        await asyncio.gather(*tasks)
        return  

if __name__ == '__main__':
    # activating worksheet
    ws = wb.active
    
    # creating loop
    loop = asyncio.get_event_loop()
   
    try:    
        # Starting timing
        t0 = time.time()
        
        #Looping events      
        loop.run_until_complete(pars_response(url))
        
        #the Time spent to complete events
        t1 = time.time()
        print("> Getting parsed took -> {0:.1f} seconds".format(t1-t0))
        
        # Saving 'xlsx'-file, printing the time spent on it 
        wb.save('coins.xlsx')
        t2 = time.time()
        print("> Parsing and writing to file tooks -> {0:.1f} seconds".format(t2-t0))

    finally:

        loop.close()
