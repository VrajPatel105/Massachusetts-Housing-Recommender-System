import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from datetime import datetime
import random
import re
import os
from zillow import MultiPropertyZillowScraper

if __name__ == "__main__":
    
    print("="*80)
    print("QUEUE-BASED MASSACHUSETTS ZILLOW SCRAPER - 10,000 PROPERTIES TARGET")
    print("="*80)
    
    # Get terminal/queue ID from environment variable (1-5)
    queue_id = int(os.getenv('QUEUE_ID', '1'))
    headless = os.getenv('HEADLESS', 'false').lower() == 'true'
    output_base_dir = os.getenv('OUTPUT_DIR', 'data')
    
    # UPDATED COUNTY QUEUES WITH PROPER URLS
    city_queues = {
    1: [("middlesex-county-ma", 1500, "https://www.zillow.com/middlesex-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-72.11103208691407%2C%22east%22%3A-70.80777891308594%2C%22south%22%3A42.013380592861715%2C%22north%22%3A42.87776941681747%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2801%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("cambridge-ma", 150, "https://www.zillow.com/cambridge-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.19366682336425%2C%22east%22%3A-71.03076017663574%2C%22south%22%3A42.32428902258168%2C%22north%22%3A42.43245615625035%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A3934%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("waltham-ma", 50, "https://www.zillow.com/waltham-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.32114432336425%2C%22east%22%3A-71.15823767663574%2C%22south%22%3A42.335530037769786%2C%22north%22%3A42.44367782271578%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A34644%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("arlington-ma", 40, "https://www.zillow.com/arlington-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.20841716168212%2C%22east%22%3A-71.12696383831786%2C%22south%22%3A42.38985647427385%2C%22north%22%3A42.44390687786365%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A43936%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A14%7D")],
    2: [("suffolk-county-ma", 800, "https://www.zillow.com/suffolk-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.40629566459278%2C%22east%22%3A-70.75466907767871%2C%22south%22%3A42.07344501265707%2C%22north%22%3A42.50671880749019%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2045%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%7D"),
        ("boston-ma", 700, "https://www.zillow.com/boston-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.21053164672853%2C%22east%22%3A-70.8847183532715%2C%22south%22%3A42.20505946340862%2C%22north%22%3A42.42161738476145%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A44269%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("revere-ma", 60, "https://www.zillow.com/revere-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.07780282336427%2C%22east%22%3A-70.91489617663575%2C%22south%22%3A42.36504188058792%2C%22north%22%3A42.47313884818802%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A54111%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D")],
    3: [("worcester-county-ma", 800, "https://www.zillow.com/worcester-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-72.54849508691407%2C%22east%22%3A-71.24524191308595%2C%22south%22%3A41.93157488769784%2C%22north%22%3A42.797083876551426%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2879%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("barnstable-county-ma", 700, "https://www.zillow.com/barnstable-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.96028908691406%2C%22east%22%3A-69.65703591308593%2C%22south%22%3A41.3620721131892%2C%22north%22%3A42.2353312747862%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2885%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("fitchburg-ma", 60, "https://www.zillow.com/fitchburg-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.97093164672852%2C%22east%22%3A-71.64511835327149%2C%22south%22%3A42.464669613877305%2C%22north%22%3A42.68033358684108%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A16567%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("marlborough-ma", 50, "https://www.zillow.com/marlborough-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.63211132336426%2C%22east%22%3A-71.46920467663574%2C%22south%22%3A42.29176775067483%2C%22north%22%3A42.39999083855605%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A19212%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D")],
    4: [("plymouth-county-ma", 800, "https://www.zillow.com/plymouth-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.35306908691408%2C%22east%22%3A-70.04981591308595%2C%22south%22%3A41.53481252077754%2C%22north%22%3A42.405729812422514%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2482%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("plymouth-ma", 250, "https://www.zillow.com/plymouth-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.96524029345703%2C%22east%22%3A-70.31361370654297%2C%22south%22%3A41.67073265744946%2C%22north%22%3A42.106756431768886%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A398673%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%7D"),
        ("falmouth-ma", 150, "https://www.zillow.com/falmouth-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.76050164672853%2C%22east%22%3A-70.4346883532715%2C%22south%22%3A41.480512018709426%2C%22north%22%3A41.699541409458114%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A398019%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("bourne-ma", 100, "https://www.zillow.com/bourne-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.7675581467285%2C%22east%22%3A-70.44174485327147%2C%22south%22%3A41.620252883865724%2C%22north%22%3A41.838808311376766%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A398346%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("sandwich-ma", 50, "https://www.zillow.com/sandwich-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.65287764672851%2C%22east%22%3A-70.32706435327148%2C%22south%22%3A41.605786926075154%2C%22north%22%3A41.82439147827071%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A397292%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D")],
    5: [("norfolk-county-ma", 800, "https://www.zillow.com/norfolk-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.74104658691407%2C%22east%22%3A-70.43779341308594%2C%22south%22%3A41.74482944132208%2C%22north%22%3A42.61288905433203%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A1948%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("newton-ma", 200, "https://www.zillow.com/newton-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.29613182336426%2C%22east%22%3A-71.13322517663575%2C%22south%22%3A42.27123493719186%2C%22north%22%3A42.37949333473135%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A40013%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("brookline-ma", 150, "https://www.zillow.com/brookline-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.22377282336427%2C%22east%22%3A-71.06086617663576%2C%22south%22%3A42.26905614545303%2C%22north%22%3A42.37731828898104%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A17188%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("medford-ma", 60, "https://www.zillow.com/medford-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.19253232336426%2C%22east%22%3A-71.02962567663575%2C%22south%22%3A42.37368665254081%2C%22north%22%3A42.481768729025994%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A53250%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("weymouth-ma", 50, "https://www.zillow.com/weymouth-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.11048464672851%2C%22east%22%3A-70.78467135327148%2C%22south%22%3A42.09335521219197%2C%22north%22%3A42.31029641892357%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A14603%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D")],
    6: [("essex-county-ma", 800, "https://www.zillow.com/essex-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.53074958691406%2C%22east%22%3A-70.22749641308593%2C%22south%22%3A42.20357091004269%2C%22north%22%3A43.065348791989685%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A503%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("franklin-county-ma", 150, "https://www.zillow.com/franklin-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.27579308691406%2C%22east%22%3A-71.97253991308594%2C%22south%22%3A42.08939774003212%2C%22north%22%3A42.95274411452706%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2281%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("lowell-ma", 100, "https://www.zillow.com/lowell-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.40699832336426%2C%22east%22%3A-71.24409167663575%2C%22south%22%3A42.58331360101628%2C%22north%22%3A42.69103383260269%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A25659%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("quincy-ma", 100, "https://www.zillow.com/quincy-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.08335932336425%2C%22east%22%3A-70.92045267663573%2C%22south%22%3A42.20273234586776%2C%22north%22%3A42.311108444907994%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A6665%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("somerville-ma", 100, "https://www.zillow.com/somerville-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.14447466168212%2C%22east%22%3A-71.06302133831787%2C%22south%22%3A42.36836873242638%2C%22north%22%3A42.422437644820164%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A54458%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A14%7D"),
        ("salem-ma", 60, "https://www.zillow.com/salem-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.98899632336426%2C%22east%22%3A-70.82608967663575%2C%22south%22%3A42.46191283551909%2C%22north%22%3A42.56984279708086%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A33821%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("gloucester-ma", 50, "https://www.zillow.com/gloucester-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.85058614672852%2C%22east%22%3A-70.52477285327149%2C%22south%22%3A42.523195540613465%2C%22north%22%3A42.73865737478083%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A18311%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("andover-ma", 50, "https://www.zillow.com/andover-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.32847314672853%2C%22east%22%3A-71.0026598532715%2C%22south%22%3A42.54216670540813%2C%22north%22%3A42.75756296832629%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A397699%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D")],
    7: [("bristol-county-ma", 800, "https://www.zillow.com/bristol-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.71868858691406%2C%22east%22%3A-70.41543541308593%2C%22south%22%3A41.31870167022555%2C%22north%22%3A42.19254759138681%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A1558%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("nantucket-county-ma", 150, "https://www.zillow.com/nantucket-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-70.51857179345703%2C%22east%22%3A-69.86694520654297%2C%22south%22%3A41.09522395170446%2C%22north%22%3A41.535140630064824%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2813%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%7D"),
        ("lynn-ma", 100, "https://www.zillow.com/lynn-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.04762132336427%2C%22east%22%3A-70.88471467663575%2C%22south%22%3A42.425200942224336%2C%22north%22%3A42.53319423152182%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A39558%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("fall-river-ma", 100, "https://www.zillow.com/fall-river-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.26761614672851%2C%22east%22%3A-70.94180285327148%2C%22south%22%3A41.57612173325771%2C%22north%22%3A41.79482698157451%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A31525%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("taunton-ma", 100, "https://www.zillow.com/taunton-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.24730164672852%2C%22east%22%3A-70.92148835327149%2C%22south%22%3A41.80727800894323%2C%22north%22%3A42.02519707430164%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A27374%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("dartmouth-ma", 60, "https://www.zillow.com/dartmouth-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.30076329345704%2C%22east%22%3A-70.64913670654298%2C%22south%22%3A41.37173616529366%2C%22north%22%3A41.80978789627603%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A51652%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A11%7D"),
        ("attleboro-ma", 50, "https://www.zillow.com/attleboro-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-71.45720314672852%2C%22east%22%3A-71.13138985327149%2C%22south%22%3A41.83095795539875%2C%22north%22%3A42.04879628367766%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A23542%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D")],
    8: [("hampden-county-ma", 500, "https://www.zillow.com/hampden-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.25658508691407%2C%22east%22%3A-71.95333191308595%2C%22south%22%3A41.73538473375339%2C%22north%22%3A42.603573105579486%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A1746%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("berkshire-county-ma", 400, "https://www.zillow.com/berkshire-county-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.88007408691406%2C%22east%22%3A-72.57682091308594%2C%22south%22%3A41.95984174613806%2C%22north%22%3A42.82496387242598%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A2628%2C%22regionType%22%3A4%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%7D"),
        ("springfield-ma", 180, "https://www.zillow.com/springfield-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-72.62779482336425%2C%22east%22%3A-72.46488817663574%2C%22south%22%3A42.05852467519942%2C%22north%22%3A42.167148047361884%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A7221%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("pittsfield-ma", 150, "https://www.zillow.com/pittsfield-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.34188132336425%2C%22east%22%3A-73.17897467663573%2C%22south%22%3A42.396767631462794%2C%22north%22%3A42.504809937617296%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A19967%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("great-barrington-ma", 60, "https://www.zillow.com/great-barrington-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.45945264672852%2C%22east%22%3A-73.13363935327149%2C%22south%22%3A42.0645347735635%2C%22north%22%3A42.28157473739406%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A398147%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A12%7D"),
        ("becket-ma", 50, "https://www.zillow.com/becket-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.16242332336427%2C%22east%22%3A-72.99951667663575%2C%22south%22%3A42.23288659804281%2C%22north%22%3A42.34121090492224%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A50932%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D"),
        ("north-adams-ma", 50, "https://www.zillow.com/north-adams-ma/?searchQueryState=%7B%22pagination%22%3A%7B%7D%2C%22isMapVisible%22%3Atrue%2C%22mapBounds%22%3A%7B%22west%22%3A-73.19974532336425%2C%22east%22%3A-73.03683867663574%2C%22south%22%3A42.629672927898845%2C%22north%22%3A42.73731294255138%7D%2C%22regionSelection%22%3A%5B%7B%22regionId%22%3A56340%2C%22regionType%22%3A6%7D%5D%2C%22filterState%22%3A%7B%22sort%22%3A%7B%22value%22%3A%22globalrelevanceex%22%7D%7D%2C%22isListVisible%22%3Atrue%2C%22mapZoom%22%3A13%7D")]
        }
    # Grand Total: ~10,000 properties (leaving room for actual availability)
    
    # Get the queue for this terminal
    my_queue = city_queues.get(queue_id, city_queues[1])
    
    # Calculate expected total for this queue
    expected_total = sum(count for city, count, _ in my_queue)
    
    print(f"Configuration:")
    print(f"  • Queue ID: {queue_id}")
    print(f"  • Cities in queue: {len(my_queue)}")
    print(f"  • Expected properties: {expected_total}")
    print(f"  • Headless mode: {headless}")
    print(f"  • Output base directory: {output_base_dir}")
    print("-" * 60)
    print(f"Queue {queue_id} cities:")
    for city, count, _ in my_queue: 
        print(f"  • {city}: {count} properties")
    print("="*80)
    
    # Create base output directory
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Initialize scraper once for all cities
    scraper = MultiPropertyZillowScraper(headless=headless)
    
    # Process each city in the queue
    total_properties_scraped = 0
    cities_completed = 0
    cities_failed = 0
    
    for city_index, (city, max_properties_this_city, search_url) in enumerate(my_queue, 1):
        print(f"\n" + "🏙️ " * 20)
        print(f"QUEUE {queue_id} - CITY {city_index}/{len(my_queue)}: {city}")
        print(f"Target: {max_properties_this_city} properties")
        print(f"Using optimized search URL: {search_url}")
        print(f"🏙️ " * 20)
        
        try:
            # Create city-specific output directory with clean name
            city_dir_name = city.replace('-MA', '').replace('-', '_').lower()
            city_output_dir = os.path.join(output_base_dir, f"queue_{queue_id}", city_dir_name)
            os.makedirs(city_output_dir, exist_ok=True)
            
            # Save current directory
            original_dir = os.getcwd()
            
            try:
                # Change to city output directory
                os.chdir(city_output_dir)
                
                # Scrape properties for this city with the PROPER URL
                print(f"\n🚀 Starting to scrape {max_properties_this_city} properties from {city}...")
                all_properties = scraper.scrape_multiple_properties(search_url, max_properties=max_properties_this_city)
                
                # Rest of the code stays the same...
                
                # Save data for this city with unique naming
                if all_properties:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    # Create unique filename with city and queue info
                    safe_city_name = city.replace('-MA', '').replace('-', '_').lower()
                    filename_prefix = f"zillow_q{queue_id}_{safe_city_name}_{max_properties_this_city}props_{timestamp}"
                    
                    json_file, csv_file = scraper.save_all_properties(filename_prefix=filename_prefix)
                    
                    # Create city summary
                    city_summary = {
                        "queue_id": queue_id,
                        "city": city,
                        "target_properties": max_properties_this_city,
                        "actual_properties": len(all_properties),
                        "city_index": city_index,
                        "timestamp": timestamp,
                        "json_file": json_file,
                        "csv_file": csv_file,
                        "output_directory": city_output_dir
                    }
                    
                    summary_file = f"summary_q{queue_id}_{safe_city_name}_{max_properties_this_city}props_{timestamp}.json"
                    with open(summary_file, 'w') as f:
                        json.dump(city_summary, f, indent=2)
                    
                    print(f"\n✅ {city} COMPLETED!")
                    print(f"   🎯 Target: {max_properties_this_city} properties")
                    print(f"   ✓ Actual: {len(all_properties)} properties")
                    print(f"   📁 Data saved to: {city_output_dir}")
                    print(f"   📄 JSON: {json_file}")
                    print(f"   📊 CSV: {csv_file}")
                    
                    total_properties_scraped += len(all_properties)
                    cities_completed += 1
                    
                    # Clear the scraper's data for next city
                    scraper.all_properties_data = []
                    
                else:
                    print(f"\n❌ {city} FAILED - No properties scraped")
                    cities_failed += 1
                    
            finally:
                # Always return to original directory
                os.chdir(original_dir)
                
        except Exception as e:
            print(f"\n❌ ERROR in {city}: {e}")
            import traceback
            traceback.print_exc()
            cities_failed += 1
            
            # Try to save partial data if any
            if scraper.all_properties_data:
                try:
                    os.chdir(city_output_dir)
                    scraper.save_all_properties(filename_prefix=f"zillow_{city}_error_partial")
                    scraper.all_properties_data = []  # Clear for next city
                except:
                    pass
                finally:
                    os.chdir(original_dir)
        
        # Progress update
        remaining_cities = len(my_queue) - city_index
        progress_percentage = (total_properties_scraped / expected_total) * 100
        
        print(f"\n📊 QUEUE {queue_id} PROGRESS:")
        print(f"   • Completed: {cities_completed}/{len(my_queue)} cities")
        print(f"   • Failed: {cities_failed}/{len(my_queue)} cities")
        print(f"   • Remaining: {remaining_cities} cities")
        print(f"   • Total properties: {total_properties_scraped}/{expected_total} ({progress_percentage:.1f}%)")
        
        if remaining_cities > 0:
            next_city, next_target, _ = my_queue[city_index]
            print(f"   • Next city: {next_city} (target: {next_target} properties)")
            print(f"\n⏳ Waiting 60 seconds before next city...")
            time.sleep(60)  # Longer pause between cities for high-volume scraping
    
    # Final cleanup
    try:
        scraper.driver.quit()
    except:
        pass
    
    # Final summary
    print(f"\n" + "🎉" * 20)
    print(f"QUEUE {queue_id} COMPLETED!")
    print(f"🎉" * 20)
    print(f"Final Results:")
    print(f"   ✅ Cities completed: {cities_completed}/{len(my_queue)}")
    print(f"   ❌ Cities failed: {cities_failed}/{len(my_queue)}")
    print(f"   🎯 Target properties: {expected_total}")
    print(f"   📊 Actual properties: {total_properties_scraped}")
    print(f"   📈 Success rate: {(total_properties_scraped/expected_total)*100:.1f}%")
    print(f"   📁 Data saved in: {output_base_dir}/queue_{queue_id}/")
    
    # Create overall queue summary
    try:
        os.chdir(output_base_dir)
        queue_summary = {
            "queue_id": queue_id,
            "total_cities": len(my_queue),
            "cities_completed": cities_completed,
            "cities_failed": cities_failed,
            "target_properties": expected_total,
            "actual_properties": total_properties_scraped,
            "success_rate": (total_properties_scraped/expected_total)*100,
            "cities_processed": [city for city, _, _ in my_queue[:cities_completed + cities_failed]],
            "completion_time": datetime.now().isoformat()
        }
        
        with open(f"queue_{queue_id}_final_summary.json", 'w') as f:
            json.dump(queue_summary, f, indent=2)
            
        print(f"   📋 Queue summary saved: queue_{queue_id}_final_summary.json")
        
    except:
        pass
    
    print("\n" + "="*80)
    print("QUEUE PROCESSING COMPLETED")
    print("="*80)