import asyncio
import yaml
import logging
import os
from typing import TextIO

import yaml
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from utils.utils import load_tickers

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

all_tickers = load_tickers("./input/tickers.yaml")

headers_map = {
    "marketcap": ["Date", "Shares Outstanding", "Market Cap"],
    "payout_ratio": ["Fiscal Year", "Payout Ratio"],
}

limit = -1
chunk_size = 50
retry_count = 1
output_file = './output/pr.json'
metric = "payout_ratio"
cache_file_path = './cache/pr.yaml'
default_cache_file_path = './cache/default.yaml'


def interceptor(request):
    # add the missing headers
    cookie = ""  # 'smplog-trace=8ce7c7335aeb5a68; udid=bc7011112c2a9001709c3072aa550f97; user-browser-sessions=1; adBlockerNewUserDomains=1714491665; lifetime_page_view_count=41; cf_clearance=_tJgehy_zKUk1hzsK6kmII7bWG1Y2CunR8SNY7iKKsg-1728395355-1.2.1.1-0L8govuLrRpOJ4BXHQnzAD7pMv40TE92WEiL.wVNjR27M6POcTZ9JI_doZITflC0KyoAAHf70JcDT3SxaZARiGE_juHoTOhHbcNCmlGGv5q0H5wDLUo0l0RPWb1eUhDuU2fuKkUU9ufYFpOdVDxXmqDYUgB8nvBWU21vq1wIEyWPi1.PVuJFtIBtKRmuB.GK7Vo1zGbsf7aftAGfjjddEPh0ydhIze_By3OoysP5e9YkZf4_xLFNmb0HuyWYyfpwFuK3J9w6.DNOAe6s6Y3qELZE1jMTRxLbVwcxjwwibWEPQ3hLjql.2mEWA0UyElOPBgGLWD9RBPtu1_TYxIngrALuQT1GQ_18Gbn4axyQyL8; g_state={"i_p":1728934604650,"i_l":3}; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A5%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22474%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A25%3A%22%2Fequities%2Fbanco-santander%22%3B%7Di%3A1%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250498%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A26%3A%22%2Fequities%2Fzagrebacka-banka%22%3B%7Di%3A2%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A4%3A%228736%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A22%3A%22%2Fequities%2Fot-bank-nyrt%22%3B%7Di%3A3%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250449%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A24%3A%22%2Fequities%2Fbrd-groupe-soc%22%3B%7Di%3A4%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22396%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A21%3A%22%2Fequities%2Fbnp-paribas%22%3B%7D%7D%7D%7D; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Oct+06+2024+21%3A22%3A17+GMT%2B0200+(Central+European+Summer+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0dd4af3e-d12d-4d54-bd59-2bbe8de5147b&interactionCount=1&isAnonUser=1&landingPath=https%3A%2F%2Fwww.investing.com%2Facademy%2Fstock-picks%2Finvestingpro-subscription-pricing-value%2F&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1; usprivacy=1YNN; __stripe_mid=42388d8d-4141-49a2-8117-db2e0dec9e52b13983; gtmFired=OK; __cflb=02DiuGRugds2TUWHMkkPGro65dgYiP1884dT36e9c69mv; adsFreeSalePopUp=1; _imntz_error=0; browser-session-counted=true; page_view_count=52; r_p_s_n=1; PHPSESSID=ri9s7dvgk6f94179q66odkspcf; proscore_card_opened=1; workstation_watchlist_opened=1; finboxio-production:jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjozMDI0ODYzLCJ2aXNpdG9yX2lkIjoidi05NTJiMjc1MWUxNWEiLCJmaXJzdF9zZWVuIjoiMjAyNC0xMC0wNlQxOToyMzo1MC4yODZaIiwiY2FwdGNoYV92ZXJpZmllZCI6ZmFsc2UsIm11c3RfcmV2ZXJpZnkiOmZhbHNlLCJwcmV2aWV3X2FjY2VzcyI6eyJhc3NldHNfdmlld2VkIjpbIkJVU0U6TUJIQkFOSyIsIkJVU0U6T1RQIiwiTkFTREFRR1M6QUFQTCIsIkJVTDpGSUIiLCJFTlhUUEE6Qk5QIl0sImFzc2V0c19tYXgiOjUsInZhbGlkX3VudGlsIjoiMjAyNC0xMC0wN1QwNzoyMzo1MC4wMDBaIn0sInJvbGVzIjpbInVzZXIiLCJpbnZlc3RpbmciXSwiYnVuZGxlIjoicHJvZmVzc2lvbmFsIiwiYm9vc3RzIjpbImRhdGEiLCJlc3NlbnRpYWxzIiwicHJlbWl1bSJdLCJhc3NldHMiOltdLCJyZWdpb25zIjpbImxhYWZtZSIsImV1cm8iLCJ1ayIsImNhbXgiLCJ1cyIsImFwYWMiXSwic2NvcGVzIjpbInJvbGU6dXNlciIsInJvbGU6aW52ZXN0aW5nIiwiYnVuZGxlOnByb2Zlc3Npb25hbCIsInJlZ2lvbjpsYWFmbWUiLCJyZWdpb246ZXVybyIsInJlZ2lvbjp1ayIsInJlZ2lvbjpjYW14IiwicmVnaW9uOnVzIiwicmVnaW9uOmFwYWMiLCJib29zdDpkYXRhIiwiYm9vc3Q6ZXNzZW50aWFscyIsImJvb3N0OnByZW1pdW0iXSwiZm9yIjoiMjAwMTo0YzRjOjIwYTE6Y2MwMDpjYzg3OjgyMTM6YWVmZjo4MDMzIiwiZXhwIjoxNzI4Mzk1NjU3LCJpYXQiOjE3MjgzOTUzNTd9.K2XWFwTGyx66evPxjkYGeoZq3dhwj8K0xD7FnCq35U8; finboxio-production:jwt.sig=d52J17DNF99ll8uWPkoE_JMZ7b8; ses_id=ZylmJ2JtMDg1cWttN2Y2MjZkYTpkYTc1MDhubmFlYnRkcGJsbzgzdT8waSdvbDklNWcwOGM9ZTdgYjM8YGBhZWdqZjZiNTA%2BNWprYzc1NjM2Y2E%2BZGI3YTBibj9hYmJpZGtiZ29rM2M%2FPGk8b2I5NDUnMCxjJ2V0YDIzY2AhYSZnaGYnYjIwazVja2I3ZTZhNmdhM2RiNzAwZG5pYWViemQv; finboxio-production:refresh=8fd07c0e-ce91-470f-9273-a8e2975af493; finboxio-production:refresh.sig=L1Jom6FtzL0xdgdCx0yOh5x_Oio; finbox-visitor-id=v-8LeJ65ogfIIQZ31_7RytT; __cf_bm=4M0nHylXbGdoXvPRnr6e85dBB7.21xqMYfyK7t6a4eM-1728393604-1.0.1.1-ZNUNcmX3Sh5aBZ8luSj0IWnXtoYrHFlYYs92EAHRFxkchlnyTp9JL0uIJh5xTxN5UkSnnca.G_BizcO887gCTtz3Y2ItkRagduiTjccarW0; accessToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjgzOTg5NTcsImp0aSI6IjI2MzAyMDkzMyIsImlhdCI6MTcyODM5NTM1NywiaXNzIjoiaW52ZXN0aW5nLmNvbSIsInVzZXJfaWQiOjI2MzAyMDkzMywicHJpbWFyeV9kb21haW5faWQiOiIxIiwiQXV0aG5TeXN0ZW1Ub2tlbiI6IiIsIkF1dGhuU2Vzc2lvblRva2VuIjoiIiwiRGV2aWNlVG9rZW4iOiIiLCJVYXBpVG9rZW4iOiJaeWxtSjJKdE1EZzFjV3R0TjJZMk1qWmtZVHBrWVRjMU1EaHVibUZsWW5Sa2NHSnNiemd6ZFQ4d2FTZHZiRGtsTldjd09HTTlaVGRnWWpNOFlHQmhaV2RxWmpaaU5UQSUyQk5XcHJZemMxTmpNMlkyRSUyQlpHSTNZVEJpYmo5aFltSnBaR3RpWjI5ck0yTSUyRlBHazhiMkk1TkRVbk1DeGpKMlYwWURJelkyQWhZU1puYUdZbllqSXdhelZqYTJJM1pUWmhObWRoTTJSaU56QXdaRzVwWVdWaWVtUXYiLCJBdXRobklkIjoiIiwiSXNEb3VibGVFbmNyeXB0ZWQiOmZhbHNlLCJEZXZpY2VJZCI6IiIsIlJlZnJlc2hFeHBpcmVkQXQiOjE3MzA5MTUzNTcsInBlcm1pc3Npb25zIjp7ImFkcy5mcmVlIjoxLCJpbnZlc3RpbmcucHJlbWl1bSI6MSwiaW52ZXN0aW5nLnBybyI6MX19.jMHYheWUzs_r-soVKRdsOZxFdEXoTp3P2UG0KX_r9JY; gcc=HU; gsc=BU; smd=bc7011112c2a9001709c3072aa550f97-1728395357'
    request.headers["Cookie"] = cookie
    request.headers["Referer"] = "https://www.investing.com/"


def find_in_table(table: WebElement, target_date: str, headers: list) -> dict:
    date_string = headers[0]
    ths = table.find_elements(By.TAG_NAME, 'th')
    if not ths:
        raise Exception("No ths in table found")

    indices = {}
    for i in range(len(ths)):
        if ths[i].text in headers:
            header = ths[i].text
            indices[header] = i
    if len(indices) == 0:
        raise Exception("No matching table headers found")

    body = table.find_elements(By.TAG_NAME, 'tbody')
    rows = body[0].find_elements(By.TAG_NAME, 'tr')
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, 'td')
        row_date = cells[indices[date_string]]
        if target_date in row_date.text:
            data = {date_string: row_date.text}
            for h in headers[1:]:
                data[h] = cells[indices[h]].text
            return data
    return {}


def find_table(page: WebDriver) -> WebElement:
    div = page.find_element(By.CSS_SELECTOR, '[data-rbd-draggable-id="Definition"]')
    if not div:
        raise Exception("No definition found")
    tables = div.find_elements(By.TAG_NAME, 'table')
    if not tables:
        raise Exception("No table found")
    return tables[0]


class FetchedData:
    ticker = ""
    data = ""
    error = None

    def __init__(self, ticker, data, error=None):
        self.ticker = ticker
        self.data = data
        self.error = error


async def fetch(ticker: str, metric: str, retry: int = 5) -> FetchedData:
    # build the web driver
    driver = build_driver()
    driver.implicitly_wait(10)
    # open the specified URL in the browser
    driver.request_interceptor = interceptor
    if not headers_map.get(metric, False):
        logger.error(f"unknown metric: {metric}")
        exit(1)

    # URL of the web page to scrape
    url = f'https://www.investing.com/pro/{ticker}/explorer/{metric}'

    retry_count = 0
    data = {}
    while retry_count < retry:
        try:
            logger.info(f"Fetching data for {ticker} for the #{retry_count + 1} time")
            logger.info(f"URL: {url}")
            try:
                await asyncio.to_thread(driver.get, url)
            except Exception as e:
                logger.error(e)
                exit(1)
            table = find_table(driver)
            data = find_in_table(table, "2023-12", headers_map[metric])
            if len(data) != 0:
                break
            retry_count += 1
        except Exception as e:
            if retry_count < retry:
                retry_count += 1
                await asyncio.sleep(5)
            else:
                driver.quit()
                return FetchedData(ticker, None, e)

    if len(data) == 0:
        with open(f"error/{ticker}.html", "w") as f:
            f.write(driver.execute_script("return document.documentElement.innerHTML;"))
        logger.warning(f"No data found for {ticker} after {retry_count} retries")
    else:
        logger.info(f"{ticker} succeeded: {data}")
    driver.quit()
    return FetchedData(ticker, data, None)


def build_driver() -> WebDriver:
    # instantiate options for Chrome
    options = webdriver.ChromeOptions()

    # run browser in headless mode
    options.add_argument('--headless=new')

    # instantiate Chrome WebDriver with options
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)


def filter_from_cache(cache: dict) -> list:
    already_fetched_tickers = set()
    for ticker, data in cache.items():
        if len(data) != 0:
            already_fetched_tickers.add(ticker)
        else:
            logger.info(f"No data found for {ticker} in cache")
    unfetched_tickers = []
    for ticker in all_tickers:
        if ticker not in already_fetched_tickers:
            unfetched_tickers.append(ticker)
    return unfetched_tickers


class Cache:
    def __init__(self, path: str):
        if os.path.exists(path):
            with open(path, "r") as f:
                self.data = yaml.safe_load(f)
            self.path = path
        else:
            self.path = default_cache_file_path
            self.data = {}

    def write(self):
        with open(self.path, "w") as f:
            yaml.dump(self.data, f, indent=2)


async def main():
    cache = Cache(cache_file_path)
    output = cache.data
    tickers = filter_from_cache(cache.data)[:limit]
    logger.info(f"Starting {len(tickers)} tickers")
    chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]

    for chunk, turn in zip(chunks, range(1, len(chunks) + 1)):
        tasks = [asyncio.create_task(fetch(ticker, metric, retry_count)) for ticker in chunk]
        logger.info(f"Waiting for {len(tickers) - turn * chunk_size} tickers to finish")
        fetched_list = await asyncio.gather(*tasks)
        for fetched_data in fetched_list:
            if fetched_data.error or len(fetched_data.data) == 0:
                cache.data[fetched_data.ticker] = {}
                output[fetched_data.ticker] = {}
            else:
                cache.data[fetched_data.ticker] = fetched_data.data
                output[fetched_data.ticker] = fetched_data.data
        cache.write()
        logger.info(f"Finished fetching data for {len(fetched_list)} items")

    logger.info(f"Writing data into {output_file}")
    with open(output_file, 'w') as f:
        yaml.dump(output, f, indent=2)


if __name__ == '__main__':
    asyncio.run(main())
