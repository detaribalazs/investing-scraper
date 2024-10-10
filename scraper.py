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
    "equity_common": ["Fiscal Year", "Common Equity"],
    "ni_cf": ["Fiscal Year", "Net Income (CF)"],
}

upper_limit = ""
lower_limit = ""
retry_count = 1
chunk_size = 10
metric = "marketcap"
doi = "2023-12"  # date of interest
output_file = f'./output/{metric}-{doi}.yaml'
cache_file_path = f'./cache/{metric}-{doi}.yaml'
default_cache_file_path = f'./cache/default_{metric}-{doi}.yaml'


def convert_to_number(number_string: str) -> str:
    if number_string.endswith(" B"):
        number_string = number_string[:-1]
        num = float(number_string)
        num *= 1000000000
        return str(num)
    if number_string.endswith(" M"):
        number_string = number_string[:-1]
        num = float(number_string)
        num *= 1000000
        return str(int(num))
    return number_string


def interceptor(request):
    # add the missing headers
    cookie = 'smplog-trace=8ce7c7335aeb5a68; udid=bc7011112c2a9001709c3072aa550f97; user-browser-sessions=1; adBlockerNewUserDomains=1714491665; lifetime_page_view_count=47; cf_clearance=lZERZvOoBm8RpgKtRA0oEfovA1Mgybq1uJpU3sbVSfk-1728501189-1.2.1.1-wBTakafYXeDmnvf2mSklCFTsV7O_c96T4AyOwWESnboKhoN1KyM5qrOFgcR4LlRbdQzyRhsOkP6_kTLfsPRtpjVq2EE3WUfzKUK2TXUV4t.aNrdHMd4kvylC2twW1vjGxWv9lksVmqet7j1UbKqx76of8bnKxtcbVBzBDEU15rGfZGTTpDin1AS5bjjjGSHr.iZsFvnc3dadgWv.2OfM4IUSjd9Cjn0yn_vujBxxCdonPIz3luU7HpI9MTcd.EzOI101RNCH3tcAMjMoJp4pis7Mp2vK_XrBLRmJVfKXd_atJnT74aqHImF1.UEY2Pm_0RdJRCvuSPr6cwbLSQITgqtPJNhxIcZypJ4tEdk1Cjo; g_state={"i_p":1728934604650,"i_l":3}; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A5%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22474%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A25%3A%22%2Fequities%2Fbanco-santander%22%3B%7Di%3A1%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250498%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A26%3A%22%2Fequities%2Fzagrebacka-banka%22%3B%7Di%3A2%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A4%3A%228736%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A22%3A%22%2Fequities%2Fot-bank-nyrt%22%3B%7Di%3A3%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250449%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A24%3A%22%2Fequities%2Fbrd-groupe-soc%22%3B%7Di%3A4%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22396%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A21%3A%22%2Fequities%2Fbnp-paribas%22%3B%7D%7D%7D%7D; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Oct+06+2024+21%3A22%3A17+GMT%2B0200+(Central+European+Summer+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0dd4af3e-d12d-4d54-bd59-2bbe8de5147b&interactionCount=1&isAnonUser=1&landingPath=https%3A%2F%2Fwww.investing.com%2Facademy%2Fstock-picks%2Finvestingpro-subscription-pricing-value%2F&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1; usprivacy=1YNN; __stripe_mid=42388d8d-4141-49a2-8117-db2e0dec9e52b13983; browser-session-counted=true; page_view_count=61; r_p_s_n=1; proscore_card_opened=1; workstation_watchlist_opened=1; finboxio-production:jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjozMDI0ODYzLCJ2aXNpdG9yX2lkIjoidi05NTJiMjc1MWUxNWEiLCJmaXJzdF9zZWVuIjoiMjAyNC0xMC0wNlQxOToyMzo1MC4yODZaIiwiY2FwdGNoYV92ZXJpZmllZCI6ZmFsc2UsIm11c3RfcmV2ZXJpZnkiOmZhbHNlLCJwcmV2aWV3X2FjY2VzcyI6eyJhc3NldHNfdmlld2VkIjpbIkJVU0U6TUJIQkFOSyIsIkJVU0U6T1RQIiwiTkFTREFRR1M6QUFQTCIsIkJVTDpGSUIiLCJFTlhUUEE6Qk5QIl0sImFzc2V0c19tYXgiOjUsInZhbGlkX3VudGlsIjoiMjAyNC0xMC0wN1QwNzoyMzo1MC4wMDBaIn0sInJvbGVzIjpbInVzZXIiLCJpbnZlc3RpbmciXSwiYnVuZGxlIjoicHJvZmVzc2lvbmFsIiwiYm9vc3RzIjpbImRhdGEiLCJlc3NlbnRpYWxzIiwicHJlbWl1bSJdLCJhc3NldHMiOltdLCJyZWdpb25zIjpbImxhYWZtZSIsImV1cm8iLCJ1ayIsImNhbXgiLCJ1cyIsImFwYWMiXSwic2NvcGVzIjpbInJvbGU6dXNlciIsInJvbGU6aW52ZXN0aW5nIiwiYnVuZGxlOnByb2Zlc3Npb25hbCIsInJlZ2lvbjpsYWFmbWUiLCJyZWdpb246ZXVybyIsInJlZ2lvbjp1ayIsInJlZ2lvbjpjYW14IiwicmVnaW9uOnVzIiwicmVnaW9uOmFwYWMiLCJib29zdDpkYXRhIiwiYm9vc3Q6ZXNzZW50aWFscyIsImJvb3N0OnByZW1pdW0iXSwiZm9yIjoiMTg4LjE1Ny4xNjAuMTg3IiwiZXhwIjoxNzI4NTAxNDg5LCJpYXQiOjE3Mjg1MDExODl9.31cVQWLtpYVUQPQRrsCuBl0QkARIGpnmAL6-2MyKcxs; finboxio-production:jwt.sig=p1mdc3KEr07dQrrBW2mBY844L6Q; finboxio-production:refresh=c98e773a-1a17-4026-9c97-85039a4d3b97; finboxio-production:refresh.sig=xX8uvZowdenvXeCWx6px6XI3SaY; comment_notification_263020933=1; gtmFired=OK; PHPSESSID=kgn6oclcdl9rmd16p9aoqchqqg; ses_id=MnwwcWRrY2swdGlvMGE4PDJgNW5kYWJgNT01NTM3NCJnc2FvYjVmIDY5PHJubWJ%2BNzFjNmYyOmBiY2c%2BYDBkNzJlMGtkMWM3MDBpZDA1OD4yajVoZGNiYDVnNWMzNTQ7Z2hhP2JgZmM2ZjwybjFiODclY39mIjorYjBnN2AhZCMyPTBxZDRjODBmaWMwazg5MmU1bGRnYmg1PDUxM2c0LGcs; upa=eyJpbnZfcHJvX2Z1bm5lbCI6IiIsIm1haW5fYWMiOiI0IiwibWFpbl9zZWdtZW50IjoiMiIsImRpc3BsYXlfcmZtIjoiMTIzIiwiYWZmaW5pdHlfc2NvcmVfYWNfZXF1aXRpZXMiOiIxMCIsImFmZmluaXR5X3Njb3JlX2FjX2NyeXB0b2N1cnJlbmNpZXMiOiIyIiwiYWZmaW5pdHlfc2NvcmVfYWNfY3VycmVuY2llcyI6IjMiLCJhY3RpdmVfb25faW9zX2FwcCI6IjAiLCJhY3RpdmVfb25fYW5kcm9pZF9hcHAiOiIxIiwiYWN0aXZlX29uX3dlYiI6IjEiLCJpbnZfcHJvX3VzZXJfc2NvcmUiOiIwIn0%3D; finbox-visitor-id=v-O0t_4XMovOx-VEyJVZh7U; accessToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3Mjg1MDIzOTQsImp0aSI6IjI2MzAyMDkzMyIsImlhdCI6MTcyODQ5ODc5NCwiaXNzIjoiaW52ZXN0aW5nLmNvbSIsInVzZXJfaWQiOjI2MzAyMDkzMywicHJpbWFyeV9kb21haW5faWQiOiIxIiwiQXV0aG5TeXN0ZW1Ub2tlbiI6IiIsIkF1dGhuU2Vzc2lvblRva2VuIjoiIiwiRGV2aWNlVG9rZW4iOiIiLCJVYXBpVG9rZW4iOiJNbnd3Y1dSclkyc3dkR2x2TUdFNFBESmdOVzVrWVdKZ05UMDFOVE0zTkNKbmMyRnZZalZtSURZNVBISnViV0olMkJOekZqTm1ZeU9tQmlZMmMlMkJZREJrTnpKbE1HdGtNV00zTURCcFpEQTFPRDR5YWpWb1pHTmlZRFZuTldNek5UUTdaMmhoUDJKZ1ptTTJaand5YmpGaU9EY2xZMzltSWpvcllqQm5OMkFoWkNNeVBUQnhaRFJqT0RCbWFXTXdhemc1TW1VMWJHUm5ZbWcxUERVeE0yYzBMR2NzIiwiQXV0aG5JZCI6IiIsIklzRG91YmxlRW5jcnlwdGVkIjpmYWxzZSwiRGV2aWNlSWQiOiIiLCJSZWZyZXNoRXhwaXJlZEF0IjoxNzMxMDE4Nzk0LCJwZXJtaXNzaW9ucyI6eyJhZHMuZnJlZSI6MSwiaW52ZXN0aW5nLnByZW1pdW0iOjEsImludmVzdGluZy5wcm8iOjF9fQ._OMfjX7gepn2son_l-it2SbrOZMlYC2wDgALb5O4-ao; __cflb=02DiuGRugds2TUWHMkkPGro65dgYiP188ZyroYad8Q5UG; __cf_bm=IH5ce4_owDxJHba3v0pX_njMLfRklZt_DpnY0KPGmRQ-1728501189-1.0.1.1-K.dvAsPwymTqxQCQugo_W9mf51bVecGV.IGvlDCgBya3O3IQRDpmBzJczDqXjY_3VybBTJw6sNBSZQAL2qZQ5o8Moqfs68XHxm_y18wzMuQ; gcc=HU; gsc=BU; smd=bc7011112c2a9001709c3072aa550f97-1728501190'
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
                data[h] = convert_to_number(cells[indices[h]].text)
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
            data = find_in_table(table, doi, headers_map[metric])
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
        self.path = path
        if os.path.exists(path):
            with open(path, "r") as f:
                self.data = yaml.safe_load(f)
        else:
            if path == "":
                self.path = default_cache_file_path
            self.data = {}

    def write(self):
        with open(self.path, "w") as f:
            yaml.dump(self.data, f, indent=2)


async def main():
    cache = Cache(cache_file_path)
    output = cache.data
    tickers = filter_from_cache(cache.data)[:]
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
