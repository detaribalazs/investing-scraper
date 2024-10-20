import asyncio
import yaml
import logging
import os
from typing import TextIO

import yaml
from selenium.common import NoSuchElementException
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


class MetricConfig:
    def __init__(self, name: str, headers: list[str], table_id: str):
        self.headers = headers
        self.table_id = table_id
        self.name = name


configs_map = {
    "marketcap": MetricConfig("marketcap", ["Date", "Shares Outstanding", "Market Cap"], table_id="Definition"),
    "payout_ratio": MetricConfig("payout_ratio", ["Fiscal Year", "Payout Ratio"], table_id="Definition"),
    "equity_common": MetricConfig("equity_common", ["Fiscal Year", "Common Equity"], table_id="Definition"),
    "ni_cf": MetricConfig("ni_cf", ["Fiscal Year", "Net Income (CF)"], table_id="Definition"),
    "beta": MetricConfig("beta", ["Ticker", "Beta (5 Year)"], table_id="Benchmarks"),
    "income_statement": MetricConfig("income_statement", ["Net Income to Stockholders", "EBT, Incl. Unusual Items"], table_id="rt-table"),
    "balance_sheet": MetricConfig("balance_sheet", ["Common Equity", "Retained Earnings"], table_id="rt-table"),
}

debug = True
upper_limit = ""
lower_limit = ""
retry_count = 1
chunk_size = 10
metric = "balance_sheet"
doi = ["2018-12", "2019-12", "2020-12", "2021-12", "2022-12", "2023-12"]  # da]te of interest
output_suffix = doi[0].split("-")[0] if len(doi) == 1 else doi[0].split("-")[0] + "_" + doi[-1].split("-")[0]
output_file = f'./output/{metric}:{output_suffix}.yaml'
cache_file_path = f'./cache/{metric}:{output_suffix}.yaml'
default_cache_file_path = f'./cache/default_{metric}:{output_suffix}.yaml'

NA = "=NA()"


def try_to_num(num_string: str) -> float:
    try:
        return float(num_string.replace(',', ''))
    except ValueError:
        return num_string


def convert_to_number(number_string: str):
    try:
        number_string = number_string.replace(',', '')
        if number_string.endswith(" B"):
            number_string = number_string[:-1]
            num = float(number_string.replace(',', ''))
            num *= 1000000000
            return int(num)
        if number_string.endswith(" M"):
            number_string = number_string[:-1]
            num = float(number_string)
            num *= 1000000
            return int(num)
        if number_string.endswith("%"):
            number_string = number_string[:-1]
            num = float(number_string.replace(',', ''))
            return num / 100
        return float(number_string.replace(',', ''))
    except ValueError:
        return number_string


def convert_to_float(number_string: str):
    try:
        number_string = number_string.replace(',', '')
        return float(number_string)
    except ValueError:
        return NA


def interceptor(request):
    # add the missing headers
    cookie = 'smplog-trace=8ce7c7335aeb5a68; udid=bc7011112c2a9001709c3072aa550f97; user-browser-sessions=1; adBlockerNewUserDomains=1714491665; lifetime_page_view_count=58; g_state={"i_p":1728934604650,"i_l":3}; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A5%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22474%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A25%3A%22%2Fequities%2Fbanco-santander%22%3B%7Di%3A1%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250498%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A26%3A%22%2Fequities%2Fzagrebacka-banka%22%3B%7Di%3A2%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A4%3A%228736%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A22%3A%22%2Fequities%2Fot-bank-nyrt%22%3B%7Di%3A3%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250449%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A24%3A%22%2Fequities%2Fbrd-groupe-soc%22%3B%7Di%3A4%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22396%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A21%3A%22%2Fequities%2Fbnp-paribas%22%3B%7D%7D%7D%7D; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Oct+06+2024+21%3A22%3A17+GMT%2B0200+(Central+European+Summer+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0dd4af3e-d12d-4d54-bd59-2bbe8de5147b&interactionCount=1&isAnonUser=1&landingPath=https%3A%2F%2Fwww.investing.com%2Facademy%2Fstock-picks%2Finvestingpro-subscription-pricing-value%2F&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1; usprivacy=1YNN; __stripe_mid=42388d8d-4141-49a2-8117-db2e0dec9e52b13983; finboxio-production:jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjozMDI0ODYzLCJ2aXNpdG9yX2lkIjoidi05NTJiMjc1MWUxNWEiLCJmaXJzdF9zZWVuIjoiMjAyNC0xMC0wNlQxOToyMzo1MC4yODZaIiwiY2FwdGNoYV92ZXJpZmllZCI6ZmFsc2UsIm11c3RfcmV2ZXJpZnkiOmZhbHNlLCJwcmV2aWV3X2FjY2VzcyI6eyJhc3NldHNfdmlld2VkIjpbIkJVU0U6TUJIQkFOSyIsIkJVU0U6T1RQIiwiTkFTREFRR1M6QUFQTCIsIkJVTDpGSUIiLCJFTlhUUEE6Qk5QIl0sImFzc2V0c19tYXgiOjUsInZhbGlkX3VudGlsIjoiMjAyNC0xMC0wN1QwNzoyMzo1MC4wMDBaIn0sInJvbGVzIjpbInVzZXIiLCJpbnZlc3RpbmciXSwiYnVuZGxlIjoicHJvZmVzc2lvbmFsIiwiYm9vc3RzIjpbImRhdGEiLCJlc3NlbnRpYWxzIiwicHJlbWl1bSJdLCJhc3NldHMiOltdLCJyZWdpb25zIjpbImxhYWZtZSIsImV1cm8iLCJ1ayIsImNhbXgiLCJ1cyIsImFwYWMiXSwic2NvcGVzIjpbInJvbGU6dXNlciIsInJvbGU6aW52ZXN0aW5nIiwiYnVuZGxlOnByb2Zlc3Npb25hbCIsInJlZ2lvbjpsYWFmbWUiLCJyZWdpb246ZXVybyIsInJlZ2lvbjp1ayIsInJlZ2lvbjpjYW14IiwicmVnaW9uOnVzIiwicmVnaW9uOmFwYWMiLCJib29zdDpkYXRhIiwiYm9vc3Q6ZXNzZW50aWFscyIsImJvb3N0OnByZW1pdW0iXSwiZm9yIjoiMjAwMTo0YzRjOjEwYTI6MjAwOmJkZGY6ZjY1MzphOWE1OjE5ZTgiLCJleHAiOjE3Mjk0NTQ3ODIsImlhdCI6MTcyOTQ1NDQ4Mn0.Q7xlW6JDV8fzIhxMi9Q6ToGtgAurRWe_b79RhD5m0jI; finboxio-production:jwt.sig=iEs3rlM3fYHSL3zSTbiJjNeN6S8; finboxio-production:refresh=c98e773a-1a17-4026-9c97-85039a4d3b97; finboxio-production:refresh.sig=xX8uvZowdenvXeCWx6px6XI3SaY; finbox-visitor-id=v-ZvAbMU7-j6kv2aeb30247; ses_id=Yy0%2BfzI9Y2s2cmBmN2YzNzZkNm1kYTo4YmplZWZiZ3E5LTQ6bjk0cj8wO3ViYTUpNz9hZ2YyMjY8bmVrZmswNmNkPmgyY2M6NmZgZDcwM2U2ZTY%2FZGs6PWJrZWVmYGdtOTs0NG5qNDc%2FazsyYj01bzclYX1mIjIjPG5lNWYnMHdjbD5%2FMmJjODZgYD83YDMyNmM2b2RjOjxiNmVgZmRnfzly; browser-session-counted=true; page_view_count=72; r_p_s_n=1; proscore_card_opened=1; workstation_watchlist_opened=1; PHPSESSID=kgn6oclcdl9rmd16p9aoqchqqg; invab=popup_2; accessToken=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3Mjk0NTY2OTYsImp0aSI6IjI2MzAyMDkzMyIsImlhdCI6MTcyOTQ1MzA5NiwiaXNzIjoiaW52ZXN0aW5nLmNvbSIsInVzZXJfaWQiOjI2MzAyMDkzMywicHJpbWFyeV9kb21haW5faWQiOiIxIiwiQXV0aG5TeXN0ZW1Ub2tlbiI6IiIsIkF1dGhuU2Vzc2lvblRva2VuIjoiIiwiRGV2aWNlVG9rZW4iOiIiLCJVYXBpVG9rZW4iOiJZeTAlMkJmekk5WTJzMmNtQm1OMll6Tnpaa05tMWtZVG80WW1wbFpXWmlaM0U1TFRRNmJqazBjajh3TzNWaVlUVXBOejloWjJZeU1qWThibVZyWm1zd05tTmtQbWd5WTJNNk5tWmdaRGN3TTJVMlpUWSUyRlpHczZQV0pyWldWbVlHZHRPVHMwTkc1cU5EYyUyRmF6c3lZajAxYnpjbFlYMW1JaklqUEc1bE5XWW5NSGRqYkQ1JTJGTW1Kak9EWmdZRDgzWURNeU5tTTJiMlJqT2p4aU5tVmdabVJuZnpseSIsIkF1dGhuSWQiOiIiLCJJc0RvdWJsZUVuY3J5cHRlZCI6ZmFsc2UsIkRldmljZUlkIjoiIiwiUmVmcmVzaEV4cGlyZWRBdCI6MTczMTk3MzA5NiwicGVybWlzc2lvbnMiOnsiYWRzLmZyZWUiOjEsImludmVzdGluZy5wcmVtaXVtIjoxLCJpbnZlc3RpbmcucHJvIjoxfX0.dIdhv83g3dcy5-21-Xms2AMedZd1Q0b6Z3znUyz1ilw; gcc=HU; gsc=BE; smd=bc7011112c2a9001709c3072aa550f97-1729453096; __cflb=02DiuGRugds2TUWHMkkPGro65dgYiP187jMB5Pk7tGtbW; __cf_bm=6MW7W6ecFqQzihzmFQr4wU13XSvQvO6naV0QLY0EkYg-1729454480-1.0.1.1-JEYOfzrABvVpatZ1L7zqu5onbksvZXgPgkZ9kmlXjcLYnTkoBcc9ulLmQXiHa5ueJA50X7AH13tFnw8Ria8STcnjy66EcVYSSm2.yvqEiD4; cf_clearance=xOJ8QXHOwwZY54Ff5UiTUwZ399U8mbu2KBYiTlFQCMw-1729454481-1.2.1.1-xsrGwsTBCmvvEIGjggYQNP3MKWiIkBpbMTe0.cjRYCIoOhSb.gBLS1Vtvg8adAHmS6sqmEwW1R6wj05elXId8ncSI3A1PWFgzjuR22kGuULa1H0UdEXsVmElGdEyYxv3pZqmBVIGvH27uQr_Yv4jq402HqAjBGQBhfw3chiDvleBy7GFAOOtn.ekohgufrVcoRp3LF12w1oaUzMUVf6ZLePEi8yyKy3C6PGwAZbqyoU1fU57TOy8KAm4cfzKeN9QGYWW0AKI.esEE0cURQC8a0n5W0xC2JfMO1xzZnswf6zlXRrXrxhiTkCno3M442HD1sfkgV9Z2zbMupaDdiUt33UTqxxeAtMktPKfC6oN7fg'
    request.headers["Cookie"] = cookie
    request.headers["Referer"] = "https://www.investing.com/"


def target_date_matches(target_dates: list[str], row_date: str) -> str:
    for target_date in target_dates:
        if target_date in row_date:
            return target_date
    return ""


def find_in_table(table: WebElement, target_data: list[str], ticker: str, headers: list, table_type: str) -> dict:
    if table_type == "Definition":
        return find_in_definition_table(table, target_data, headers)
    if table_type == "Benchmarks":
        return find_in_benchmark_table(table, ticker, headers)
    if table_type == "rt-table":
        return find_in_financials_table(table, target_data, headers)
    logger.warning(f"Table type not supported: {table_type}")
    return {}


def find_in_financials_table(table: WebElement, target_dates: list[str], headers: list) -> dict:
    date_indices = {}
    # header_row = table.find_element(By.CLASS_NAME, "rt-thead -header")
    # if not header_row:
    #     raise NoSuchElementException("Header row not found with class rt-thead -header")
    dates = table.find_elements(By.CSS_SELECTOR, f'[role="columnheader"]')
    if not dates:
        raise NoSuchElementException("Date row not found with role 'columnheader'")
    for date, i in zip(dates, range(len(dates))):
        date_text = target_date_matches(target_dates, date.text)
        if date_text:
            date_indices[date_text] = i

    rows = table.find_elements(By.CSS_SELECTOR, f'[role="rowgroup"]')
    data = {}
    for row in rows:
        cells = row.find_elements(By.CSS_SELECTOR, f'[role="gridcell"]')
        if len(cells) == 0:
            continue
        if cells[0].text in headers:
            financial_measure = cells[0].text
            for date, index in date_indices.items():
                if not data.get(financial_measure, False):
                    data[financial_measure] = {}
                data[financial_measure][date[:4]] = convert_to_number(cells[index].text)
    return data


def find_in_definition_table(table: WebElement, target_dates: list[str], headers: list) -> dict:
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

    data = {}
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, 'td')
        row_date = cells[indices[date_string]]
        date_postfix = target_date_matches(target_dates, row_date.text)
        if date_postfix != "":
            #data = {date_string: row_date.text}
            for h in headers[1:]:
                if not data.get(h, False):
                    data[h] = {}
                try:
                    data[h][date_postfix[:4]] = convert_to_number(cells[indices[h]].text)
                except Exception as e:
                    logger.error(f"Could not convert {cells[indices[h]].text} to number")
    return data


def find_in_benchmark_table(table: WebElement, ticker: str, headers: list) -> dict:
    ticker_string = headers[0]
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
        row_ticker = cells[indices[ticker_string]]
        if ticker in row_ticker.text:
            data = {}
            for h in headers[1:]:
                data[h] = {"2023": convert_to_float(cells[indices[h]].text)}
            return data
    return {}


def find_table(page: WebDriver, id: str, metric: str) -> WebElement:
    if metric in {"balance_sheet", "income_statement"}:
        return find_table_in_financials(page, id)
    return find_table_in_explorer(page, id)


def find_table_in_financials(page: WebDriver, cls: str) -> WebElement:
    div = page.find_element(By.CLASS_NAME, cls)
    if not div:
        raise Exception(f"No table found by class ID {cls}")
    return div

def find_table_in_explorer(page: WebDriver, id="Definition") -> WebElement:
    div = page.find_element(By.CSS_SELECTOR, f'[data-rbd-draggable-id="{id}"]')
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


def build_url(ticker: str, metric: str) -> str:
    if metric in {"income_statement", "balance_sheet"}:
        return f"https://www.investing.com/pro/{ticker}/financials/{metric}"
    return f'https://www.investing.com/pro/{ticker}/explorer/{metric}'


async def fetch(ticker: str, metric: str, retry: int = 5) -> FetchedData:
    # build the web driver
    driver = build_driver()
    driver.implicitly_wait(10)
    # open the specified URL in the browser
    driver.request_interceptor = interceptor
    if not configs_map.get(metric, False):
        logger.error(f"unknown metric: {metric}")
        exit(1)

    # URL of the web page to scrape
    url = build_url(ticker, metric)

    cfg = configs_map[metric]
    try_count = 0
    data = {}
    while try_count < retry:
        try_count += 1
        try:
            logger.info(f"Fetching data for {ticker} for the #{try_count} time")
            logger.info(f"URL: {url}")
            try:
                await asyncio.to_thread(driver.get, url)
            except Exception as e:
                logger.error(e)
                exit(1)
            table = find_table(driver, cfg.table_id, metric)
            data = find_in_table(table, doi, ticker, cfg.headers, cfg.table_id)
            if len(data) != 0:
                break
            try_count += 1
        except Exception as e:
            if try_count < retry:
                try_count += 1
                await asyncio.sleep(5)
            else:
                logger.error(f"fetching from {url}: {str(e)}")
                driver.quit()
                return FetchedData(ticker, None, e)

    if len(data) == 0 and debug:
        with open(f"error/{ticker}.html", "w") as f:
            f.write(driver.execute_script("return document.documentElement.innerHTML;"))
        logger.warning(f"No data found for {ticker} after {try_count} retries")
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


def filter_from_cache(cache: dict, metric_config: MetricConfig, dates: list[str]) -> list:
    already_fetched_tickers = set()
    incomplete_entries = set()
    for ticker, data in cache.items():
        if len(data) != 0:
            for fin_data in metric_config.headers:
                if not data.get(fin_data, False):
                    already_fetched = False
                    break
                else:
                    for full_date in dates:
                        date = full_date.split('-')[0]
                        known_dates = set(data[fin_data].keys())
                        if date not in known_dates:
                            already_fetched = False
                            break
            if not already_fetched:
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
    tickers = filter_from_cache(cache.data, configs_map[metric], doi)[:]
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
