import asyncio
import time

from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager


def interceptor(request):

    # add the missing headers
    cookie = 'smplog-trace=8ce7c7335aeb5a68; udid=bc7011112c2a9001709c3072aa550f97; user-browser-sessions=1; adBlockerNewUserDomains=1714491665; lifetime_page_view_count=40; browser-session-counted=true; page_view_count=50; cf_clearance=.Lcgv3CnuIGxa0Ve2sjWQ6AS4kPRENDYr4Rb4kUCtLM-1728330332-1.2.1.1-fwWDzmS39kGEWvWYCEeQ3BEonkp24TnpykpGVxsPkXr6RR.kIFey2Ryp6FVLl52VEKjngeIjEwm4PwneZ_bbUX2VlVIn2xcw0rg3CMMr0tbLOTnAPKcXH4ECbWNLAb.9qqN4f_HL0C.MIc_IJx92hmh.bJ2IBL24GOnfG5lMqSewJ9MYr8mXAuoC_8s9HUb6oAW7uhNCXFt1DB9VY3WiVJg3GBkD6VMKdz8mSchsG0mrPymNJEtqBeaT.xCsrEKt09ztRqvp3HPFA1b.vFGq1R62dYqA72ti3.VHmO_7P3FxZFym4Omhg7KnMnAuGCAewf19kC.P87Bxh3KRvw0crOVI8k6Fau.GXyBj2cRJmAcr8Ia7uq7U5jDggWc78FR2keRFOIsMWprRzvRE.pYqZQ; g_state={"i_p":1728934604650,"i_l":3}; r_p_s_n=1; PHPSESSID=ri9s7dvgk6f94179q66odkspcf; proscore_card_opened=1; SideBlockUser=a%3A2%3A%7Bs%3A10%3A%22stack_size%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Bi%3A8%3B%7Ds%3A6%3A%22stacks%22%3Ba%3A1%3A%7Bs%3A11%3A%22last_quotes%22%3Ba%3A4%3A%7Bi%3A0%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A3%3A%22474%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A25%3A%22%2Fequities%2Fbanco-santander%22%3B%7Di%3A1%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250498%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A26%3A%22%2Fequities%2Fzagrebacka-banka%22%3B%7Di%3A2%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A4%3A%228736%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A22%3A%22%2Fequities%2Fot-bank-nyrt%22%3B%7Di%3A3%3Ba%3A3%3A%7Bs%3A7%3A%22pair_ID%22%3Bs%3A5%3A%2250449%22%3Bs%3A10%3A%22pair_title%22%3Bs%3A0%3A%22%22%3Bs%3A9%3A%22pair_link%22%3Bs%3A24%3A%22%2Fequities%2Fbrd-groupe-soc%22%3B%7D%7D%7D%7D; finboxio-production:jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjozMDI0ODYzLCJ2aXNpdG9yX2lkIjoidi05NTJiMjc1MWUxNWEiLCJmaXJzdF9zZWVuIjoiMjAyNC0xMC0wNlQxOToyMzo1MC4yODZaIiwiY2FwdGNoYV92ZXJpZmllZCI6ZmFsc2UsIm11c3RfcmV2ZXJpZnkiOmZhbHNlLCJwcmV2aWV3X2FjY2VzcyI6eyJhc3NldHNfdmlld2VkIjpbIkJVU0U6TUJIQkFOSyIsIkJVU0U6T1RQIiwiTkFTREFRR1M6QUFQTCIsIkJVTDpGSUIiXSwiYXNzZXRzX21heCI6NSwidmFsaWRfdW50aWwiOiIyMDI0LTEwLTA3VDA3OjIzOjUwLjAwMFoifSwicm9sZXMiOlsidXNlciIsImludmVzdGluZyJdLCJidW5kbGUiOiJwcm9mZXNzaW9uYWwiLCJib29zdHMiOlsiZGF0YSIsImVzc2VudGlhbHMiLCJwcmVtaXVtIl0sImFzc2V0cyI6W10sInJlZ2lvbnMiOlsibGFhZm1lIiwiZXVybyIsInVrIiwiY2FteCIsInVzIiwiYXBhYyJdLCJzY29wZXMiOlsicm9sZTp1c2VyIiwicm9sZTppbnZlc3RpbmciLCJidW5kbGU6cHJvZmVzc2lvbmFsIiwicmVnaW9uOmxhYWZtZSIsInJlZ2lvbjpldXJvIiwicmVnaW9uOnVrIiwicmVnaW9uOmNhbXgiLCJyZWdpb246dXMiLCJyZWdpb246YXBhYyIsImJvb3N0OmRhdGEiLCJib29zdDplc3NlbnRpYWxzIiwiYm9vc3Q6cHJlbWl1bSJdLCJmb3IiOiIxMzQuMjM4LjcxLjIyNyIsImV4cCI6MTcyODMzMTE4OCwiaWF0IjoxNzI4MzMwODg4fQ.711CvaarjwYJdSfhBtFOEOv0SQsx92iW-YBPaDnJwqE; finboxio-production:jwt.sig=cEv2TrVYvzSYwjTJypN6sQb3AU0; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Oct+06+2024+21%3A22%3A17+GMT%2B0200+(Central+European+Summer+Time)&version=202405.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=0dd4af3e-d12d-4d54-bd59-2bbe8de5147b&interactionCount=1&isAnonUser=1&landingPath=https%3A%2F%2Fwww.investing.com%2Facademy%2Fstock-picks%2Finvestingpro-subscription-pricing-value%2F&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1; finboxio-production:refresh=555acdf3-3383-46ac-b082-1fc3461dd310; finboxio-production:refresh.sig=G4eakNwr4kXUs7LhPUE-sd4HYcs; workstation_watchlist_opened=1; comment_notification_263020933=1; Adsfree_conversion_score=0; adsFreeSalePopUp65b250503b7670d99695a19fc04453f7=1; usprivacy=1YNN; __stripe_mid=42388d8d-4141-49a2-8117-db2e0dec9e52b13983; finbox-visitor-id=v-m1q-YgoYUR9juQA52hQer; geoC=HU; gtmFired=OK; nyxDorf=ZGA2Zmc1ZSdlMWlmYDtjfz9pYTtkfTAwNTZmYg%3D%3D; __cflb=02DiuGRugds2TUWHMkkPGro65dgYiP1884dT36e9c69mv; smd=bc7011112c2a9001709c3072aa550f97-1728329279; UserReactions=true; reg_trk_ep=pro%20top%20bar%20sign%20in; adsFreeSalePopUp=1; gcc=HU; gsc=BU; _imntz_error=0; login_method=email; ses_id=ZSsyc25hMztlIWlvN2Y5PTJgMmllYDY0YGhgYDYyY3VkcDI8bjliJDU6aScwMzgkPm1lZWMxMWNgNm5mMTU0ZmUyMjJuOjM6ZTVpbDdnOTMyMDJtZWE2MWA3YGM2ZGNvZGcyY246YmU1amkxMG04Nz4sZXljJzEgYDJuPjFwNHNlajJzbj4zaGUzaWE3ZzkzMjIybmVqNjJgN2BgNjNje2Qv; __cf_bm=8ZYYpdRS6JoB0wHk127HYHi7a1vw0xfsPa05S8my8mM-1728330482-1.0.1.1-bjrceq3NXzRzEm3cH02RSTEu90StmydLR3uG0u5MaSpgqly4ZRtCisa8RdSS.c.XCn03KcHc76cQPVD8txWc9ukpClSl4B3A0kVGWtVGC7E'
    request.headers["Cookie"] = cookie
    request.headers["Referer"] = "https://www.investing.com/"

async def fetch():
    # instantiate options for Chrome
    options = webdriver.ChromeOptions()

    # run browser in headless mode
    options.add_argument('--headless=new')

    # instantiate Chrome WebDriver with options
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

    # URL of the web page to scrape
    url = 'https://www.investing.com/pro/BUSE:OTP/explorer/marketcap'


    # open the specified URL in the browser
    driver.request_interceptor = interceptor
    driver.get(url)

    # find elements by class name 'product-name'
    tables = driver.find_elements(By.TAG_NAME, 'table')
    for table in tables:
        print(table.text)
        print("------")
    html = driver.execute_script("return document.documentElement.innerHTML;")
    ts = time.time()
    with open(f"./investing_{ts}.html", 'w') as file:
        file.write(html)

    # close the browser
    driver.quit()

if __name__ == '__main__':
    asyncio.run(fetch())