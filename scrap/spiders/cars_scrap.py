import random
import scrapy
from urllib.parse import urljoin
from .mariaDBConnector import *
import datetime;


def get_scraping_links():
    today = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(1), '%Y-%m-%d')
    ad = get_TODAYs_DISTINCT_CAR_URLs(today)
    ids = []
    urls = []
    for i in range(len(ad)):
        urls.append(urljoin('https://fr.getaround.com/', ad[i][1]))
        ids.append(ad[i][0])
    return urls, ids


class PriceSpider(scrapy.Spider):
    proxy_pool = ['lum-customer-hiveway-zone-laketahoe1:srje9g0dmd2h@zproxy.lum-superproxy.io:22225',
                  'lum-customer-hiveway-zone-laketahoe2:iw77mv49ir8e@zproxy.lum-superproxy.io:22225',
                  'lum-customer-hiveway-zone-laketahoe3:8360k363yo78@zproxy.lum-superproxy.io:22225',
                  'lum-customer-hiveway-zone-laketahoe4:sxhq8w7srio1@zproxy.lum-superproxy.io:22225']
    name = "cars"
    urls, ids = get_scraping_links()
    id = ''

    def start_requests(self):
        for i in range(len(self.urls)):
            self.id = self.ids[i]
            yield scrapy.Request(self.urls[i],
                                 callback=self.parse_api,
                                 meta={'proxy': random.choice(self.proxy_pool)})

    def parse_api(self, response):
        CAR_ID = self.id
        raw_data = response.css('span.car_info_header__attributes ::text').extract()
        print(raw_data)
        MODEL_YEAR = raw_data[1].replace("\\n", "")
        NO_OF_SEATS = raw_data[2].replace("\\n", "")
        raw_data = response.css('div.car_technical_features p.cobalt-text-body::text').extract()
        ENGINE_TYPE = raw_data[0]
        MILEAGE = raw_data[1]
        TRANSMISSION = raw_data[2]
        PLATFORM = "Get around"
        PLATFORM_ID = "001"
        MODEL = response.css('h1.car_info_header__title::text').get()
        ADDRESS = response.css('div.location_section_address__content div.cobalt-text--emphasized *::text').get()
        insert_car(CAR_ID,PLATFORM,PLATFORM_ID,MODEL,MODEL_YEAR,NO_OF_SEATS,TRANSMISSION,ENGINE_TYPE,MILEAGE,ADDRESS)
