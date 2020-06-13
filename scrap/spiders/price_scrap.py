import random
import scrapy
import json
import datetime
from urllib.parse import urljoin
from .mariaDBConnector import *
from scrapy.http import JsonRequest


# from scrapy.utils.response import open_in_browser
def get_scraping_links():
    today = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(1), '%Y-%m-%d')
    tomorrow = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(2), '%Y-%m-%d')
    search_time = '&start_date=' + today + '&start_time=12A00&end_date=' + tomorrow + '&end_time=12A00'
    urls = []
    static_addresses = get_STATIC_ADDRESSES()
    for i in range(len(static_addresses)):
        x = static_addresses[i][2].split()
        url = "search.json?address=" + static_addresses[i][2] + "&address_source=google&poi_id=&latitude=" + \
              static_addresses[i][0] + "&longitude=" + static_addresses[i][1] + "&city_display_name=" + x[
                  -2].replace(",", "")
        url = url.replace(" ", "+")
        url = url.replace(",", "%2C")
        url = url.replace("\"", "")
        url = url.replace("\n", "")
        urls.append('https://fr.getaround.com/' +
                    url +
                    search_time +
                    '&country_scope=FR&car_sharing=true&user_interacted_with_car_sharing=false&administrative_area=%C3%8Ele-de-France')
    return urls


def get_add_index(urlX):
    index = 0
    static_addresses = get_STATIC_ADDRESSES()
    for data in static_addresses:
        index = index + 1
        lat = ""
        long = ""
        for i in urlX.split("&"):
            if i.split("=")[0] == "latitude":
                lat = i.split("=")[1]
        for i in urlX.split("&"):
            if i.split("=")[0] == "longitude":
                long = i.split("=")[1]

        if (lat == data[0] and long == data[1]):
            return index


class PriceSpider(scrapy.Spider):
    proxy_pool = []
    name = "price"
    allowed_domains = ["fr.getaround.com"]
    urls = get_scraping_links()
    Page = 0

    def start_requests(self):

        for i in range(len(self.urls)):
            self.Page = 1
            yield JsonRequest(self.urls[i],
                              callback=self.parse_api,
                              meta={'proxy': random.choice(self.proxy_pool)})

    def parse_api(self, response):
        raw_data = response.body
        data = json.loads(raw_data)
        dist_data = response.css('span.car_card_revamp__distance::attr(title)').getall()
        # for pagination
        x = 0
        isPage = False
        for i in response.url.split("&"):
            if i.split("=")[0] == "page":
                isPage = True
                self.Page = i.split("=")[1]
        if (isPage == False):
            self.Page = 1

        for car in data['cars']:
            rdist = dist_data[x].split()
            CAR_ID = car['id']
            MODEL = car['carTitle']
            PRICE = car['humanPrice'].replace("€", "")
            DISTANCE = rdist[1] + rdist[2]
            SCRAPED_DATE = datetime.datetime.today()
            STARTING_DATE = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(1),
                                                       '%Y-%m-%d')
            CAR_DETAIL = car['carPreviewUrl']
            insert_searched_result(CAR_ID,
                                   get_add_index(response.url),
                                   DISTANCE,
                                   self.Page,
                                   SCRAPED_DATE,
                                   STARTING_DATE,
                                   '1',
                                   PRICE,
                                   CAR_DETAIL)

            x += 1

        next_page = response.css('div.search_pagination a::attr(rel)').getall()

        if (next_page[-1] == '\\"next\\"'):
            next_page = response.css('div.search_pagination a::attr(href)').getall()[-1].replace("\\\"", "")

            yield JsonRequest(urljoin('https://fr.getaround.com', next_page.replace("search?", "search.json?")),
                              callback=self.parse_api,
                              meta={'proxy': random.choice(self.proxy_pool)})
