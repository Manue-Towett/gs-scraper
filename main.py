import os
import re
import json
import threading
from queue import Queue
from datetime import date
from typing import Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

from utils import Logger

SCHOOLS = list[dict[str, str]]

PARAMS = {
    "keywords": "",
    "distance": "100km",
    "page": "2"
}

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Origin": "https://www.goodschools.com.au/",
    "Referer": "https://www.goodschools.com.au/compare-schools/search/primary-school-and-high-school?keywords=&distance=&site_type=&grade_fee_range_grade=kinder_domestic&grade_fee_range_min=0&grade_fee_range_max=50000",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
}

class GSScraper:
    """Scrapes schools from https://www.goodschools.com.au/"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*"*5 + __class__.__name__ + "*"*5)

        self.schools = []
        self.crawled = []
        self.url_queue = []
        self.crawled_addresses = []

        self.queue = Queue()
        self.address_queue = Queue()

        self.settings = self.__get_settings()

        self.base_url = "https://www.goodschools.com.au/compare-schools/search"

    def __get_settings(self) -> dict[str, str|list[str]]:
        """Fetches settings from the config file"""
        with open("./settings/settings.json", "r") as file:
            return json.load(file)

    def __fetch_page(self, 
                     url: str, 
                     params: Optional[dict[str, str]] = None) -> BeautifulSoup:
        """Fetches the webpage with the given url"""
        for _ in range(3):
            try:
                response = requests.get(url, params=params, headers=HEADERS)
                
                if response.ok:
                    return BeautifulSoup(response.text, "html.parser")
                
            except:
                if params:
                    self.logger.warn(
                        f"Error fetching info from page {params}. Retrying...")
                else:
                    self.logger.warn(
                        f"Error fetching info from {url}. Retrying...")
        
        self.logger.error(
            "FATAL ERROR: Failed to retrieve info after three attempts!")

    def __extract_schools(self, soup: BeautifulSoup) -> SCHOOLS:
        """Extracts schools from html text"""
        schools = []

        for school_tag in soup.select("div#search-results > div.row"):
            try:
                school: dict[str, str] = {
                    "ID": "",
                    "URL": school_tag.select_one("h5").parent["href"],
                    "NAME": school_tag.select_one("h5").get_text(strip=True),
                    "ADDRESS": school_tag.select_one("p.primary-site").get_text(strip=True),
                    "CITY": "",
                    "SCHOOL TYPE": "",
                    "LEVEL CODE": ""
                }

                state = re.search(r"[A-Z]{2,}", school["ADDRESS"]).group()
                school["CITY"] = school["ADDRESS"].split(state)[0].strip()

                school["ID"] = re.search(r"\d+", school["URL"]).group()

                for div in school_tag.select("div"):
                    try:
                        if re.search(r"level", div.select_one("b").text, re.I):
                            school["LEVEL CODE"] = " ".join(div.get_text().strip().split(" ")[1:])

                        if re.search(r"sector", div.select_one("b").text, re.I):
                            school["SCHOOL TYPE"] = " ".join(div.get_text().strip().split(" ")[1:])

                    except:pass
                
                schools.append(school)

            except:pass
        
        return schools

    def __get_full_address(self) -> None:
        """Gets the full address for a given school"""
        while True:
            school = self.address_queue.get()

            soup = self.__fetch_page(school["URL"])

            school["ADDRESS"] = soup.select_one("span.map-address")["data-address"]

            self.url_queue.remove(school)
            self.crawled_addresses.append(school)

            args = (len(self.url_queue), len(self.crawled_addresses))

            self.logger.info(
                "Queued Schools: {} || Crawled Schools: {}".format(*args))

            self.address_queue.task_done()

    def __save_to_csv(self) -> None:
        """Saves data retrieved to a csv file"""
        self.logger.info("Saving data retrieved to csv...")

        if not os.path.exists("./data/"):
            os.makedirs("./data/")
        
        filename = "results_{}.csv".format(date.today())

        df = pd.DataFrame(self.schools).drop_duplicates()
        
        df.to_csv("./data/{}".format(filename), index=False)

        self.logger.info("{} records saved to {}".format(len(df), filename))

    def __create_jobs(self, 
                      items: list[str|dict[str, str]], 
                      for_address: Optional[bool] = False) -> None:
        """Puts items in the queue"""
        if not for_address:
            queue = self.queue
        else:
            queue = self.address_queue
        
        [queue.put(item) for item in items]
        [self.url_queue.append(item) for item in items]

        queue.join()

    def work(self) -> None:
        """Work to be done by school fetching threads"""
        while True:
            page = self.queue.get()

            soup = self.__fetch_page(self.base_url, {**PARAMS, "page": str(page)})

            schools = self.__extract_schools(soup)

            for school in schools:
                if school in self.schools \
                    or school["LEVEL CODE"] in self.settings["level_code_blacklist"] \
                        or school["SCHOOL TYPE"] in self.settings["school_type_blacklist"]:
                    continue

                self.schools.append(school)

            self.crawled.append(page)
            self.url_queue.remove(page)

            args = (len(self.url_queue), len(self.crawled), len(self.schools))

            self.logger.info(
                "Queued Pages: {} || Crawled Pages: {} || Schools Found: {}".format(*args))
            
            self.queue.task_done()

    def scrape(self) -> None:
        """Entry point to the scraper"""
        for _ in range(self.settings["thread_num"]):
            threading.Thread(target=self.work, daemon=True).start()
            threading.Thread(target=self.__get_full_address, daemon=True).start()
        
        soup = self.__fetch_page(self.base_url, params=PARAMS)

        number_of_pages = soup.select("li.page-item")[-2].a.get_text(strip=True)

        self.__create_jobs(list(range(1, int(number_of_pages.replace(",", "")))))

        if self.settings["full_address"]:
            self.logger.info("Fetching full addresses...")

            self.__create_jobs(self.schools, for_address=True)
        
        self.__save_to_csv()

if __name__ == "__main__":
    scraper = GSScraper()
    scraper.scrape()