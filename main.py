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

LEVEL_MAPPINGS = {
    "p": "primary",
    "s": "secondary",
    "c": "combined"
}

TYPE_MAPPINGS = {
    "g": "government",
    "i": "independent",
    "c": "catholic"
}

PARAMS = {
    "keywords": "",
    "page": "2"
}

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Origin": "https://www.goodschools.com.au/",
    "Referer": "https://www.goodschools.com.au/",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
}

class GSScraper:
    """Scrapes schools from https://www.goodschools.com.au/"""
    def __init__(self) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("*"*5 + __class__.__name__ + " started" + "*"*5)

        self.schools = []
        self.crawled = []
        self.url_queue = []
        self.crawled_addresses = []

        self.queue = Queue()
        self.address_queue = Queue()

        self.settings = self.__get_settings()

        self.base_url = "https://www.goodschools.com.au/compare-schools/search/{}"

    def __get_settings(self) -> dict[str, str|list[str]]:
        """Fetches settings from the config file"""
        with open("./settings/settings.json", "r") as file:
            return json.load(file)

    def __fetch_page(self, 
                     url: str, 
                     params: Optional[dict[str, str]] = None) -> BeautifulSoup:
        """Fetches the webpage with the given url"""
        while True:
            try:
                response = requests.get(url, params=params, headers=HEADERS, timeout=10)
                
                if response.ok:
                    return BeautifulSoup(response.text, "html.parser")
                
            except:
                if params:
                    self.logger.warn(
                        f"Error fetching info from page {params['page']}. Retrying...")
                else:
                    self.logger.warn(
                        f"Error fetching info from {url}. Retrying...")
    
    def __extract_params(self, soup: BeautifulSoup) -> None:
        """Extracts the filter params as per the settings"""
        index, sectors = 0, []

        for sector in soup.find_all("input", {"name": "sector_ids[]"}):
            if sector["value"] in sectors: 
                continue

            for config_sector in self.settings["filter"]["sectors"]:
                if re.search(TYPE_MAPPINGS[config_sector], sector["data-url-part"], re.I):
                    PARAMS[f"sector_ids[{index}]"] = sector["value"]

                    index += 1
            
            sectors.append(sector["value"])

        index, levels = 0, []

        for school in soup.find_all("input", {"name": "school_level_ids[]"}):
            if school["value"] in levels: 
                continue

            for config_sector in self.settings["filter"]["levels"]:
                if re.search(LEVEL_MAPPINGS[config_sector], school["data-url-part"], re.I):
                    PARAMS[f"school_level_ids[{index}]"] = school["value"]

                    index += 1
            
            levels.append(school["value"])

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

                address_list = school["ADDRESS"].split(state)

                address_list = list(filter(lambda x: x.strip(), 
                                           [part.lstrip(",").strip() for part in address_list]))

                address_list.insert(1, state)

                school["ADDRESS"] = ", ".join(address_list)

                school["ID"] = re.search(r"\d+", school["URL"]).group()

                for div in school_tag.select("div"):
                    try:
                        if re.search(r"level", div.select_one("b").text, re.I):
                            level = div.get_text().strip()

                            school["LEVEL CODE"] = " ".join(level.split(" ")[1:])

                        if re.search(r"sector", div.select_one("b").text, re.I):
                            school_type = div.get_text().strip()

                            school["SCHOOL TYPE"] = " ".join(school_type.split(" ")[1:])

                    except:pass
                
                schools.append(school)

            except:pass
        
        return schools

    def __get_full_address(self) -> None:
        """Gets the full address for a given school"""
        while True:
            school = self.address_queue.get()

            soup = self.__fetch_page(school["URL"])

            if not soup:
                self.address_queue.task_done()
                
                continue

            school["ADDRESS"] = soup.select_one("span.map-address")["data-address"]

            self.url_queue.remove(school)
            self.crawled_addresses.append(school)

            args = (len(self.url_queue), len(self.crawled_addresses))

            self.logger.info(
                "Queued Schools: {} || Crawled Schools: {}".format(*args))

            self.address_queue.task_done()

    def __save_to_csv(self, schools: Optional[list] = []) -> None:
        """Saves data retrieved to a csv file"""
        self.logger.info("Saving data retrieved to csv...")

        if not os.path.exists("./data/"):
            os.makedirs("./data/")
        
        filename = "results_{}.csv".format(date.today())

        if not len(schools):
            schools = [school for school in self.schools]

        df = pd.DataFrame(schools).drop_duplicates()
        
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

            soup = self.__fetch_page(self.url, {**PARAMS, "page": str(page)})

            schools = self.__extract_schools(soup)

            for school in schools:
                school["LEVEL CODE"] = school["LEVEL CODE"][0].lower()

                if school in self.schools :
                    continue

                self.schools.append(school)

                school_list = [school for school in self.schools]

                if len(school_list) and (len(school_list) % 300) == 0:
                    self.__save_to_csv(school_list)

            self.crawled.append(page)
            self.url_queue.remove(page)

            args = (len(self.url_queue), len(self.crawled), len(self.schools))

            self.logger.info(
                "Queued Pages: {} || Crawled Pages: {} || Schools Found: {}".format(*args))
            
            self.queue.task_done()
    
    def __scrape(self, url_slug: str) -> None:
        """scrapes schools from the given url path"""
        if url_slug is not None:
            self.url = self.base_url.format(url_slug)
        else:
            self.logger.error("Please specify the levels in the filter settings!")
        
        soup = self.__fetch_page(self.url, params=PARAMS)

        self.__extract_params(soup)

        number_of_pages = soup.select("li.page-item")[-2].a.get_text(strip=True)

        self.__create_jobs(list(range(1, int(number_of_pages.replace(",", "")) + 1)))

        if self.settings["full_address"]:
            self.logger.info("Fetching full addresses...")

            self.__create_jobs(self.schools, for_address=True)

    def scrape(self) -> None:
        """Entry point to the scraper"""
        for _ in range(self.settings["thread_num"]):
            threading.Thread(target=self.work, daemon=True).start()

            if self.settings["full_address"]:
                threading.Thread(target=self.__get_full_address, daemon=True).start()
        
        level_slug, type_slug, url_slug = None, None, None

        for key, value in LEVEL_MAPPINGS.items():
            if key in self.settings["filter"]["levels"]:
                if level_slug is None:
                    level_slug = value
                else:
                    level_slug += f"-and-{value}"

        for key, value in TYPE_MAPPINGS.items():
            if key == "c":
                continue
            
            if key in self.settings["filter"]["sectors"]:
                if type_slug is None:
                    type_slug = value
                else:
                    type_slug += f"-and-{value}"

        if type_slug:
            self.logger.info(
                f"Scraping schools from {type_slug.replace('-', ' ')} sector")
            url_slug = type_slug
        
        if level_slug:
            if type_slug:
                url_slug += f"/{level_slug}"
            else:
                url_slug = level_slug
        
        self.__scrape(url_slug)

        if "c" in self.settings["filter"]["sectors"]:
            self.logger.info("Scraping schools from catholic sector")

            type_slug = TYPE_MAPPINGS['c']

            if level_slug is not None:
                url_slug = f"{type_slug}/{level_slug}"
            else:
                url_slug = type_slug
            
            self.__scrape(url_slug)
        
        self.__save_to_csv()

if __name__ == "__main__":
    scraper = GSScraper()
    scraper.scrape()