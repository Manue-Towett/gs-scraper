import re
import argparse
import threading
import configparser
from queue import Queue
from typing import Optional

import pandas as pd

from utils import Logger

config = configparser.ConfigParser()

with open("./settings/settings.ini", "r") as file:
    config.read_file(file)

INPUT_PATH = config.get("paths", "input")

OUTPUT_PATH = config.get("paths", "output")

class StatesFilter:
    """Filters schools in a csv by state"""
    def __init__(self, filename: str) -> None:
        self.logger = Logger(__class__.__name__)
        self.logger.info("****StatesFilter Started*****")

        self.queue = Queue()

        self.df = self.__read_csv(filename)

    def __read_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """Reads a csv from a given csv"""
        try:
            self.logger.info("Reading the file >>> {}".format(filename))

            df = pd.read_csv(f"{INPUT_PATH}{filename}")

            self.logger.info("Number of schools found: {}".format(len(df)))

            return df
        
        except:
            self.logger.error("FATAL: {} not found!".format(filename))

    def __filter_state(self, state: str) -> pd.DataFrame:
        """Filter a dataframe by a given state"""
        self.logger.info("Filtering schools from >>> {}".format(state))

        df = self.df[self.df["state"] == state].drop(columns="state")

        self.logger.info("Number of schools found in {}: {}".format(state, len(df)))

        return df
    
    def __save_to_csv(self, df: pd.DataFrame, state: str) -> None:
        """Saves the data obtained from a given state to csv"""
        self.logger.info("Saving data from {} to csv...".format(state))

        file_path = f"{OUTPUT_PATH}{state}_combined_.csv"

        df.to_csv(file_path, index=False)

        self.logger.info("{} records saved to {}".format(len(df), file_path))
    
    def work(self) -> None:
        """Work to be done by threads"""
        while True:
            state = self.queue.get()

            df = self.__filter_state(state)

            self.__save_to_csv(df, state)

            self.queue.task_done()

    def run(self) -> None:
        """Entry point to the filter"""
        self.logger.info("Extracting states from the data...")

        self.df["state"] = [re.search(r"[A-Z]{2,}", address).group() 
                            for address in self.df["ADDRESS"]]
        
        states = set(self.df["state"].to_list())

        self.logger.info(f"Number of states found: {len(states)} "
                         "|| Filtering schools for each state...")

        [threading.Thread(target=self.work, daemon=True).start() 
         for _ in range(len(states))]
        
        [self.queue.put(state) for state in states]

        self.queue.join()

        self.logger.info("Done.")

parser = argparse.ArgumentParser(description="Split a csv file by state")

parser.add_argument("filename", help="The name of the file to be split")

filename = parser.parse_args().filename

if __name__ == "__main__":
    filter_app = StatesFilter(filename)
    filter_app.run()