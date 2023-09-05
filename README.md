# gs-scraper
scrapes schools from https://www.goodschools.com.au/

#### Configuration
Configure the following settings for the scraper using the settings json file:
- thread_num:
    - the number of threads to be allocated scraping tasks
    - takes an integer as value

- full_address:
    - whether the scraper should get the full address or just use state and city as address
    - takes either true or false asb a value

- filter:
    - how the results should be filtered as per the site criteria of sectors and levels

    - sectors:
        - determine whether to scrape schools from government, independent or catholic sectors
        - values are ["g", "i", "c"]
            ```
            g = government
            i = independent
            c = catholic
            ```
        - Remove any of the values from the list if you do not want schools from the sector it represents
    
    - levels:
        - determine whether to scrape schools from primary, secondary or combined levels
        - values are ["p", "s", "c"]
            ```
            p = primary
            s = secondary
            c = combined
            ```
        - Remove any of the values from the list if you do not want schools from the level it represents
    

#### Usage
- Requires python 3.10+
- Open the terminal
- change terminal's directory into the project directory
- If running for the first time, install dependencies using the command:
    
    ```pip install -r requirements.txt```

- Run the script using the command:
    - For linux/mac:
        
        ```python3 main.py```

    - For windows:
        
        ```python main.py```