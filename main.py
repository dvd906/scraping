import asyncio
import csv
import os
import re
from typing import Generator, Tuple, List, Iterable
from urllib import parse
from requests_html import AsyncHTMLSession
from difflib import SequenceMatcher, Match
from pandas import read_excel

XLSX_PATH = os.getcwd() + os.sep + 'assignment.xlsx'
RESULTS_PATH = os.getcwd() + os.sep + 'results.csv'
GOOGLE_DOMAINS = (
    'https://www.google.', 
    'https://google.', 
    'https://webcache.googleusercontent.', 
    'http://webcache.googleusercontent.', 
    'https://policies.google.',
    'https://support.google.',
    'https://maps.google.',
    'https://translate.google.com'
)
URL_REGEX = r"https?://[^\s]+\.\w{2,}/"

def read_next_account(xlsx_path: str) -> Generator[Tuple[str, str], None, None]:
    excel = read_excel(xlsx_path)
    for _, (account_name, _, address) in excel.iterrows():
        yield account_name, address
    return None

def get_longest_match(find: str, search_in: str) -> Match:
    s = SequenceMatcher(None, find, search_in)
    return s.find_longest_match(0, len(find), 0, len(search_in))

async def scrape_urls_from_google(account_name: str, address: str, session: AsyncHTMLSession = None) -> list:
    query = parse.quote_plus(f"{account_name} {address}".strip())
    if session is None:
        session = AsyncHTMLSession()
    response = await session.get(f"https://www.google.com/search?q={query}")
    links: List[str] = list(response.html.absolute_links)
    for url in links[:]:
        if url.startswith(GOOGLE_DOMAINS):
            links.remove(url)
    return links

async def search_possible_company_url(account_name: str, address: str) -> str:
    account_urls = await scrape_urls_from_google(account_name, '')
    address_urls = await scrape_urls_from_google('', address)
    combination_urls = await scrape_urls_from_google(account_name, address)
    urls = account_urls + address_urls + combination_urls
    unique_urls = set(
        map(
            lambda url: re.match(URL_REGEX, url)[0] if re.match(URL_REGEX, url) else '', urls
        )
    )
    urls_with_matches = {url: get_longest_match(account_name.lower(), url) for url in unique_urls}
    if not urls_with_matches:
        return ''

    max_val = max(urls_with_matches.values(), key=lambda element: element.size)
    for url, match_value in urls_with_matches.items():
        if match_value == max_val:
            return url

    return '' 

def create_csv_with_header():
    with open(RESULTS_PATH, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Account Name', 'Address', 'URL'])

def append_result(data: Iterable):
    with open(RESULTS_PATH, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(data)

async def company_scrape_task(account_name:str, address: str) -> Tuple[str, str, str]:
    url = await search_possible_company_url(account_name, address)
    print(f'{account_name}: {url}')
    return account_name, address, url

async def main():
    print('Scraping fun begins')
    create_csv_with_header()
    tasks = [
        asyncio.create_task(company_scrape_task(account_name, address))
        for account_name, address in read_next_account(XLSX_PATH)
    ]
    print('Tasks spawned')
    results = await asyncio.gather(*tasks)
    print('Tasks finished')
    for result in results:
        append_result(result)
    print('Scraping fun finished')
        

if __name__ == '__main__':
    asyncio.run(main())
    