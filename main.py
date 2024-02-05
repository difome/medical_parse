import os

import aiohttp
import asyncio
import json
import logging
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from typing import List

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class MedicalInstitution:
    name: str
    url: str
    city: str

async def fetch_page(session: aiohttp.ClientSession, url: str) -> str:
    logging.info(f"Fetching {url}")
    async with session.get(url) as response:
        return await response.text()

async def parse_medical_institutions(html: str) -> List[MedicalInstitution]:
    soup = BeautifulSoup(html, 'html.parser')
    institutions = []

    for entry in soup.find_all('tr', class_='sectiontableentry'):
        name_data = entry.find('th', class_='table-titles')
        name = name_data.text.strip()
        url = name_data.find('a')['href']
        city_data = entry.find_all('td')
        city = city_data[1].text.strip() if len(city_data) > 1 else "Не указан"
        institutions.append(MedicalInstitution(name=name, url=f'https://zdorov-info.com.ua{url}', city=city))
        logging.info(f"Processed: {city} - {name} - {url}")

    return institutions

async def save_to_file(data: List[MedicalInstitution], filename: str):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump([asdict(inst) for inst in data], f, ensure_ascii=False, indent=4)

async def read_last_processed_info() -> dict:
    try:
        with open('temp_page.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"last_processed_start": 0, "last_processed_page": 0}  # Default values


async def main(base_url: str):
    try:
        async with aiohttp.ClientSession() as session:
            last_info = await read_last_processed_info()
            current_page = last_info["last_processed_page"]
            current_page_url = f"{base_url}&start={last_info['last_processed_start']}" if last_info["last_processed_start"] else base_url
            all_institutions = []
            while current_page_url:
                logging.info(f"Processing URL: {current_page_url} (Page {current_page})")
                html = await fetch_page(session, current_page_url)
                soup = BeautifulSoup(html, 'html.parser')

                institutions = await parse_medical_institutions(html)
                all_institutions.extend(institutions)
                await save_to_file(all_institutions, 'medical_institutions.json')

                # Identify the current page and next page in the pagination block
                current_page_span = soup.select_one('.pageslinks span.pagenav')
                if current_page_span:
                    # Find the next page link; it's typically right after the current page span
                    next_page_link = current_page_span.find_next_sibling('a', class_='pagenav')
                else:
                    next_page_link = None  # No current page span found, might be the last page

                if next_page_link and 'href' in next_page_link.attrs:
                    next_page_start = next_page_link['href'].split("start=")[-1]
                    with open('temp_page.json', 'w') as f:
                        json.dump({'last_processed_start': next_page_start, 'last_processed_page': current_page + 1}, f)
                    current_page_url = "https://zdorov-info.com.ua" + next_page_link['href']
                else:
                    # No more pages or reached the last page
                    current_page_url = None  # Stop the loop
                    if os.path.exists('temp_page.json'):
                        os.remove('temp_page.json')  # Delete temp file when done

                current_page += 1  # Increment the page counter
                await asyncio.sleep(2)  # Delay between processing pages


    except Exception as e:
        logging.error(f"An error occurred: {e}")
if __name__ == '__main__':
    base_url = "https://zdorov-info.com.ua/meduchrezhdenija.html?_route_=meduchrezhdenija.html"
    asyncio.run(main(base_url))
