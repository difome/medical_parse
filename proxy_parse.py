import aiohttp
import asyncio
import json
import logging
import os
import random
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict
from typing import List, Optional
from glob import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

os.makedirs('temp_p', exist_ok=True)
os.makedirs('result_p', exist_ok=True)

@dataclass
class MedicalInstitution:
    name: str
    url: str
    city: str

async def fetch_proxies() -> List[str]:
    proxy_url = 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt'
    async with aiohttp.ClientSession() as session:
        async with session.get(proxy_url) as response:
            proxies = await response.text()
            return [proxy.strip() for proxy in proxies.splitlines()]

async def fetch_page(session: aiohttp.ClientSession, url: str, proxies: List[str]) -> str:
    while proxies:
        proxy = random.choice(proxies)
        try:
            logging.info(f"Fetching {url} using proxy {proxy}")
            async with session.get(url, proxy=f"socks5://{proxy}", timeout=10) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    logging.error(f"Error fetching {url}: status {response.status} with proxy {proxy}")
        except Exception as e:
            logging.error(f"Error fetching {url} with proxy {proxy}: {e}")
        finally:
            proxies.remove(proxy) if proxy in proxies else None
    return ""

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
    temp_filename = os.path.join('temp_p', filename)
    with open(temp_filename, 'w', encoding='utf-8') as f:
        json.dump([asdict(inst) for inst in data], f, ensure_ascii=False, indent=4)

async def worker(session: aiohttp.ClientSession, proxies: List[str], base_url: str, start_page: int, end_page: Optional[int] = None):
    current_page = start_page
    while current_page <= end_page or end_page is None:
        page_url = f"{base_url}&start={current_page * 40}"
        html = await fetch_page(session, page_url, proxies)
        if html:
            institutions = await parse_medical_institutions(html)
            await save_to_file(institutions, f'medical_institutions_{current_page}.json')
        current_page += 1
        await asyncio.sleep(3)

def combine_all_data():
    all_data = []
    for temp_file in glob('temp_p/*.json'):
        with open(temp_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_data.extend(data)
    with open('result_p/combined_medical_institutions.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

async def main(base_url: str, total_pages: int, workers: int):
    proxies = await fetch_proxies()
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            pages_per_worker = total_pages // workers
            for i in range(workers):
                start_page = i * pages_per_worker
                end_page = start_page + pages_per_worker - 1 if i < workers - 1 else total_pages
                task = asyncio.create_task(worker(session, proxies, base_url, start_page, end_page))
                tasks.append(task)
            await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    combine_all_data()

if __name__ == '__main__':
    base_url = "https://zdorov-info.com.ua/meduchrezhdenija.html?_route_=meduchrezhdenija.html"
    total_pages = 186
    workers = 5
    asyncio.run(main(base_url, total_pages, workers))
