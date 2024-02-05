from typing import List

import aiohttp
import asyncio
import json
import logging
import os
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict, field
import pandas as pd
from glob import glob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ensure the result directories exist
os.makedirs('result', exist_ok=True)
os.makedirs('result/detail', exist_ok=True)


@dataclass
class MedicalInstitutionDetail:
    phone: List[str] = field(default_factory=list)
    category: str = ""
    profile: str = ""
    city: str = ""
    address: List[str] = field(default_factory=list)
    hours: str = ""
    email: str = ""
    views: str = ""


async def fetch_page(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text() if response.status == 200 else ""


async def parse_details(html: str) -> MedicalInstitutionDetail:
    soup = BeautifulSoup(html, 'html.parser')
    infoblock = soup.select_one('.infoblock.onecols')
    details = {}

    for item in infoblock.find_all('li'):
        label = item.select_one('.label').text.strip()
        value = item.select_one('.value').get_text(separator="\n").split("\n")
        if "Телефон" in label:
            details['phone'] = value
        elif "Категория" in label:
            details['category'] = value[0]
        elif "Лечебный профиль" in label:
            details['profile'] = value[0]
        elif "Город" in label:
            details['city'] = value[0]
        elif "Адрес" in label:
            details['address'] = value
        elif "Время работы" in label:
            details['hours'] = value[0]
        elif "E-mail" in label:
            details['email'] = value[0]
        elif "Кол-во просмотров" in label:
            details['views'] = value[0]

    return MedicalInstitutionDetail(**details)


async def process_institution(institution, session, index):
    detail_html = await fetch_page(institution['url'])
    if detail_html:
        details = await parse_details(detail_html)
        filename = f"temp/details/details_{index}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(asdict(details), f, ensure_ascii=False, indent=4)


async def worker(institutions: List[dict], worker_id: int):
    async with aiohttp.ClientSession() as session:
        for index, institution in enumerate(institutions):
            await process_institution(institution, session, worker_id * 1000 + index)


def combine_all_data():
    all_data = []
    for temp_file in glob('temp/details/*.json'):
        with open(temp_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_data.extend(data)
    with open('result/combined_medical_institution_details.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)


async def convert_to_excel_and_csv():
    combined_json_path = 'result/combined_medical_institution_details.json'
    with open(combined_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    df.to_excel('result/data_details.xlsx', index=False)
    df.to_csv('result/data_details.csv', index=False)


async def main():
    with open('result/combined_medical_institutions.json', 'r', encoding='utf-8') as f:
        institutions = json.load(f)

    num_workers = 10
    chunk_size = len(institutions) // num_workers + (len(institutions) % num_workers > 0)
    tasks = []

    for i in range(num_workers):
        chunk = institutions[i * chunk_size:(i + 1) * chunk_size]
        task = asyncio.create_task(worker(chunk, i))
        tasks.append(task)

    await asyncio.gather(*tasks)

    combine_all_data()
    await convert_to_excel_and_csv()


if __name__ == '__main__':
    asyncio.run(main())
