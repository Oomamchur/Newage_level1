import asyncio
import io
import time

import aiohttp
import gspread
import pandas as pd
from PIL import Image
from tqdm import tqdm

URL = "https://docs.google.com/spreadsheets/d/1QX2IhFyYmGDFMvovw2WFz3wAT4piAZ_8hi5Lzp7LjV0/edit#gid=1902149593"


async def parse_single_size(session: aiohttp.ClientSession, url: str) -> dict:
    page = {"image_url": url, "SIZE": None}
    try:
        async with session.get(url) as response:
            data = await response.read()
            image = Image.open(io.BytesIO(data))
            page["SIZE"] = f"{image.size[0]}x{image.size[1]}"
    except Exception as e:
        pass
    return page


async def update_worksheet(worksheet, data_chunk) -> None:
    async with aiohttp.ClientSession() as session:
        tasks = [
            parse_single_size(session, item["image_url"])
            for item in data_chunk
        ]
        chunk_result = await asyncio.gather(*tasks)
        dataframe = pd.DataFrame(chunk_result)
        dataframe.fillna("Invalid Size", inplace=True)
        worksheet.append_rows(
            dataframe.values.tolist(), value_input_option="RAW"
        )


def split_data_into_chunks(data: list[dict]) -> list[dict]:
    for i in range(0, len(data), 1000):
        yield data[i:i + 1000]


async def main():
    gc = gspread.service_account(filename="credentials.json")
    sh = gc.open_by_url(URL)
    data = sh.sheet1.get_all_records()

    new_sh = gc.create("new_age_level1")
    new_sh.share("oomamchur@gmail.com", perm_type="user", role="writer")
    worksheet = new_sh.sheet1

    chunked_data = list(split_data_into_chunks(data))

    for chunk in tqdm(chunked_data, desc="Processing data chunks"):
        await update_worksheet(worksheet, chunk)

    print("Check your email")


if __name__ == "__main__":
    start = time.perf_counter()
    asyncio.run(main())
    end = time.perf_counter()
    print("Elapsed:", end - start)
