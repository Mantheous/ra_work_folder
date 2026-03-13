import os
import piexif
import pandas as pd
import asyncio
import aiohttp
import logging

FRI_FOLDER_PATH = "W:\\papers\\current\\french_records\\french_record_images\\"
RUN_NAME = "Creuse"
SHORT_URL = "https://archives.creuse.fr/"
CSV_PATH ='ra_work_folder/Civil_Status/Creuse/Creuse_cleaned.csv'
CONCURRENCY_LIMIT = 1
TRIES = 50
CREATE_FOLDERS = False

# Path: W:\papers\curent\french_records\french_record_images\{Department (Name)}\{Record_types}\{Commune}\{Period}\{Department}_{Record_types}_{Commune}_{Batch}_{page}

# Setup Logging for Failed Downloads
logging.basicConfig(
    filename=f'failed_downloads_{RUN_NAME}.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

async def download_page(session, semaphore, mod_link, download_path, cote):
    """Handles the actual download of a single image, retrying once after 60s on failure."""
    async with semaphore:
        for attempt in range(TRIES):  # attempt 0 = first try, attempt 1 = retry
            try:
                async with session.get(mod_link, timeout=60) as response:
                    if response.status == 200:
                        content = await response.read()

                        # Save binary content (Blocking I/O, but small enough usually)
                        with open(download_path, 'wb') as f:
                            f.write(content)

                        # Metadata (Running in thread to keep loop moving)
                        await asyncio.to_thread(insert_metadata, download_path, cote, SHORT_URL)
                        logging.info(f"Downloaded | {mod_link} | {download_path}")
                        print(f"Downloaded | {mod_link} | {download_path}")
                        return  # Success — no retry needed
                    else:
                        logging.error(f"HTTP {response.status} | {mod_link} | {download_path}")
            except Exception as e:
                logging.error(f"Failed | {mod_link} | {download_path} | Error: {str(e)}")

            print(f"Retrying in 60s | {mod_link} | {download_path}")
            await asyncio.sleep(60)

        logging.error(f"Giving up after retry | {mod_link} | {download_path}")
        print(f"Giving up after retry | {mod_link} | {download_path}")

def build_tasks_for_row(row, session, semaphore):
    """Builds download tasks for a single row (runs in a thread for parallel isfile checks)."""
    dept = str(row.department).replace(" ", "_")
    r_type = str(row.record_type).replace(" ", "_")
    commune = str(row.commune).replace(" ", "_")
    period = str(row.period).replace(" ", "_")
    cote = str(row.cote).replace(" ", "_").replace("/", "%")

    folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)
    if CREATE_FOLDERS:
        os.makedirs(folder_path, exist_ok=True)

    row_tasks = []
    for page in range(row.count):
        download_path = os.path.join(folder_path, f"{cote}_{page}.jpg")
        if os.path.isfile(download_path):
            logging.info(f"Already downloaded | {row.url} | {download_path}")
            continue  # already downloaded, skip
        mod_link = row.url.replace("/0/full", f"/{page}/full")
        row_tasks.append(download_page(session, semaphore, mod_link, download_path, row.cote))
    return row_tasks

async def run_downloader(file_path):

    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'count', 'url']
    df = pd.read_csv(file_path, header=None, delimiter='|', names=column_headers, encoding='utf-8')

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        print(f"Building tasks for {len(df)} rows in parallel...")
        # Run all rows' isfile checks concurrently in a thread pool
        row_task_lists = await asyncio.gather(
            *[asyncio.to_thread(build_tasks_for_row, row, session, semaphore)
              for row in df.itertuples()]
        )

        # Flatten the list of lists into a single task list
        tasks = [t for row_tasks in row_task_lists for t in row_tasks]
        print(f"Running {len(tasks)} download tasks...")
        await asyncio.gather(*tasks)


def insert_metadata(image_path, cote, url):
    """
    Inserts 'cote' into Title and Subject, and 'url' into Copyright.
    """
    # 1. Prepare the tags
    # XP tags (Title/Subject) need UTF-16
    # Standard tags (Copyright) usually take UTF-8 or ASCII
    zeroth_ifd = {
        piexif.ImageIFD.XPTitle: cote.encode('utf-16'),
        piexif.ImageIFD.XPSubject: cote.encode('utf-16'),
        piexif.ImageIFD.Copyright: url.encode('utf-8')
    }
    
    # 2. Bundle into the EXIF structure
    exif_dict = {"0th": zeroth_ifd}
    
    # 3. Convert to bytes
    exif_bytes = piexif.dump(exif_dict)
    
    # 4. Inject into the image file
    piexif.insert(exif_bytes, image_path)

if __name__ == "__main__":
    asyncio.run(run_downloader(CSV_PATH))