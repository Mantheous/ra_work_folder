import os
import piexif
import pandas as pd
import asyncio
import aiohttp
import logging

FRI_FOLDER_PATH = "ra_work_folder/Civil_Status/Results" #"W:\\papers\\curent\\french_records\\french_record_images\\"
SHORT_URL = "https://www.archives-aube.fr/"
CONCURRENCY_LIMIT = 20
TRIES = 5

# Path: W:\papers\curent\french_records\french_record_images\{Department (Name)}\{Record_types}\{Commune}\{Period}\{Department}_{Record_types}_{Commune}_{Batch}_{page}

# Setup Logging for Failed Downloads
logging.basicConfig(
    filename='failed_downloads.log',
    level=logging.ERROR,
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

            if attempt == 0:
                print(f"Retrying in 60s | {mod_link} | {download_path}")
                await asyncio.sleep(60)

        logging.error(f"Giving up after retry | {mod_link} | {download_path}")
        print(f"Giving up after retry | {mod_link} | {download_path}")

async def run_downloader(file_path):
    """Parses a CSV to extract records and asynchronously download their constituent images."""
    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'count', 'url']
    df = pd.read_csv(file_path, header=None, delimiter='|', names=column_headers)
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for row in df.itertuples():
            # Sanitize each component to remove spaces
            dept = str(row.department).replace(" ", "_")
            r_type = str(row.record_type).replace(" ", "_")
            commune = str(row.commune).replace(" ", "_")
            period = str(row.period).replace(" ", "_")
            cote = str(row.cote).replace(" ", "_")

            # Folder path stays the same
            folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)
            os.makedirs(folder_path, exist_ok=True)

            for page in range(row.count):
                download_path = os.path.join(folder_path, f"{cote}_{page}.jpg")
                if os.path.isfile(download_path):
                    continue  # already downloaded, skip
                mod_link = row.url.replace("/0/full", f"/{page}/full")
                tasks.append(download_page(session, semaphore, mod_link, download_path, row.cote))

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
    csv_path ='ra_work_folder/Civil_Status/Aube2/Aube2_progress.csv'
    asyncio.run(run_downloader(csv_path))