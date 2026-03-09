import os
import piexif
import pandas as pd
import asyncio
import aiohttp
import logging

FRI_FOLDER_PATH = "ra_work_folder/Civil_Status/Results"
SHORT_URL = "https://www.archives-aube.fr/"
CONCURRENCY_LIMIT = 20

# Setup Logging for Failed Downloads
logging.basicConfig(
    filename='failed_missed_downloads.log',
    level=logging.ERROR,
    format='%(asctime)s - %(message)s'
)

async def download_page(session, semaphore, mod_link, download_path, cote):
    """Handles the actual download of a single image."""
    async with semaphore:
        try:
            async with session.get(mod_link, timeout=60) as response:
                if response.status == 200:
                    content = await response.read()
                    
                    # Save binary content
                    with open(download_path, 'wb') as f:
                        f.write(content)
                    
                    # Metadata
                    await asyncio.to_thread(insert_metadata, download_path, cote, SHORT_URL)
                    logging.info(f"Downloaded | {mod_link}")
                else:
                    logging.error(f"HTTP {response.status} | {mod_link}")
        except Exception as e:
            logging.error(f"Failed | {mod_link} | Error: {str(e)}")

async def run_downloader(file_path):
    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'count', 'url']
    # Use pipe delimiter as seen in missed_pages.csv
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

            # Folder path logic
            folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)
            os.makedirs(folder_path, exist_ok=True)

            batch = 0 
            base_filename = f"{dept}_{r_type}_{commune}_"
            
            # Determine batch number (same logic as original downloader)
            while os.path.isfile(os.path.join(folder_path, f"{base_filename}{batch}_0.jpg")):
                batch += 1

            # Use column 8 (row.count) for the filename page index
            page_idx = row.count
            mod_link = row.url
            download_path = os.path.join(folder_path, f"{base_filename}{batch}_{page_idx}.jpg")
            
            tasks.append(download_page(session, semaphore, mod_link, download_path, row.cote))

        await asyncio.gather(*tasks)

def insert_metadata(image_path, cote, url):
    """
    Inserts 'cote' into Title and Subject, and 'url' into Copyright.
    """
    zeroth_ifd = {
        piexif.ImageIFD.XPTitle: cote.encode('utf-16'),
        piexif.ImageIFD.XPSubject: cote.encode('utf-16'),
        piexif.ImageIFD.Copyright: url.encode('utf-8')
    }
    exif_dict = {"0th": zeroth_ifd}
    exif_bytes = piexif.dump(exif_dict)
    piexif.insert(exif_bytes, image_path)

if __name__ == "__main__":
    # Based on the user's provided path
    csv_path = 'w:/RA_work_folders/Ashton_Reed/ra_work_folder/Civil_Status/Aube2/missed_pages.csv'
    if os.path.exists(csv_path):
        asyncio.run(run_downloader(csv_path))
    else:
        print(f"CSV not found at {csv_path}")
