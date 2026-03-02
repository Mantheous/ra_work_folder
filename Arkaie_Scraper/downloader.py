import os
import piexif
import pandas as pd
import requests

FRI_FOLDER_PATH = "ra_work_folder/Civil_Status/Results" #"W:\\papers\\curent\\french_records\\french_record_images\\"
SHORT_URL = "https://www.archives-aube.fr/"

# Path: W:\papers\curent\french_records\french_record_images\{Department (Name)}\{Record_types}\{Commune}\{Period}\{Department}_{Record_types}_{Commune}_{Batch}_{page}

def download(file_path):

    column_headers = ['department','page','index','cote', 'commune', 'period', 'record_type', 'count', 'url']
    df = pd.read_csv(file_path, header=None, delimiter='|', names=column_headers)
    for row in df.itertuples():
        # Sanitize each component to remove spaces
        # TODO Remove this after Aube2 is scraped. I fixed it upstream
        dept = str(row.department).replace(" ", "_")
        r_type = str(row.record_type).replace(" ", "_")
        commune = str(row.commune).replace(" ", "_")
        period = str(row.period).replace(" ", "_")

        # check to see if there is already a file with the same path
        folder_path = os.path.join(FRI_FOLDER_PATH, dept, r_type, commune, period)
        os.makedirs(folder_path, exist_ok=True)

        batch = 0 
        base_filename = f"{dept}_{r_type}_{commune}_"
        
        # Determine batch number
        while os.path.isfile(os.path.join(folder_path, f"{base_filename}{batch}_0.jpg")):
            batch += 1

        for page in range(row.count):
            mod_link = row.url.replace("/0/full", f"/{page}/full")
            download_path = os.path.join(folder_path, f"{base_filename}{batch}_{page}.jpg")
            try:
                # 1. Download image
                response = requests.get(mod_link, timeout=30)
                response.raise_for_status() # Check for 404, 500, etc.

                # 2. Save binary content
                with open(download_path, 'wb') as f:
                    f.write(response.content)

                # 3. Metadata
                insert_metadata(image_path=download_path, cote=row.cote, url=SHORT_URL)

            except requests.exceptions.RequestException as e:
                print(f"Failed to download page {page} at {mod_link}: {e}")
            except Exception as e:
                print(f"Error processing page {page}: {e}")


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
    download('ra_work_folder\Civil_Status\Aube2\Aube2_cleaned.csv')