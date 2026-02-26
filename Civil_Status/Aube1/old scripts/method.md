The current scrapping pipeline is as follows

- aube_find_communs.py: 
    - Aube.Communes.txt: Lists the Communes

- aube_civil_status_by_commun.py: Uses the Aube.Communes as a que, to start in a different spot modify that file
    - Aube.csv: This is formated nicely, the number of images is used for creating variants of the download link
- validate_csv.py
    This is an attempt to clean up the data as best I can. This isn't a problem with the scraper, I just can't find
    a way to get the 
- table_to_pair.py: This modifies the nice datatable format to make a download file name and then a link for each image
    - This outputs another csv file
    
- downloader.py
    - folder of images


This scraper is getting shelved for now because the site lacks pretty essential information. The method I have used to get
the commune information results in duplicates and I is really a problem with the website. I did scrape all of the links
however to use we need to deal with the duplicates. If there are two copies the solution is probably just to choose a commune name
and just merge them. There is a case where there is a commune that is a combination of two neighboring communes.