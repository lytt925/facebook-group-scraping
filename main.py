import concurrent.futures
from timeit import default_timer as timer
from facebook_scraper import *
from tqdm import tqdm
import pandas as pd
import re
import pygsheets


def process_post(item, desired_keys, progress):
    # keep track of the progress
    progress.update(1)

    for post in get_posts(post_urls=[item['post_id']], cookies="www.facebook.com_cookies.json"):
        # Extract the desired key-value pairs from the item
        extracted_dict = {key: post[key] for key in desired_keys if key in post}
        try:
            extracted_dict['pheonix'] = "鳳凰電波" in extracted_dict['text'] and extracted_dict['likes'] > 100
        except:
            extracted_dict['pheonix'] = None
            print(extracted_dict)


    # extracted_dict = {key: item[key] for key in desired_keys if key in item}
    
    return extracted_dict

def scrape(num_post, group_id):

    results = []
    progress = tqdm(total=num_post)
    
    start_url = None
    def handle_pagination_url(url):
        global start_url
        start_url = url
        if results:
            print(f"{len(results)}: {results[-1]['time']}: {start_url}")

    posts = get_posts(group=group_id, 
                    page_limit=300, 
                    start_url=start_url,
                    cookies="www.facebook.com_cookies.json",
                    request_url_callback=handle_pagination_url, 
                    options={"allow_extra_requests": False})


    num_workers = 15  # Adjust the number of workers as per your requirements
    desired_keys = ['post_id', 'text', 'likes', 'time', 'post_url']
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for post in posts:
            futures.append(executor.submit(process_post, post, desired_keys, progress))
            if len(futures) >= num_post:
                break

        # Retrieve the results from the completed futures
        for future in futures:
            result = future.result()
            results.append(result)

    df = pd.DataFrame(results)
    return df


def main():
    gc = pygsheets.authorize(client_secret='client_secret.json')

    
    # Open the Google Sheet by title
    sheet_title = 'bevenus 3000 posts'
    # Find the Google Sheets file by name
    try:
        sheet = gc.open(sheet_title)
        print("Found the sheet:", sheet.title)
    except pygsheets.SpreadsheetNotFound:
        print("The sheet with the name '{}' was not found. Creating a new one.".format(sheet_title))
        
        # Create a new spreadsheet with the given name
        sheet = gc.create(sheet_title)
        print("New sheet created with the title:", sheet.title)


    df = scrape(3000, group_id=686552765061367)
    
    worksheet = sheet[0]
    # Write the DataFrame to the Google Sheet starting from cell 'A1'
    worksheet.set_dataframe(df, start='A1')


if __name__ == '__main__':
    main()