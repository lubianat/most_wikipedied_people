import os
import gzip
from datetime import datetime, timedelta
from tqdm import tqdm
from pathlib import Path
import json

HERE = Path(__file__).parent.resolve()


def generate_urls(first_date, last_date):
    current_date = first_date
    urls = []
    while current_date <= last_date:
        formatted_date = current_date.strftime("%Y%m%d-%H%M%S")
        formatted_year_month = current_date.strftime("%Y-%m")
        formatted_year = current_date.strftime("%Y")

        url = f"https://dumps.wikimedia.org/other/pageviews/{formatted_year}/{formatted_year_month}/pageviews-{formatted_date}.gz"
        urls.append(url)
        current_date += timedelta(hours=1)
    return urls


def process_gz_file(url):
    gz_file = url.split("/")[-1]
    output_file = gz_file.replace(".gz", ".tsv")

    # Download the .gz file using wget
    os.system(f"wget {url}")

    # Unzip the .gz file and filter the lines starting with 'en'
    with gzip.open(gz_file, "rt") as f_in, open(output_file, "w") as f_out:
        for line in f_in:
            if line.startswith("en "):
                f_out.write(line)
                lang, page_name, views, na = line.strip().split(" ")
                views = int(views)
                if page_name in page_views:
                    page_views[page_name] += views
                else:
                    page_views[page_name] = views

    # Delete the .gz file and .tsv file
    os.remove(gz_file)
    os.remove(output_file)  # Remove the .tsv file


# Convert the input dates to datetime objects
first_date = datetime.strptime("20230401-000000", "%Y%m%d-%H%M%S")
last_date = datetime.strptime("20230501-000000", "%Y%m%d-%H%M%S")


# Format the first and last date for the output file name
first_date_str = first_date.strftime("%Y%m%d")
last_date_str = last_date.strftime("%Y%m%d")
output_file = f"aggregated_page_views_{first_date_str}-{last_date_str}.tsv"

# Generate the URLs for the specified date range
urls = generate_urls(first_date, last_date)
page_views = {}

# Process each .gz file with a progress bar
for url in tqdm(urls, desc="Processing .gz files"):
    process_gz_file(url)

# Save the aggregated view counts to a new file
with open(output_file, "w") as f:
    for page_name, views in page_views.items():
        f.write(f"{page_name}\t{views}\n")

print(f"Aggregated page views saved in {output_file}")
