import os
from collections import defaultdict

# List the TSV files to merge
tsv_files = [
    "aggregated_page_views_20230101-20230201.tsv",
    "aggregated_page_views_20230201-20230301.tsv",
    "aggregated_page_views_20230301-20230401.tsv",
    "aggregated_page_views_20230401-20230501.tsv",
]

# Initialize a dictionary to store the aggregated page views
merged_page_views = defaultdict(int)

# Iterate over the TSV files and accumulate the page view counts
for tsv_file in tsv_files:
    with open(tsv_file, "r") as f:
        for line in f:
            parts = line.strip().split("\t")

            if len(parts) == 2:
                page_name, views = parts
                views = int(views)
                merged_page_views[page_name] += views
            else:
                print(f"Skipping malformed line in {tsv_file}: {line.strip()}")
# Write the merged page views to a new TSV file
output_file = "merged_page_views_20230101-20230501.tsv"
with open(output_file, "w") as f:
    for page_name, views in merged_page_views.items():
        f.write(f"{page_name}\t{views}\n")

print(f"Merged page views saved in {output_file}")
