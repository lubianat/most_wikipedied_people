import json
import os
from wdcuration import query_wikidata
import pandas as pd
import urllib.parse
from SPARQLWrapper import SPARQLWrapper, JSON
from tqdm import tqdm
import os
import pandas as pd
from datetime import datetime
import pickle
import os
import urllib


def generate_cache_filename(country_qid):
    return f"query_cache/cache_{country_qid}.json"


def save_to_cache(country_qid, data):
    with open(generate_cache_filename(country_qid), "w") as f:
        json.dump(data, f)


def load_from_cache(country_qid):
    with open(generate_cache_filename(country_qid), "r") as f:
        return json.load(f)


def get_people_from_a_country_from_wikidata(country_qid="Q155"):
    cache_file = generate_cache_filename(country_qid)
    print(cache_file)
    if os.path.exists(cache_file):
        print("Ok")
        with open(cache_file, "r") as f:
            article_names = json.load(f)
        return article_names

    query_template = """
    SELECT ?person ?article WHERE {{
        ?person wdt:P27 wd:{} . 
        ?person wdt:P31 wd:Q5 . # Instance of human
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/> .
        ?article schema:inLanguage "en" .
        {}
    }}
    LIMIT 1000000
    """

    additional_filter = (
        "?person wikibase:sitelinks ?sitelinks . FILTER(?sitelinks >= 2)"
        if country_qid == "Q30"
        else ""
    )

    query = query_template.format(country_qid, additional_filter)
    data = query_wikidata(query)
    article_names = [
        urllib.parse.unquote(
            article["article"].replace("https://en.wikipedia.org/wiki/", "")
        )
        for article in data
    ]

    save_to_cache(country_qid, article_names)

    return article_names


def get_most_viewed_for_country(
    country_qid, pageviews_dump="aggregated_page_views.tsv"
):
    df = pd.read_csv(pageviews_dump, sep="\t")
    df.columns = ["article", "pageviews"]
    article_names = get_people_from_a_country_from_wikidata(country_qid)

    article_names = [a.replace(" ", "_") for a in article_names]
    filtered_df = df[df["article"].isin(article_names)]

    # Find the index of the row with the highest pageviews
    try:
        max_pageviews_index = filtered_df["pageviews"].idxmax()
    except:
        try:
            return (article_names[0], 0)
        except IndexError:
            return ("None", 0)

    # Extract the row with the highest pageviews
    top_pageviews_row = filtered_df.loc[max_pageviews_index]

    return (top_pageviews_row.article, top_pageviews_row.pageviews)


if __name__ == "__main__":
    # define the endpoint URL for the Wikidata Query Service
    endpoint_url = "https://query.wikidata.org/sparql"

    # create a SPARQLWrapper object for the endpoint URL
    sparql = SPARQLWrapper(endpoint_url)

    # define the SPARQL query
    query = """
    SELECT DISTINCT ?country ?countryLabel
    WHERE {
      ?country wdt:P31 wd:Q6256.
      SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    """

    # set the query and response format for the SPARQLWrapper object
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    # execute the query and get the results as a JSON object
    results = sparql.query().convert()

    # extract the QIDs and names from the results and store them in a list of tuples
    qids_and_names = [
        (result["countryLabel"]["value"], result["country"]["value"].split("/")[-1])
        for result in results["results"]["bindings"]
    ]

    tuples = []
    flag = 0

    # Convert the input dates to datetime objects
    first_date = datetime.strptime("20230101-000000", "%Y%m%d-%H%M%S")
    last_date = datetime.strptime("20230501-000000", "%Y%m%d-%H%M%S")
    # Format the first and last date for the output file name
    first_date_str = first_date.strftime("%Y%m%d")
    last_date_str = last_date.strftime("%Y%m%d")

    # Define the filename with current date
    filename = f"output_{first_date_str}-{last_date_str}.tsv"

    # Check if file exists and create a new one if not
    if not os.path.isfile(filename):
        with open(filename, "w") as f:
            f.write("name\tpageviews\tcountry\n")

    # Read the data into a pandas dataframe
    df = pd.read_csv(filename, sep="\t")

    for name, country_qid in tqdm(qids_and_names):
        print(df)
        if name in list(df["country"]):
            continue
        print(name)

        country_tuple = get_most_viewed_for_country(
            country_qid,
            pageviews_dump=f"aggregated_page_views_{first_date_str}-{last_date_str}.tsv",
        )
        tuples.append(country_tuple + (name,))
        flag += 1
        if flag % 3 == 0:
            # create a DataFrame from the list of tuples
            new_df = pd.DataFrame(tuples, columns=["name", "pageviews", "country"])
            tuples = []
            df = pd.concat([df, new_df])
            # save the DataFrame to a TSV file with tab delimiter
            df.to_csv(filename, sep="\t", index=False)
