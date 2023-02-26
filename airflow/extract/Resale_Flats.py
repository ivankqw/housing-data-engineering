import requests
import pandas as pd 

def get_urls(resource_ids, base_url, route):
    return [
        base_url + route + "?resource_id=" + resource_id + "&limit=1000"
        for resource_id in resource_ids.values()
    ]


def get_resale_flats(url, base_url) -> list:
    result = []
    while url:
        # print(url)
        try:
            curr_json = requests.get(url).json()
            if not result:
                # first time
                result = curr_json["result"]["records"]
            elif curr_json["result"]["records"]:
                result.extend(curr_json["result"]["records"])
            else:  # no more records
                return result
            # check if there is a next page or prev and next not the same
            if ("next" in curr_json["result"]["_links"]) or (
                "prev" in curr_json["result"]["_links"]
                and curr_json["result"]["_links"]["next"]
                != curr_json["result"]["_links"]["prev"]
            ):
                url = base_url + curr_json["result"]["_links"]["next"]
            else:
                url = None
        except Exception as e:
            print(e)
            return result  # return what we have so far
    return result

def get_all_resale_flats(start_urls, base_url) -> list:
    result = []
    for url in start_urls:
        result.extend(get_resale_flats(url, base_url))
    return result


def run(resource_ids, base_url, route, output_file):
    result = get_all_resale_flats(get_urls(resource_ids, base_url, route), base_url)
    pd.DataFrame(result).to_csv(output_file, index=False)


if __name__ == "__main__":
    print("hello world!")