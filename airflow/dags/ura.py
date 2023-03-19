# %%
import json
import http.client
import pandas as pd
# from dotenv import load_dotenv
import os
# load_dotenv()  # take environment variables from .env.

# %%
def get_token(access_key): 
    conn = http.client.HTTPSConnection("www.ura.gov.sg")
    payload = ''
    headers = {
    'AccessKey': access_key
    }
    conn.request("GET", "/uraDataService/insertNewToken.action", payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))['Result']

# %%
def get_result(token, access_key, route):
    conn = http.client.HTTPSConnection("www.ura.gov.sg")
    payload = ""
    headers = {
        "AccessKey": access_key,
        "Token": token,
        "User-Agent": "PostmanRuntime/7.26.8",
    }
    conn.request("GET", route, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))['Result']


def get_all_results(token, access_key, routes):
    result = []
    for route in routes:
        result.extend(get_result(token, access_key, route))
    return pd.DataFrame(result)


# %%

def get_all_ura():
    private_transactions_routes = [
        "/uraDataService/invokeUraDS?service=PMI_Resi_Transaction&batch=" + str(i)
        for i in range(1, 5)
    ]

    private_rental_routes = [
        f"/uraDataService/invokeUraDS?service=PMI_Resi_Rental&refPeriod={q}"
        # start from 14q1 to 23q1
        for q in [f"{y}q{i}" for y in range(14, 24) for i in range(1, 5)]
    ]

    planning_decisions_routes = [
        "/uraDataService/invokeUraDS?service=Planning_Decision&year=" + str(i)
        for i in range(2000, 2024)
    ]

    access_key = "480de617-6aee-4c71-a04a-3b6c3a9596b2"
    token = get_token(access_key)

    # get_all_results(token, access_key, private_transactions_routes).to_csv(
    #     "../data/private_transactions.csv", index=False
    # )
    # get_all_results(token, access_key, private_rental_routes).to_csv(
    #     "../data/private_rental.csv", index=False
    # )
    # get_all_results(token, access_key, planning_decisions_routes).to_csv(
    #     "../data/planning_decisions.csv", index=False
    # )
    print("Getting private transactions...")
    df_private_transactions = get_all_results(token, access_key, private_transactions_routes)
    print("Getting private rental...")
    df_private_rental = get_all_results(token, access_key, private_rental_routes)
    print("Getting planning decisions...")
    df_planning_decisions = get_all_results(token, access_key, planning_decisions_routes)

    return df_private_transactions, df_private_rental, df_planning_decisions




