import pandas as pd
import numpy as np
import time
import pickle

data_path = "/opt/airflow/dags/data/"

"""helper functions"""


def get_cpi_from_db():
    pass


def get_cpi_df(cpf_df_path):
    # cpi
    cpi_df = pd.read_csv(cpf_df_path)
    # convert cpi month to datetime
    cpi_df["Month"] = pd.to_datetime(cpi_df["Month"], format="%Y-%m")
    # rename value to cpi
    cpi_df = cpi_df.rename(columns={"Value": "cpi"})
    return cpi_df


def get_ts_per_district(grouped):
    # get a ts for each district
    districts = grouped["district"].unique()
    all_district_var_ts = {}
    for district in districts:
        all_district_var_ts[district] = grouped[
            grouped["district"] == district
        ].set_index("month_year")

        # create missing months (months with no transactions will be filled with NaN)
        # for each year except current year, check if all months are present
        years = all_district_var_ts[district].index.year.unique()
        years = years[years != time.localtime().tm_year]
        # if not, create missing months and fill with NaN
        for year in years:
            months = all_district_var_ts[district].loc[str(year)].index.month.unique()
            if len(months) < 12:
                missing_months = set(range(1, 13)) - set(months)
                for month in missing_months:
                    all_district_var_ts[district].loc[
                        pd.to_datetime(f"{year}-{month}-01")
                    ] = np.nan
        # missing months for current year up to current month
        curr_year_months = (
            all_district_var_ts[district]
            .loc[str(time.localtime().tm_year)]
            .index.month.unique()
        )
        if len(curr_year_months) < time.localtime().tm_mon:
            missing_months = set(range(1, time.localtime().tm_mon)) - set(
                curr_year_months
            )
            for month in missing_months:
                all_district_var_ts[district].loc[
                    pd.to_datetime(f"{time.localtime().tm_year}-{month}-01")
                ] = np.nan
        # sort by month_year
        all_district_var_ts[district] = all_district_var_ts[district].sort_index()

    # remove district_ column
    for district_no, district_df in all_district_var_ts.items():
        all_district_var_ts[district_no] = all_district_var_ts[district_no].drop(
            "district", axis=1
        )

    # remove nans
    for district_no, district_df in all_district_var_ts.items():
        all_district_var_ts[district_no] = all_district_var_ts[district_no].dropna()

    return all_district_var_ts


"""transform functions"""


def transform_resale_transactions_ml(
    cpi_df_path, resale_flat_transactions_path, hdb_info_path
):
    cpi_df = get_cpi_df(cpi_df_path)
    # resale_flat_transactions = pd.read_csv("data/resale_flats_transformed.csv")
    resale_flat_transactions = pd.read_csv(resale_flat_transactions_path)
    # hdb_info = pd.read_csv("data/hdb_information.csv")
    hdb_info = pd.read_csv(hdb_info_path)
    # look_back = 5
    # resale_flat_transactions = resale_flat_transactions[
    #     resale_flat_transactions["year"] >= 2023 - look_back
    # ]
    # create primary key for resale flat transactions and hdb information
    resale_flat_transactions["full_address"] = (
        resale_flat_transactions["block"]
        + " "
        + resale_flat_transactions["street_name"]
    )
    hdb_info["full_address"] = hdb_info["blk_no"] + " " + hdb_info["street"]

    # join the resale flat transactions with the hdb information
    resale_flat_transactions_new = resale_flat_transactions.merge(
        hdb_info, on="full_address", how="left"
    )
    # drop NIL
    resale_flat_transactions_new = resale_flat_transactions_new[
        resale_flat_transactions["district"] != "NIL"
    ]
    # convert district to int
    resale_flat_transactions_new["district"] = resale_flat_transactions_new[
        "district"
    ].astype(int)
    # remove unnecessary columns
    # like duplicated columns after merge
    # like aggregated columns that leak information about the future
    to_remove = [
        "street_name",  # duplicated
        "x",  # duplicated
        "y",  # duplicated
        "street_name_with_block",  # duplicated
        "blk_no",  # duplicated
        "bldg_contract_town",  # duplicated (but not exactly) - town
        "_id_y",  # not needed
        "_id_x",  # not needed
        "_id_y",  # not needed
        "exec_sold",  # leaky
        "1room_rental",  # leaky
        "1room_sold",  # leaky
        "2room_rental",  # leaky
        "2room_sold",  # leaky
        "3room_rental",  # leaky
        "3room_sold",  # leaky
        "4room_sold",  # leaky
        "5room_sold",  # leaky
        "multigen_sold",  # leaky
        "studio_apartment_sold",  # leaky
        "other_room_rental",  # leaky
    ]
    resale_flat_transactions_new = resale_flat_transactions_new.drop(to_remove, axis=1)
    resale_flat_transactions_new = resale_flat_transactions_new.drop(
        "residential", axis=1
    )
    # remove year_completed
    resale_flat_transactions_new = resale_flat_transactions_new.drop(
        "year_completed", axis=1
    )
    # remove lease_commence_date
    resale_flat_transactions_new = resale_flat_transactions_new.drop(
        "lease_commence_date", axis=1
    )

    def get_remaining_lease(row):
        a = row["remaining_lease"]
        year = a.split(" ")[0]
        if len(a.split(" ")) < 3:
            return int(year)
        month = a.split(" ")[2]
        return int(year) + int(month) / 12

    # aggregate resale flat transactions by district and time (month and year)
    resale_flat_transactions_new["month_year"] = (
        resale_flat_transactions_new["month"].astype(str)
        + "-"
        + resale_flat_transactions_new["year"].astype(str)
    )
    resale_flat_transactions_new["month_year"] = pd.to_datetime(
        resale_flat_transactions_new["month_year"], format="%m-%Y"
    )  # assume first day of month for illustration purposes
    resale_flat_transactions_new = resale_flat_transactions_new.drop(
        ["month", "year"], axis=1
    )

    # convert cpi month to datetime
    cpi_df["Month"] = pd.to_datetime(cpi_df["Month"], format="%Y-%m")
    # rename value to cpi
    cpi_df = cpi_df.rename(columns={"Value": "cpi"})

    # merge with cpi
    resale_flat_transactions_new = pd.merge(
        resale_flat_transactions_new,
        cpi_df,
        how="left",
        left_on="month_year",
        right_on="Month",
    )

    # if cpi is null, fill with previous value
    # sort resale_flat_transactions_clean by month_year
    resale_flat_transactions_new = resale_flat_transactions_new.sort_values(
        by="month_year"
    )
    resale_flat_transactions_new["cpi"] = resale_flat_transactions_new["cpi"].fillna(
        method="ffill"
    )

    # drop Month column
    resale_flat_transactions_new = resale_flat_transactions_new.drop(["Month"], axis=1)
    # convert to binary variable
    resale_flat_transactions_new["precinct_pavilion"] = resale_flat_transactions_new[
        "precinct_pavilion"
    ].apply(lambda x: 1 if x == "Y" else 0)
    resale_flat_transactions_new["commercial"] = resale_flat_transactions_new[
        "commercial"
    ].apply(lambda x: 1 if x == "Y" else 0)
    resale_flat_transactions_new["market_hawker"] = resale_flat_transactions_new[
        "market_hawker"
    ].apply(lambda x: 1 if x == "Y" else 0)
    resale_flat_transactions_new["miscellaneous"] = resale_flat_transactions_new[
        "miscellaneous"
    ].apply(lambda x: 1 if x == "Y" else 0)

    # convert remaining lease to years
    resale_flat_transactions_new[
        "remaining_lease_years"
    ] = resale_flat_transactions_new.apply(get_remaining_lease, axis=1)

    # rename to price
    resale_flat_transactions_new = resale_flat_transactions_new.rename(
        columns={"resale_price": "price"}
    )

    # lag all except price by 1
    lag_cols = resale_flat_transactions_new.columns.drop(["price", "month_year"])
    resale_flat_transactions_new[lag_cols] = resale_flat_transactions_new[
        lag_cols
    ].shift(1)

    # group
    resale_flat_transactions_df_grouped = (
        resale_flat_transactions_new.groupby(["district", "month_year"])
        .agg(
            {
                "price": "median",
                "floor_area_sqm": "median",
                "remaining_lease_years": "median",
                "max_floor_lvl": "median",
                "precinct_pavilion": "sum",
                "commercial": "sum",
                "market_hawker": "sum",
                "miscellaneous": "sum",
                "cpi": "first",  # cpi is the same for all transactions in a month
            }
        )
        .fillna(0)
        .reset_index()
    )  # fillna(0) to fill NaN values with 0

    resale_flat_transactions_df_grouped_dict = get_ts_per_district(
        resale_flat_transactions_df_grouped
    )

    return resale_flat_transactions_df_grouped_dict
    # with open(data_path + "resale_flat_transactions_df_grouped_dict.pkl", "wb") as f:
    #     pickle.dump(resale_flat_transactions_df_grouped_dict, f)


def transform_flat_rental_ml(cpi_df_path, flat_rental_path):
    cpi_df = get_cpi_df(cpi_df_path)
    # since based on the results from modelling, linear interpolation does not work well, we will not use it here
    # in some cases SARIMAX does outperform SARIMA and baseline, so we need to do feature engineering to add features in as exogenous variables
    # transformations already applied to flat_transactions_df so skip that
    # flat_rental_df
    # flat_rental_df = pd.read_csv("data/flat_rental_transformed.csv")
    flat_rental_df = pd.read_csv(flat_rental_path)
    # take year and month column and convert to datetime
    flat_rental_df["month"] = flat_rental_df["month"].astype(str)
    flat_rental_df["year"] = flat_rental_df["year"].astype(str)
    flat_rental_df["month_year"] = (
        flat_rental_df["year"] + "-" + flat_rental_df["month"]
    )
    # convert month_year to datetime
    flat_rental_df["month_year"] = pd.to_datetime(
        flat_rental_df["month_year"], format="%Y-%m"
    )
    # drop year and month columns
    flat_rental_df = flat_rental_df.drop(columns=["year", "month"])
    # merge with cpi
    flat_rental_df = flat_rental_df.merge(
        cpi_df, how="left", left_on="month_year", right_on="Month"
    )
    # sort by month_year
    flat_rental_df = flat_rental_df.sort_values(by="month_year")
    # ffill cpi for missing values
    flat_rental_df["cpi"] = flat_rental_df["cpi"].fillna(method="ffill")
    # drop Month column
    flat_rental_df = flat_rental_df.drop(columns=["Month"])
    # drop other irrelavant columns
    flat_rental_df = flat_rental_df.drop(
        columns=[
            "town",
            "street_name",
            "_id",
            "block",
            "street_name_with_block",
            "x",
            "y",
            "lat",
            "lon",
            "postal",
        ]
    )
    # convert flat_type to dummy variables
    flat_rental_df = pd.get_dummies(
        flat_rental_df, columns=["flat_type"], drop_first=True
    )  # prevent multicollinearity

    print(flat_rental_df.columns)

    # rename to price
    flat_rental_df = flat_rental_df.rename(columns={"monthly_rent": "price"})

    # lag all except price by 1
    lag_cols = flat_rental_df.columns.drop(["price", "month_year"])
    flat_rental_df[lag_cols] = flat_rental_df[lag_cols].shift(1)

    # group
    flat_rental_df_grouped = (
        flat_rental_df.groupby(["district", "month_year"])
        .agg(
            {
                "cpi": "first",
                "flat_type_2-ROOM": "sum",
                "flat_type_3-ROOM": "sum",
                "flat_type_4-ROOM": "sum",
                "flat_type_5-ROOM": "sum",
                "flat_type_EXECUTIVE": "sum",
                "price": "median",
            }
        )
        .fillna(0)
        .reset_index()
    )

    flat_rental_df_grouped_dict = get_ts_per_district(flat_rental_df_grouped)

    return flat_rental_df_grouped_dict
    # with open(data_path + "flat_rental_df_grouped_dict.pkl", "wb") as f:
    #     pickle.dump(flat_rental_df_grouped_dict, f)


def transform_private_transactions_ml(cpi_df_path, private_transactions_path):
    cpi_df = get_cpi_df(cpi_df_path)
    # private_transactions_df = pd.read_csv("data/private_transactions_transformed.csv")
    private_transactions_df = pd.read_csv(private_transactions_path)
    private_transactions_df = private_transactions_df.drop(
        columns=["floorRange", "nettPrice", "street", "project"]
    )
    # divide price by noOfUnits to get price per unit
    private_transactions_df["price_per_unit"] = (
        private_transactions_df["price"] / private_transactions_df["noOfUnits"]
    )
    # drop price and noOfUnits columns
    private_transactions_df = private_transactions_df.drop(
        columns=["price", "noOfUnits"]
    )
    # transform date
    private_transactions_df["month"] = private_transactions_df["month"].astype(str)
    private_transactions_df["year"] = private_transactions_df["year"].astype(str)
    private_transactions_df["month_year"] = (
        private_transactions_df["year"] + "-" + private_transactions_df["month"]
    )
    private_transactions_df["month_year"] = pd.to_datetime(
        private_transactions_df["month_year"], format="%Y-%m"
    )
    private_transactions_df = private_transactions_df.drop(columns=["year", "month"])
    private_transactions_df = private_transactions_df.merge(
        cpi_df, how="left", left_on="month_year", right_on="Month"
    )
    private_transactions_df = private_transactions_df.sort_values(by="month_year")
    private_transactions_df["cpi"] = private_transactions_df["cpi"].fillna(
        method="ffill"
    )
    private_transactions_df = private_transactions_df.drop(columns=["Month"])

    # convert typeOfArea to dummy variables
    private_transactions_df = pd.get_dummies(
        private_transactions_df, columns=["typeOfArea"], drop_first=True
    )  # prevent multicollinearity
    # convert propertyType to dummy variables
    private_transactions_df = pd.get_dummies(
        private_transactions_df, columns=["propertyType"], drop_first=True
    )  # prevent multicollinearity
    # convert typeOfSale to dummy variables
    private_transactions_df = pd.get_dummies(
        private_transactions_df, columns=["typeOfSale"], drop_first=True
    )  # prevent multicollinearity
    # convert marketSegment to dummy variables
    private_transactions_df = pd.get_dummies(
        private_transactions_df, columns=["marketSegment"], drop_first=True
    )  # prevent multicollinearity
    # transform tenure into a binary variable whether freehold or not
    private_transactions_df["is_freehold"] = private_transactions_df["tenure"].apply(
        lambda x: 1 if x == "Freehold" else 0
    )
    private_transactions_df.drop(columns=["tenure"], inplace=True)

    # rename to price
    private_transactions_df.rename(columns={"price_per_unit": "price"}, inplace=True)

    # lag all except price by 1
    lag_cols = private_transactions_df.columns.drop(["price", "month_year"])
    private_transactions_df[lag_cols] = private_transactions_df[lag_cols].shift(1)

    # group by district and month_year
    print(private_transactions_df.columns)
    private_transactions_df_grouped = (
        private_transactions_df.groupby(["district", "month_year"])
        .agg(
            {
                "price": "std",
                "cpi": "first",
                "area": "median",
                # "noOfUnits": "sum",
                "is_freehold": "sum",
                "typeOfArea_Strata": "sum",
                "propertyType_Condominium": "sum",
                "propertyType_Detached": "sum",
                "propertyType_Executive Condominium": "sum",
                "propertyType_Semi-detached": "sum",
                "propertyType_Strata Semi-detached": "sum",
                "propertyType_Strata Detached": "sum",
                "propertyType_Strata Terrace": "sum",
                "propertyType_Terrace": "sum",
                "typeOfSale_2": "sum",
                "typeOfSale_3": "sum",
                "marketSegment_OCR": "sum",
                "marketSegment_RCR": "sum",
                "is_freehold": "sum",
            }
        )
        .fillna(0)
        .reset_index()
    )

    private_transactions_df_grouped_dict = get_ts_per_district(
        private_transactions_df_grouped
    )

    # return private_transactions_df_grouped_dict
    with open(data_path + "private_transactions_df_grouped_dict.pkl", "wb") as f:
        pickle.dump(private_transactions_df_grouped_dict, f)


def transform_private_rental_ml(cpi_df_path, private_rental_df_path):
    # private_rental_df = pd.read_csv("data/private_rental_transformed.csv")
    cpi_df = pd.read_csv(cpi_df_path)
    private_rental_df = pd.read_csv(private_rental_df_path)
    private_rental_df = private_rental_df.drop(columns=["street", "project"])
    # convert areaSqft to dummy variables
    private_rental_df = pd.get_dummies(
        private_rental_df, columns=["areaSqft"], drop_first=True
    )  # prevent multicollinearity
    private_rental_df = pd.get_dummies(
        private_rental_df, columns=["areaSqm"], drop_first=True
    )  # prevent multicollinearity
    private_rental_df = pd.get_dummies(
        private_rental_df, columns=["propertyType"], drop_first=True
    )  # prevent multicollinearity
    private_rental_df["month"] = private_rental_df["month"].astype(str)
    private_rental_df["year"] = private_rental_df["year"].astype(str)
    private_rental_df["month_year"] = (
        private_rental_df["year"] + "-" + private_rental_df["month"]
    )
    private_rental_df["month_year"] = pd.to_datetime(
        private_rental_df["month_year"], format="%Y-%m"
    )
    private_rental_df = private_rental_df.drop(columns=["year", "month", "leaseDate"])
    private_rental_df = private_rental_df.merge(
        cpi_df, how="left", left_on="month_year", right_on="Month"
    )
    private_rental_df = private_rental_df.sort_values(by="month_year")
    private_rental_df["cpi"] = private_rental_df["cpi"].fillna(method="ffill")
    private_rental_df = private_rental_df.drop(columns=["Month"])

    # rename to price
    private_rental_df.rename(columns={"rent": "price"}, inplace=True)

    # lag all except price by 1
    lag_cols = private_rental_df.columns.drop(["price", "month_year"])
    private_rental_df[lag_cols] = private_rental_df[lag_cols].shift(1)

    # create dict for columns to group by
    agg_dict = {}
    for col in private_rental_df.columns:
        if "areaSqft" in col:
            agg_dict[col] = "sum"
        elif "areaSqm" in col:
            agg_dict[col] = "sum"
        elif "propertyType" in col:
            agg_dict[col] = "sum"
    agg_dict["cpi"] = "first"
    agg_dict["price"] = "median"
    # group by district and month_year
    private_rental_df_grouped = (
        private_rental_df.groupby(["district", "month_year"])
        .agg(agg_dict)
        .fillna(0)
        .reset_index()
    )

    private_rental_df_grouped_dict = get_ts_per_district(private_rental_df_grouped)
