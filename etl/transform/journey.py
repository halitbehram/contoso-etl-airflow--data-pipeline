import pandas as pd


def customer_journey_lag(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame, dim_customer:pd.DataFrame) -> pd.DataFrame:

    dim_date.rename(columns={"Datekey": "DateKey"}, inplace=True)
    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["FullDateLabel"] = pd.to_datetime(df["FullDateLabel"])
    first_shopping = df.groupby("CustomerKey").agg(

        FirstShoppingDay = ("FullDateLabel", "min")
    ).reset_index()

    dim_customer["CustomerRegistrationDate"] = pd.to_datetime(dim_customer["CustomerRegistrationDate"])

    dataframes = dim_customer.merge(first_shopping, on = "CustomerKey", how="left")
    dataframes["LagDay"] = (dataframes  ["FirstShoppingDay"] - dataframes["CustomerRegistrationDate"]).dt.days

    return dataframes[["CustomerKey", "CustomerRegistrationDate", "FirstShoppingDay", "LagDay"]]

#hiç sipariş vermemiş müşteriler?
def never_purchased(dataframes):
    
    return dataframes[dataframes["FirstShoppingDay"].isna()]

#kayıt olduktan sonra ortalama kac gun sonra sipariş veriliyor

def avg_lag_days(dataframes):

    return dataframes["LagDay"].mean()

#Kaç müşteri hiç sipariş vermemiş?
def num_never_purchased(dataframes):

    return dataframes["FirstShoppingDay"].isna().sum()

#kaç müşteri sipariş vermiş ? (Sadece alışveriş yapanlar)

def num_purchased(dataframes):

    return dataframes["FirstShoppingDay"].count()  #count na olan satırları saymaz

#tüm müşteri sayısı

def all_num(dataframes):

    return len(dataframes)