from typing import Optional
import pandas as pd


def churn_analysis(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame, reference_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:

    dim_date.rename(columns={"Datekey": "DateKey"}, inplace=True)
    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["OrderDate"] = pd.to_datetime(df["FullDateLabel"])
    last_purchase = df.groupby("CustomerKey").agg(

        LastPurchase = ("OrderDate", "max")
    ).reset_index()
    
    if reference_date == None :
        reference_date = df["OrderDate"].max()

    last_purchase["DiffDate"] = (reference_date - last_purchase["LastPurchase"]).dt.days

    def date_type(date):
        if date < 31 :
            return "Active"
        elif  31 <= date < 91:
            return "At Risk"
        elif date >= 91:
            return "Churned"
    
    last_purchase["RiskGroup"] = last_purchase["DiffDate"].apply(date_type)
    result = last_purchase.groupby("RiskGroup").agg(

        NumCustomers = ("CustomerKey", "nunique")
    ).reset_index()

    return result