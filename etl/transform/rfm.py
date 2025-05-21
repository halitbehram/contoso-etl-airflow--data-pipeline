import pandas as pd
from typing import Optional


def rfm_analysis(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame, reference_day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    
    dim_date.rename(columns={"Datekey": "DateKey"}, inplace=True)
    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["OrderDate"] = pd.to_datetime(df["FullDateLabel"])
    
    if reference_day == None:
        reference_day = df["OrderDate"].max()

    rfm = df.groupby("CustomerKey").agg(

        Recency = ("OrderDate", lambda x: (reference_day - x.max()).days),
        Frequency = ("SalesOrderNumber", "nunique"),
        Monetary = ("SalesAmount", "sum")
    ).reset_index()

    rfm["R-Score"] = pd.cut(rfm["Recency"], 5, labels = [5,4,3,2,1]).astype(int)
    rfm["F-Score"] = pd.cut(rfm["Frequency"], 5, labels = [1,2,3,4,5]).astype(int)
    rfm["M-Score"] = pd.cut(rfm["Monetary"], 5, labels = [1,2,3,4,5]).astype(int)
    rfm["RFM-Score"] = rfm["R-Score"].astype(str) + rfm["F-Score"].astype(str) + rfm["M-Score"].astype(str)

    def  rfm_segment(df):
        if df["R-Score"] >= 4 and df["F-Score"] >= 4 and df["M-Score"] >= 4:
            return "VIP"
        elif df["R-Score"] >= 3 and df["F-Score"] >= 3:
            return "Potential Loyalist"
        elif df["R-Score"] <= 2 and df["F-Score"] <= 2:
            return "At Risk"
        else:
            return "Others"
    rfm["Segment"] = rfm.apply(rfm_segment, axis = 1)
    rfm = rfm[["CustomerKey", "Recency", "Frequency", "Monetary", "RFM-Score", "Segment"]]

    return rfm