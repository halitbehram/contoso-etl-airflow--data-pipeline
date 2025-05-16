import pandas as pd
from typing import Optional


def rfm_analysis(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame, reference_day: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    
    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["OrderDate"] = pd.to_datetime(df["FullDateLabel"])
    
    if reference_day == None:
        reference_day = df["OrderDate"].max()

    rfm = df.groupby("CustomerKey").agg(

        Recency = ("OrderDate", lambda x: (reference_day - x.max()).days),
        Frequency = ("SalesOrderNumber", "nunique"),
        Monetary = ("SalesAmount", "sum")
    ).reset_index()

    rfm["R-Score"] = pd.qcut(rfm["Recency"], 5, labels = [5,4,3,2,1]).astype(int)
    rfm["F-Score"] = pd.qcut(rfm["Frequency"], 5, labels = ["a", "b", "c", "d", "e"]).astype(str)
    rfm["M-Score"] = pd.qcut(rfm["Monetary"], 5, labels = [1,2,3,4,5]).astype(int)
    rfm["RFM-Score"] = rfm["R-Score"].astype(str) + rfm["F-Score"] + rfm["M-Score"].astype(str)

    def  rfm_segment(df):
        if df["R-Score"] >= 4 and df["F-Score"] in ["d", "e"] and df["M-Score"] >= 4:
            return "VIP"
        elif df["R-Score"] >= 3 and df["F-Score"] in ["c", "d"]:
            return "Potential Loyalist"
        elif df["R-Score"] <= 2 and df["F-Score"] <= 2:
            return "At Risk"
        else:
            return "Others"
    rfm["Segment"] = rfm.apply(rfm_segment, axis = 1)
    rfm = rfm[["CustomerKey", "Recency", "Frequency", "Monetary", "RFM-Score", "Segment"]]

    return rfm