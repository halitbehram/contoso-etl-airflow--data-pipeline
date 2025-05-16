import pandas as pd


def order_frequency_analysis(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:

    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["OrderDate"] = pd.to_datetime(df["FullDateLabel"])
    uniqe_orders = df.drop_duplicates(subset = ["CustomerKey", "SalesOrderNumber"])
    df_sorted = uniqe_orders.sort_values(["CustomerKey", "OrderDate"])
    df_sorted["DateDiff"] = df_sorted.groupby("CustomerKey")["OrderDate"].diff().dt.days
    df_avg = df_sorted.groupby("CustomerKey").agg(

        OrderAvg = ("DateDiff", "mean"),
        TotalOrders = ("SalesOrderNumber", "nunique")
    ).reset_index()
    df_avg["OrderAvg"] = df_avg["OrderAvg"].fillna(0)

    spending = df.groupby("CustomerKey").agg(

        TotalSpending=("SalesAmount", "sum")
    ).reset_index()

    result = df_avg.merge(spending, on = "CustomerKey")

    return result.sort_values("OrderAvg")