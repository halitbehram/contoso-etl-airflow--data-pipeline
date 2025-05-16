import pandas as pd

def repeat_customers(fact_online_sales: pd.DataFrame) -> pd.DataFrame:

    df = fact_online_sales.groupby("CustomerKey").agg(

        NumOrders = ("SalesOrderNumber", "nunique")
    ).reset_index()
    fact_online_sales = fact_online_sales.merge(df, on = "CustomerKey")

    def customer_type(NumOrders):
        if NumOrders == 1:
            return "First"
        elif NumOrders > 1:
            return "Repeat"
    
    fact_online_sales["CustomerType"] = fact_online_sales["NumOrders"].apply(customer_type)
    result = fact_online_sales.groupby("CustomerType").agg(

        NumCustomers = ("CustomerKey", "nunique"),
        TotalRevenue = ("SalesAmount", "sum")
    ).reset_index()

    total = result["TotalRevenue"].sum()
    result["PercentTotalRevenue"] = result["TotalRevenue"] / total * 100

    return result.sort_values("TotalRevenue", ascending = False)