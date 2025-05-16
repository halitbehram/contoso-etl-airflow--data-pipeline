import pandas as pd


def geographical_sales_analysis(fact_online_sales: pd.DataFrame, dim_customer: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:

    df = fact_online_sales.merge(dim_customer, on = "CustomerKey").merge(dim_product, on = "ProductKey")
    top_city = df.groupby("city").agg(

        TotalRevenue = ("SalesAmount", "sum")
    ).reset_index()
    top_city_sort = top_city.sort_values("TotalRevenue", ascending = False).head(10)["City"].tolist()
    df_top_city = df[df["City"].isin(top_city_sort)]
    top_city_group = df_top_city.groupby(["City", "ProductName"]).agg(

        TotalQuantity=('SalesQuantity', 'sum'),
        TotalRevenue=('SalesAmount', 'sum')
    ).reset_index()
    top_city_group_sort = top_city_group.sort_values(["City", "TotalRevenue"], ascending = [True, False]).groupby("City").head(5).reset_index(drop=True)
    
    return top_city_group_sort