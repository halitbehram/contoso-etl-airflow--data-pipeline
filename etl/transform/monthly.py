import pandas as pd


def monthly_sales_timeseries(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:

    df = fact_online_sales.merge(dim_date, on = "DateKey")
    df["DateYearMonth"] = pd.to_datetime(df["FullDateLabel"]).dt.to_period("M")
    MonthlyRevenue = df.groupby("DateYearMonth").agg(

        Revenue = ("SalesAmount", "sum")
    ).reset_index()

    MonthlyRevenue["MonthName"] = MonthlyRevenue["DateYearMonth"].dt.month_name()
    seasonality = MonthlyRevenue.groupby("MonthName").agg(

        avg_sales=("Revenue", "mean")
    ).reset_index()

    month_order = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
        ]

    seasonality["MonthName"] = pd.Categorical(seasonality["MonthName"], categories = month_order, ordered = True)
    seasonality = seasonality.sort_values("MonthName")

    return seasonality