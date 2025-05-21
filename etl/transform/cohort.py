import pandas as pd

def cohort_analysis(fact_online_sales: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:

    dim_date.rename(columns={"Datekey": "DateKey"}, inplace=True)
    df = fact_online_sales.merge(dim_date, on = "DateKey")

    df["YearMonth"] = pd.to_datetime(df["FullDateLabel"]).dt.to_period("M")
    first_orders = df.groupby('CustomerKey').agg(
        
        FirstOrder=('YearMonth', 'min')
    ).reset_index()
    df = df.merge(first_orders, on='CustomerKey', how='left')

    df["CohortIndex"] = ((df["YearMonth"].dt.year - df["FirstOrder"].dt.year) * 12 +
    (df["YearMonth"].dt.month - df["FirstOrder"].dt.month))
    
    result = df.groupby(["YearMonth", "CohortIndex"]).agg(

        NumCustomers = ("CustomerKey", "nunique")
    ).reset_index()

    cohort_table = result.pivot(index = "YearMonth", columns = "CohortIndex", values = "NumCustomers")

    return cohort_table
