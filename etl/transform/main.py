from typing import Optional
import pandas as pd
from .rfm import rfm_analysis
from .cohort import cohort_analysis
from .churn import churn_analysis
from .frequency import order_frequency_analysis
from .geography import geographical_sales_analysis
from .monthly import monthly_sales_timeseries
from.journey import (customer_journey_lag, never_purchased, num_never_purchased, num_purchased, all_num, avg_lag_days)

class CustomerAnalytics:
    def __init__(self, dfs: dict[str, pd.DataFrame]):
        
        self.dfs = dfs
        self.fact_online_sales = dfs["FactOnlineSales"]
        self.dim_date = dfs["DimDate"]
        self.dim_customer = dfs["DimCustomer"]
        self.dim_product = dfs["DimProduct"]
        self.journey_lag_data = None

    def rfm(self, reference_day: Optional[pd.Timestamp] = None):
        return rfm_analysis(self.fact_online_sales, self.dim_date, reference_day)
    
    def cohort(self):
        return cohort_analysis(self.fact_online_sales, self.dim_date)
    
    def churn(self, reference_day: Optional[pd.Timestamp] = None):
        return churn_analysis(self.fact_online_sales, self.dim_date, reference_day)
        
    def order_frequency(self):
        return order_frequency_analysis(self.fact_online_sales, self.dim_date)
    
    def geography_sales(self):
        return geographical_sales_analysis(self.fact_online_sales, self.dim_customer, self.dim_date,self.dim_product)    
    
    def monthly(self):
        return monthly_sales_timeseries(self.fact_online_sales, self.dim_date)
    
    def customer_journey(self):
        if self.journey_lag_data == None:
            self.journey_lag_data = customer_journey_lag(self.fact_online_sales, self.dim_date, self.dim_customer)
        return self.journey_lag_data
    
    def never_purchased_customers(self):
        return never_purchased(self.customer_journey())
    
    def avg_lag_days_customers(self):
        return avg_lag_days(self.customer_journey())
    
    def num_never_purchased_customers(self):
        return num_never_purchased(self.customer_journey())
    
    def num_purchased_customers(self):
        return num_purchased(self.customer_journey())
    
    def all_customers_num(self):
        return all_num(self.customer_journey())
    

    


        