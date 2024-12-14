import streamlit as st
import pandas as pd
import pg8000
def get_db_connection():
    conn = pg8000.connect(
        host="database-2.cl0c4miswffz.ap-south-1.rds.amazonaws.com",
        port=5432,
        database="postgres",
        user="postgres",
        password="Subash4315"
    )
    return conn
def execute_query(query):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
    finally:
        conn.close()

# app setup
st.title("Project Retail Data Analysis with PostgreSQL")
st.subheader("Explore business insights through PostgreSQL queries")

#query group selection sIDEBAR
query_group = st.sidebar.selectbox(
    "Category Selection:",
    ["Guvi Queries", "Own Queries", "Business Insights"]
)
query_options = {"Guvi Queries":{
    "1. Top 10 Highest Revenue Generating Products": """
        SELECT A.sub_category, SUM(B.sale_price * B.quantity) AS revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.sub_category ORDER BY revenue DESC LIMIT 10;""",
    "2. Top 5 Cities with the Highest Profit Margins": """
        SELECT A.city,CASE WHEN SUM(B.sale_price) = 0 THEN 0 ELSE SUM(B.profit) / SUM(B.sale_price) * 100 
        END AS profit_margin FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.city ORDER BY profit_margin DESC LIMIT 5;""",
    "3. Total Discount Given for Each Category": """
        SELECT A.category, SUM(B.discount) AS total_discount FROM retail1 AS A 
        JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.category ORDER BY total_discount DESC;""",
    "4. Average Sale Price Per Product Category": """
        SELECT A.sub_category, A.category, AVG(B.sale_price) AS average_sale_price 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.sub_category, A.category ORDER BY average_sale_price DESC;""",
    "5. Region with the Highest Average Sale Price": """
        SELECT A.region, AVG(B.sale_price) AS highest_average 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.region ORDER BY highest_average DESC LIMIT 1;""",
    "6. Total Profit Per Category": """
        SELECT A.category, SUM(B.profit) AS total_profit 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.category ORDER BY total_profit DESC;""",
    "7. Top 3 Segments with the Highest Quantity of Orders": """
        SELECT segment, COUNT(order_id) AS quantity_of_orders 
        FROM retail1 GROUP BY segment ORDER BY quantity_of_orders DESC LIMIT 3;""",
    "8. Average Discount Percentage Per Region": """
        SELECT A.region, AVG(B.discount_percent) AS average_discount 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.region ORDER BY average_discount;""",
    "9. Product Category with the Highest Total Profit": """
        SELECT A.category, SUM(B.profit) AS total_profit 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.category ORDER BY total_profit DESC;""",
    "10. Total Revenue Generated Per Year": """
        SELECT EXTRACT(YEAR FROM A.order_date) AS year, SUM(B.sale_price * B.quantity) AS total_revenue_per_year FROM retail1 AS A 
        JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY year ORDER BY year;"""},
"Own Queries": {
    "1. Sub-categories generating the most revenue": """
        SELECT A.sub_category, SUM(B.sale_price * B.quantity) AS revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.sub_category ORDER BY revenue DESC;""",
    "2. Average order size (quantity) for each region": """
        SELECT A.region, AVG(B.quantity) AS avg_order_size 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.region ORDER BY avg_order_size DESC;""",
    "3. Revenue by different shipping modes": """
        SELECT A.ship_mode, SUM(B.sale_price * B.quantity) AS total_revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.ship_mode ORDER BY total_revenue DESC;""",
    "4. Products where no profit was made": """
        SELECT DISTINCT A.sub_category FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id WHERE B.profit = 0;""",
    "5. Total revenue for each category in each region": """
        SELECT A.region, A.category, SUM(B.sale_price * B.quantity) AS total_revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.region, A.category ORDER BY total_revenue DESC;""",
    "6. Products with discounts > 2% and total units sold": """
        SELECT A.sub_category, SUM(B.quantity) AS total_units_sold 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id 
        WHERE B.discount_percent > 2 GROUP BY A.sub_category ORDER BY total_units_sold DESC;""",
    "7. Revenue contribution by customer segments": """
        SELECT A.segment, SUM(B.sale_price * B.quantity) AS revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.segment ORDER BY revenue DESC;""",
    "8. States generating the highest revenue in each category": """
        SELECT A.state, A.category, SUM(B.sale_price * B.quantity) AS total_revenue 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id 
        GROUP BY A.state, A.category ORDER BY A.category, total_revenue DESC;""",
    "9. Top 5 states offering the highest average discount": """
        SELECT A.state, AVG(B.discount_percent) AS avg_discount 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.state ORDER BY avg_discount DESC LIMIT 5;""",
    "10. Average discount offered in each product category": """
        SELECT A.category, AVG(B.discount_percent) AS avg_discount 
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.category 
        ORDER BY avg_discount DESC;"""},
"Business Insights" : {
    "1. Monthly Sales Analysis": """
        SELECT DATE_TRUNC('month', order_date) AS sales_month,
        EXTRACT(YEAR FROM order_date) AS sales_year,SUM(sale_price * quantity) AS total_sales
        FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id
        GROUP BY DATE_TRUNC('month', order_date), EXTRACT(YEAR FROM order_date)
        ORDER BY sales_month;""",
    "2. Customer Segment Analysis": """
        SELECT segment,SUM(B.sale_price * B.quantity) AS total_revenue,
         COUNT(A.order_id) AS total_orders FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY segment ORDER BY total_revenue DESC;""",
    "3.Regional Sales Analysis (Query sales data by region)": """ 
        SELECT A.region,SUM(B.sale_price * B.quantity) AS total_sales, 
        COUNT(DISTINCT A.order_id) AS number_of_orders FROM retail1 AS A JOIN retail2 AS B ON A.order_id = B.order_id GROUP BY A.region ORDER BY total_sales DESC;""",}}
#select your queries
filter_queries=query_options.get(query_group,{})    
if not filter_queries:
    st.error(f"No queries found:{query_group}")
query_choice = st.selectbox("Select Query", list(filter_queries.keys()))
#Filter button
if st.button("Tap here!"):
    query = filter_queries.get(query_choice, "")
    if query:
        try:
            data = execute_query(query)
            st.success("Result!")
            st.subheader("Answer")
            st.dataframe(data)
            if query_choice == "1. Top 10 Highest Revenue Generating Products":
                st.bar_chart(data.set_index("sub_category")["revenue"])
            elif query_choice == "2. Top 5 Cities with the Highest Profit Margins":
                st.bar_chart(data.set_index("city")["profit_margin"])
            elif query_choice == "3. Total Discount Given for Each Category":
                st.bar_chart(data.set_index("category")["total_discount"])
            elif query_choice == "4. Average Sale Price Per Product Category":
                st.bar_chart(data.set_index("sub_category")["average_sale_price"])
            elif query_choice == "5. Region with the Highest Average Sale Price":
                st.write("Region with the highest average sale price:", data.iloc[0]["region"])
            elif query_choice == "6. Total Profit Per Category":
                st.bar_chart(data.set_index("category")["total_profit"])
            elif query_choice == "7. Top 3 Segments with the Highest Quantity of Orders":
                st.bar_chart(data.set_index("segment")["quantity_of_orders"])
            elif query_choice == "8. Average Discount Percentage Per Region":
                st.bar_chart(data.set_index("region")["average_discount"])
            elif query_choice == "9. Product Category with the Highest Total Profit":
                st.bar_chart(data.set_index("category")["total_profit"])
            elif query_choice == "10. Total Revenue Generated Per Year":
                st.line_chart(data.set_index("year")["total_revenue_per_year"])
        except Exception as e:
            st.error(f"Error executing query: {e}")
    else:
        st.error("No query selected.")
