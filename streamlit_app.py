# Import python packages
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# App Title
st.title("Revenue Dashboard")

# Get the current credentials
session = get_active_session()

def format_revenue(revenue):
    #return f"₹{revenue / 1_000_000:.1f}M"
    return f"₹{revenue:.1f}"

# Function to alternate row colors
def highlight_rows(row):
    color = '#e8f4f8' if row.name % 2 == 0 else 'white'  # Alternate rows with light blue
    return ['background-color: {}'.format(color)] * len(row)

# Function to fetch yearly comparison data
def fetch_yearly_comparison_data():
    query = """
    SELECT 
        year,
        total_revenue,
        total_orders,
        avg_revenue_per_order,
        avg_revenue_per_item,
        max_order_value
    FROM foodapp_sandbox.consumption_schema.vw_yearly_revenue_kpis
    ORDER BY year;
    """
    return session.sql(query).collect()

# Function to fetch monthly data for all years (for year-over-year comparison)
def fetch_monthly_data_all_years():
    query = """
    SELECT 
        year,
        month,
        total_revenue,
        total_orders
    FROM foodapp_sandbox.consumption_schema.vw_monthly_revenue_kpis
    ORDER BY year, month;
    """
    return session.sql(query).collect()

# Function to fetch restaurant performance summary
def fetch_restaurant_performance_summary(year):
    query = f"""
    SELECT 
        restaurant_name,
        SUM(total_revenue) as annual_revenue,
        SUM(total_orders) as annual_orders,
        AVG(avg_revenue_per_order) as avg_revenue_per_order,
        COUNT(DISTINCT month) as active_months
    FROM foodapp_sandbox.consumption_schema.vw_monthly_revenue_by_restaurant
    WHERE year = {year}
    GROUP BY restaurant_name
    ORDER BY annual_revenue DESC
    LIMIT 15;
    """
    return session.sql(query).collect()

# Function to fetch quarterly data
def fetch_quarterly_data(year):
    query = f"""
    SELECT 
        CASE 
            WHEN month IN (1,2,3) THEN 'Q1'
            WHEN month IN (4,5,6) THEN 'Q2'
            WHEN month IN (7,8,9) THEN 'Q3'
            WHEN month IN (10,11,12) THEN 'Q4'
        END as quarter,
        SUM(total_revenue) as quarterly_revenue,
        SUM(total_orders) as quarterly_orders
    FROM foodapp_sandbox.consumption_schema.vw_monthly_revenue_kpis
    WHERE year = {year}
    GROUP BY quarter
    ORDER BY quarter;
    """
    return session.sql(query).collect()
def fetch_kpi_data():
    query = """
    SELECT 
        year,
        total_revenue,
        total_orders,
        avg_revenue_per_order,
        avg_revenue_per_item,
        max_order_value
    FROM foodapp_sandbox.consumption_schema.vw_yearly_revenue_kpis
    ORDER BY year;
    """
    return session.sql(query).collect()

#TO_CHAR(TO_DATE(month::text, 'MM'), 'Mon') AS month_abbr,  -- Converts month number to abbreviated month name
def fetch_monthly_kpi_data(year):
    query = f"""
    SELECT 
        month::number(2) as month,
        total_revenue::NUMBER(10) AS TOTAL_REVENUE
    FROM 
    foodapp_sandbox.consumption_schema.vw_monthly_revenue_kpis
    WHERE year = {year}
    ORDER BY month;
    """
    return session.sql(query).collect()


def fetch_unique_months(year):
    query = f"""
    SELECT 
        DISTINCT MONTH FROM 
    foodapp_sandbox.consumption_schema.vw_monthly_revenue_by_restaurant 
    WHERE YEAR = {year} 
    ORDER BY MONTH;
    """
    return session.sql(query).collect()
    
def fetch_top_restaurants(year, month):
    query = f"""
    SELECT
        restaurant_name,
        total_revenue,
        total_orders,
        avg_revenue_per_order,
        avg_revenue_per_item,
        max_order_value
    FROM
        foodapp_sandbox.consumption_schema.vw_monthly_revenue_by_restaurant
    WHERE
        YEAR = {year}
        AND MONTH = {month}
    ORDER BY
        total_revenue DESC
    LIMIT 10;
    """
    return session.sql(query).collect()
    
# Function to convert Snowpark DataFrame to Pandas DataFrame
def snowpark_to_pandas(snowpark_df):
    return pd.DataFrame(
        snowpark_df,
        columns=[
            'Restaurant Name',
            'Total Revenue (₹)',
            'Total Orders',
            'Avg Revenue per Order (₹)',
            'Avg Revenue per Item (₹)',
            'Max Order Value (₹)'
        ]
    )
# Fetch data
sf_df = fetch_kpi_data()
df = pd.DataFrame(
    sf_df,
    columns=['YEAR','TOTAL_REVENUE','TOTAL_ORDERS','AVG_REVENUE_PER_ORDER','AVG_REVENUE_PER_ITEM','MAX_ORDER_VALUE']
)

# Aggregate Metrics for All Years
#st.subheader("Aggregate KPIs: Overall Performance")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Revenue (All Years)", format_revenue(df['TOTAL_REVENUE'].sum()))
with col2:
    st.metric("Total Orders (All Years)", f"{df['TOTAL_ORDERS'].sum():,}")
with col3:
    st.metric("Max Order Value (Overall)", f"₹{df['MAX_ORDER_VALUE'].max():,.0f}")

st.divider()

# Year Selection Box
years = df["YEAR"].unique()
default_year = max(years)  # Select the most recent year by default
selected_year = st.selectbox("Select Year", sorted(years), index=list(years).index(default_year))

# Filter data for selected year
year_data = df[df["YEAR"] == selected_year]
total_revenue = year_data["TOTAL_REVENUE"].iloc[0]
total_orders = year_data["TOTAL_ORDERS"].iloc[0]
avg_revenue_per_order = year_data["AVG_REVENUE_PER_ORDER"].iloc[0]
avg_revenue_per_item = year_data["AVG_REVENUE_PER_ITEM"].iloc[0]
max_order_value = year_data["MAX_ORDER_VALUE"].iloc[0]

# Get previous year data
previous_year = selected_year - 1
previous_year_data = df[df["YEAR"] == previous_year]

# If previous year data exists, calculate differences
if not previous_year_data.empty:
    prev_total_revenue = previous_year_data["TOTAL_REVENUE"].iloc[0]
    prev_total_orders = previous_year_data["TOTAL_ORDERS"].iloc[0]
    prev_avg_revenue_per_order = previous_year_data["AVG_REVENUE_PER_ORDER"].iloc[0]
    prev_avg_revenue_per_item = previous_year_data["AVG_REVENUE_PER_ITEM"].iloc[0]
    prev_max_order_value = previous_year_data["MAX_ORDER_VALUE"].iloc[0]

    # Calculate differences
    revenue_diff = total_revenue - prev_total_revenue
    orders_diff = total_orders - prev_total_orders
    avg_rev_order_diff = avg_revenue_per_order - prev_avg_revenue_per_order
    avg_rev_item_diff = avg_revenue_per_item - prev_avg_revenue_per_item
    max_order_diff = max_order_value - prev_max_order_value
else:
    # If previous year data is not found, set differences to None or zero
    revenue_diff = orders_diff = avg_rev_order_diff = avg_rev_item_diff = max_order_diff = None


# Display Metrics for Selected Year with Comparison to Previous Year
# st.subheader(f"KPI Scorecard for {selected_year}")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        "Total Revenue", 
        format_revenue(total_revenue), 
        delta=f"₹{revenue_diff:.1f}" if revenue_diff is not None else None
    )
    st.metric("Total Orders", f"{total_orders:,}", delta=f"{orders_diff:,}" if orders_diff is not None else None)

    #st.metric("Total Revenue", f"₹{total_revenue:,.0f}", delta=f"₹{revenue_diff:,.0f}" if revenue_diff is not None else None)
    #st.metric("Total Orders", f"{total_orders:,}", delta=f"{orders_diff:,}" if orders_diff is not None else None)

with col2:
    st.metric("Avg Revenue per Order", f"₹{avg_revenue_per_order:,.0f}", delta=f"₹{avg_rev_order_diff:,.0f}" if avg_rev_order_diff is not None else None)
    st.metric("Avg Revenue per Item", f"₹{avg_revenue_per_item:,.0f}", delta=f"₹{avg_rev_item_diff:,.0f}" if avg_rev_item_diff is not None else None)

with col3:
    st.metric("Max Order Value", f"₹{max_order_value:,.0f}", delta=f"₹{max_order_diff:,.0f}" if max_order_diff is not None else None)



st.divider()
# -----------------------------------------


# Fetch and prepare data
month_sf_df = fetch_monthly_kpi_data(selected_year)
month_df = pd.DataFrame(
    month_sf_df,
    columns=['Month', 'Total Monthly Revenue']
)

# Map numeric months to abbreviated month names
month_mapping = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
    5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
    9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}
month_df['Month'] = month_df['Month'].map(month_mapping)

# Ensure months are in the correct chronological order
month_df['Month'] = pd.Categorical(
    month_df['Month'],
    categories=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
    ordered=True
)
month_df = month_df.sort_values('Month')  # Sort by chronological month order

# Convert revenue to millions
month_df['Total Monthly Revenue'] = month_df['Total Monthly Revenue'] 

# Plot Monthly Revenue Trend using Bar Chart
st.subheader(f"{selected_year} - Monthly Revenue Trend")
# Create the Altair Bar Chart with Custom Color
bar_chart = alt.Chart(month_df).mark_bar(color="#2E86AB").encode(
    x=alt.X('Month', sort='ascending', title='Month'),
    y=alt.Y('Total Monthly Revenue', title='Revenue (₹)')
).properties(
    width=700,
    height=400
)

# Display the chart in Streamlit
st.altair_chart(bar_chart, use_container_width=True)

# Add a Trending Chart using Altair
st.subheader(f"{selected_year} - Monthly Revenue Trend")

trend_chart = alt.Chart(month_df).mark_line(color="#2E86AB", point=alt.OverlayMarkDef(color="#A23B72")).encode(
    x=alt.X('Month', sort='ascending', title='Month'),
    y=alt.Y('Total Monthly Revenue', title='Revenue (₹)', scale=alt.Scale(domain=[0, month_df['Total Monthly Revenue'].max()])),
    tooltip=[
        alt.Tooltip('Month', title='Month'),
        alt.Tooltip('Total Monthly Revenue', title='Revenue (₹M)', format='.2f')  # Format to 2 decimal places
    ]
).properties(
    width=700,
    height=400
).configure_point(
    size=60
)

st.altair_chart(trend_chart, use_container_width=True)

st.divider()

# Year-over-Year Revenue Comparison
st.subheader("📊 Year-over-Year Performance Analysis")

# Fetch yearly comparison data
yearly_data = fetch_yearly_comparison_data()
yearly_df = pd.DataFrame(
    yearly_data,
    columns=['Year', 'Total Revenue', 'Total Orders', 'Avg Revenue per Order', 'Avg Revenue per Item', 'Max Order Value']
)

# Create year-over-year comparison charts
col1, col2 = st.columns(2)

with col1:
    # Revenue trend over years
    revenue_trend = alt.Chart(yearly_df).mark_line(color="#2E86AB", point=alt.OverlayMarkDef(color="#A23B72", size=80)).encode(
        x=alt.X('Year:O', title='Year'),
        y=alt.Y('Total Revenue:Q', title='Revenue (₹)'),
        tooltip=['Year', 'Total Revenue']
    ).properties(
        title="Revenue Trend Over Years",
        width=350,
        height=300
    )
    st.altair_chart(revenue_trend, use_container_width=True)

with col2:
    # Orders trend over years
    orders_trend = alt.Chart(yearly_df).mark_line(color="#2E86AB", point=alt.OverlayMarkDef(color="#A23B72", size=80)).encode(
        x=alt.X('Year:O', title='Year'),
        y=alt.Y('Total Orders:Q', title='Number of Orders'),
        tooltip=['Year', 'Total Orders']
    ).properties(
        title="Orders Trend Over Years",
        width=350,
        height=300
    )
    st.altair_chart(orders_trend, use_container_width=True)

# Average Revenue per Order and per Item comparison
col3, col4 = st.columns(2)

with col3:
    avg_revenue_chart = alt.Chart(yearly_df).mark_bar(color="#2E86AB").encode(
        x=alt.X('Year:O', title='Year'),
        y=alt.Y('Avg Revenue per Order:Q', title='Average Revenue per Order (₹)'),
        tooltip=['Year', 'Avg Revenue per Order']
    ).properties(
        title="Average Revenue per Order",
        width=350,
        height=300
    )
    st.altair_chart(avg_revenue_chart, use_container_width=True)

with col4:
    avg_item_chart = alt.Chart(yearly_df).mark_bar(color="#A23B72").encode(
        x=alt.X('Year:O', title='Year'),
        y=alt.Y('Avg Revenue per Item:Q', title='Average Revenue per Item (₹)'),
        tooltip=['Year', 'Avg Revenue per Item']
    ).properties(
        title="Average Revenue per Item",
        width=350,
        height=300
    )
    st.altair_chart(avg_item_chart, use_container_width=True)

st.divider()

# Quarterly Performance Analysis
st.subheader("📅 Quarterly Performance Analysis")

quarterly_data = fetch_quarterly_data(selected_year)
quarterly_df = pd.DataFrame(
    quarterly_data,
    columns=['Quarter', 'Quarterly Revenue', 'Quarterly Orders']
)

if not quarterly_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Quarterly revenue pie chart
        quarterly_revenue_pie = alt.Chart(quarterly_df).mark_arc(innerRadius=50).encode(
            theta=alt.Theta('Quarterly Revenue:Q'),
            color=alt.Color('Quarter:N', scale=alt.Scale(range=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])),
            tooltip=['Quarter', 'Quarterly Revenue']
        ).properties(
            title=f"Quarterly Revenue Distribution - {selected_year}",
            width=350,
            height=300
        )
        st.altair_chart(quarterly_revenue_pie, use_container_width=True)
    
    with col2:
        # Quarterly orders comparison
        quarterly_orders_bar = alt.Chart(quarterly_df).mark_bar(color="#2E86AB").encode(
            x=alt.X('Quarter:O', title='Quarter'),
            y=alt.Y('Quarterly Orders:Q', title='Number of Orders'),
            tooltip=['Quarter', 'Quarterly Orders']
        ).properties(
            title=f"Quarterly Orders - {selected_year}",
            width=350,
            height=300
        )
        st.altair_chart(quarterly_orders_bar, use_container_width=True)

st.divider()

# Top Performing Restaurants Analysis
st.subheader("🏆 Top Performing Restaurants Analysis")

restaurant_data = fetch_restaurant_performance_summary(selected_year)
restaurant_df = pd.DataFrame(
    restaurant_data,
    columns=['Restaurant Name', 'Annual Revenue', 'Annual Orders', 'Avg Revenue per Order', 'Active Months']
)

if not restaurant_df.empty:
    # Top 10 restaurants by revenue
    top_10_restaurants = restaurant_df.head(10)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Horizontal bar chart for top restaurants
        top_restaurants_chart = alt.Chart(top_10_restaurants).mark_bar(color="#2E86AB").encode(
            x=alt.X('Annual Revenue:Q', title='Annual Revenue (₹)'),
            y=alt.Y('Restaurant Name:N', sort='-x', title='Restaurant'),
            tooltip=['Restaurant Name', 'Annual Revenue', 'Annual Orders']
        ).properties(
            title=f"Top 10 Restaurants by Revenue - {selected_year}",
            width=400,
            height=400
        )
        st.altair_chart(top_restaurants_chart, use_container_width=True)
    
    with col2:
        # Scatter plot: Revenue vs Orders
        scatter_chart = alt.Chart(top_10_restaurants).mark_circle(size=100, color="#A23B72").encode(
            x=alt.X('Annual Orders:Q', title='Annual Orders'),
            y=alt.Y('Annual Revenue:Q', title='Annual Revenue (₹)'),
            tooltip=['Restaurant Name', 'Annual Revenue', 'Annual Orders', 'Avg Revenue per Order']
        ).properties(
            title=f"Revenue vs Orders Relationship - {selected_year}",
            width=400,
            height=400
        )
        st.altair_chart(scatter_chart, use_container_width=True)

    # Restaurant performance metrics table
    st.subheader("📋 Restaurant Performance Metrics")
    
    # Format the dataframe for better display
    formatted_restaurant_df = restaurant_df.copy()
    formatted_restaurant_df['Annual Revenue'] = formatted_restaurant_df['Annual Revenue'].apply(lambda x: f"₹{x:,.0f}")
    formatted_restaurant_df['Annual Orders'] = formatted_restaurant_df['Annual Orders'].apply(lambda x: f"{x:,}")
    formatted_restaurant_df['Avg Revenue per Order'] = formatted_restaurant_df['Avg Revenue per Order'].apply(lambda x: f"₹{x:,.0f}")
    
    # Apply styling
    styled_restaurant_df = formatted_restaurant_df.style.apply(highlight_rows, axis=1)
    st.dataframe(styled_restaurant_df, hide_index=True)

st.divider()

# Monthly Year-over-Year Comparison
if len(yearly_df) > 1:
    st.subheader("📈 Monthly Year-over-Year Comparison")
    
    # Fetch monthly data for all years
    monthly_all_years_data = fetch_monthly_data_all_years()
    monthly_all_df = pd.DataFrame(
        monthly_all_years_data,
        columns=['Year', 'Month', 'Total Revenue', 'Total Orders']
    )
    
    if not monthly_all_df.empty:
        # Add month names
        monthly_all_df['Month Name'] = monthly_all_df['Month'].map(month_mapping)
        monthly_all_df['Year'] = monthly_all_df['Year'].astype(str)
        
        # Create year-over-year comparison chart
        yoy_comparison = alt.Chart(monthly_all_df).mark_line(point=True).encode(
            x=alt.X('Month Name:O', sort=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], title='Month'),
            y=alt.Y('Total Revenue:Q', title='Revenue (₹)'),
            color=alt.Color('Year:N', scale=alt.Scale(range=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])),
            tooltip=['Year', 'Month Name', 'Total Revenue', 'Total Orders']
        ).properties(
            title="Monthly Revenue Comparison Across Years",
            width=800,
            height=400
        )
        st.altair_chart(yoy_comparison, use_container_width=True)

st.divider()

# Month Selection based on the selected year
if selected_year:

    #get the unique months
    month_sf_df = fetch_unique_months(selected_year)
    print(month_sf_df)
    #convert into df
    month_df = pd.DataFrame(
        month_sf_df,
        columns=['MONTH']
    )
    print(month_df)

    # Year Selection Box
    months = month_df["MONTH"].unique()
    default_month = max(months)  # Select the most recent year by default
    selected_month = st.selectbox(f"Select Month For {selected_year}", sorted(months), index=list(months).index(default_month))

    # Fetch and Display Data
    if selected_month:
        st.subheader(f"Top 10 Restaurants for {selected_month}/{selected_year}")
        top_restaurants = fetch_top_restaurants(selected_year, selected_month)
        if top_restaurants:
            top_restaurants_df = snowpark_to_pandas(top_restaurants)
            # Remove index from DataFrame by resetting it and dropping the index column
            #top_restaurants_df_reset = top_restaurants_df.reset_index(drop=True)

            # Display the DataFrame without index
            #st.dataframe(top_restaurants_df_reset)
            #st.dataframe(top_restaurants_df)

            # Apply the alternate color style
            styled_df = top_restaurants_df.style.apply(highlight_rows, axis=1)

            # Display the styled DataFrame
            st.dataframe(styled_df, hide_index= True)
        else:
            st.warning("No data found for the selected year and month.")