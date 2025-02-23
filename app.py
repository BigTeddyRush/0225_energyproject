import streamlit as st
import psycopg2
import pandas as pd
import altair as alt

def load_data():
    """Connect to the PostgreSQL database and load data into a Pandas DataFrame."""
    conn = psycopg2.connect(
        host="db",
        database="energydata",
        user="user",
        password="password"
    )
    df = pd.read_sql_query("SELECT * FROM energy_timeseries", conn)
    conn.close()
    return df

def main():
    st.title("Energy Timeseries Data Dashboard")
    st.write("This dashboard shows the energy data loaded into the PostgreSQL database as a table and an interactive bar plot.")

    try:
        data = load_data()
        if data.empty:
            st.warning("No data available. Please ensure the ETL process has inserted data.")
            return

        # Convert the timestamp to a datetime column (assuming timestamp is in milliseconds)
        data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms')
        data = data.sort_values(by='datetime')

        # Display the raw data in a table
        st.subheader("Data Table")
        st.dataframe(data)

        # Sidebar interactive date range filter
        st.sidebar.header("Filter Data")
        min_date = data['datetime'].min().date()
        max_date = data['datetime'].max().date()
        date_range = st.sidebar.date_input("Select date range", [min_date, max_date], min_value=min_date, max_value=max_date)
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_data = data[(data['datetime'].dt.date >= start_date) & (data['datetime'].dt.date <= end_date)]
        else:
            filtered_data = data

        st.subheader("Interactive Bar Plot")
        if filtered_data.empty:
            st.info("No data available for the selected date range.")
        else:
            # Create an interactive Altair bar chart
            # This chart uses the datetime for the x-axis and energy value for the y-axis.
            chart = alt.Chart(filtered_data).mark_bar().encode(
                x=alt.X("datetime:T", title="Time"),
                y=alt.Y("value:Q", title="Energy Value"),
                tooltip=["datetime:T", "value:Q"]
            ).properties(
                width=700,
                height=400,
                title="Energy Data Over Time"
            ).interactive()

            st.altair_chart(chart, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading data: {e}")

if __name__ == '__main__':
    main()
