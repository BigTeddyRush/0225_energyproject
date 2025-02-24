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
    st.write("This dashboard shows the energy data loaded into the PostgreSQL database.")

    try:
        data = load_data()
        if data.empty:
            st.warning("No data available. Please ensure the ETL process has inserted data.")
            return

        # Convert the timestamp to a datetime column (assuming timestamp is in milliseconds)
        data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms')
        data = data.sort_values(by='datetime')

        # Sidebar filters for filter label, region, and resolution.
        st.sidebar.header("Filter Data")
        filter_labels = sorted(data['filter_label'].unique())
        selected_filter = st.sidebar.selectbox("Select Filter", filter_labels)
        
        region_options = sorted(data['region'].unique())
        selected_region = st.sidebar.selectbox("Select Region", region_options)
        
        resolution_options = sorted(data['resolution'].unique())
        selected_resolution = st.sidebar.selectbox("Select Resolution", resolution_options)
        
        # Filter the data based on selections.
        filtered_data = data[
            (data['filter_label'] == selected_filter) &
            (data['region'] == selected_region) &
            (data['resolution'] == selected_resolution)
        ]
        
        # Date range filter.
        if not filtered_data.empty:
            min_date = filtered_data['datetime'].min().date()
            max_date = filtered_data['datetime'].max().date()
        else:
            min_date = pd.to_datetime("today").date()
            max_date = pd.to_datetime("today").date()
        date_range = st.sidebar.date_input("Select date range", [min_date, max_date], min_value=min_date, max_value=max_date)
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_data = filtered_data[(filtered_data['datetime'].dt.date >= start_date) & (filtered_data['datetime'].dt.date <= end_date)]
        
        st.subheader("Data Table")
        st.dataframe(filtered_data)
        
        st.subheader("Interactive Bar Plot")
        if filtered_data.empty:
            st.info("No data available for the selected filters and date range.")
        else:
            # Create an interactive Altair bar chart.
            chart = alt.Chart(filtered_data).mark_bar().encode(
                x=alt.X("datetime:T", title="Time"),
                y=alt.Y("value:Q", title="Energy Value"),
                tooltip=["datetime:T", "value:Q"]
            ).properties(
                width=700,
                height=400,
                title=f"Energy Data: {selected_filter} | {selected_region} | {selected_resolution}"
            ).interactive()
            st.altair_chart(chart, use_container_width=True)

        st.subheader("AI Data Description")
        prompt = st.text_area("Enter a prompt for the AI to describe the data", value="Please describe the following energy data:")
        if st.button("Get AI Description"):
            try:
                # Import the Ollama client
                from ollama import Client

                def get_ai_response(prompt):
                    # Use host.docker.internal to reach your host's Ollama service
                    client = Client(host="host.docker.internal")
                    response = client.chat(model='llama3:8b', messages=[
                        {'role': 'user', 'content': prompt}
                    ])
                    return response['message']['content']

                # Optionally, append a summary of the filtered data to the prompt.
                if not filtered_data.empty:
                    data_summary = filtered_data.describe().to_string()
                    full_prompt = f"{prompt}\n\nData Summary:\n{data_summary}"
                else:
                    full_prompt = prompt

                ai_response = get_ai_response(full_prompt)
                st.write("### AI Response")
                st.write(ai_response)
            except Exception as e:
                st.error(f"Error fetching AI response: {e}")

    except Exception as e:
        st.error(f"Error loading data: {e}")

if __name__ == '__main__':
    main()
