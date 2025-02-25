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

        # Convert timestamp (assumed in milliseconds) to datetime and sort.
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
        
        # Filter the data based on sidebar selections.
        filtered_data = data[
            (data['filter_label'] == selected_filter) &
            (data['region'] == selected_region) &
            (data['resolution'] == selected_resolution)
        ]
        
        # Date range filter based on the filtered data (or fallback to overall date range).
        if not filtered_data.empty:
            min_date = filtered_data['datetime'].min().date()
            max_date = filtered_data['datetime'].max().date()
        else:
            min_date = data['datetime'].min().date()
            max_date = data['datetime'].max().date()
        date_range = st.sidebar.date_input("Select date range", [min_date, max_date], min_value=min_date, max_value=max_date)
        if len(date_range) == 2:
            start_date, end_date = date_range
            filtered_data = filtered_data[(filtered_data['datetime'].dt.date >= start_date) & (filtered_data['datetime'].dt.date <= end_date)]
        
        st.subheader("Data Table")
        st.dataframe(filtered_data)
        
        st.subheader("Interactive Bar Plot (Selected Filter)")
        if filtered_data.empty:
            st.info("No data available for the selected filters and date range.")
        else:
            chart = alt.Chart(filtered_data).mark_bar().encode(
                x=alt.X("datetime:T", title="Time", axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("value:Q", title="Energy Value"),
                tooltip=["datetime:T", "value:Q"]
            ).properties(
                width=700,
                height=400,
                title=f"Energy Data: {selected_filter} | {selected_region} | {selected_resolution}"
            ).interactive()
            st.altair_chart(chart, use_container_width=True)
        
        st.subheader("Aggregated Production vs Consumption")
        st.write("This plot shows, for each timestamp, the total energy for Stromerzeugung and Stromverbrauch. For Stromverbrauch, instead of plotting 'Gesamt (Netzlast)' directly, the chart computes 'Stromverbrauch: Sonstiges' as the difference between 'Gesamt (Netzlast)' and the sum of 'Residuallast' and 'Pumpspeicher'.")

        # For the aggregated chart, use data for the selected region and resolution, regardless of filter.
        agg_data = data[
            (data['region'] == selected_region) &
            (data['resolution'] == selected_resolution)
        ]
        # Apply date range filter.
        agg_data = agg_data[(agg_data['datetime'].dt.date >= start_date) & (agg_data['datetime'].dt.date <= end_date)]
        
        # Create a new column "category" from the filter_label.
        agg_data['category'] = agg_data['filter_label'].apply(lambda x: x.split(":")[0] if ":" in x else x)
        
        # Process "Stromverbrauch" category to compute "Sonstiges"
        verbrauch = agg_data[agg_data['category'] == "Stromverbrauch"].copy()
        verbrauch_grouped = []
        # Group by datetime, region, resolution (each timestamp group)
        for (dt, region, resolution), group in verbrauch.groupby(["datetime", "region", "resolution"]):
            # Sum values for each type in the group.
            gesamt = group[group['filter_label'].str.contains("Gesamt")]['value'].sum()
            residuallast = group[group['filter_label'].str.contains("Residuallast")]['value'].sum()
            pumpspeicher = group[group['filter_label'].str.contains("Pumpspeicher")]['value'].sum()
            # Compute "Sonstiges" as difference.
            sonstiges = gesamt - (residuallast + pumpspeicher)
            # Create rows for residuallast, pumpspeicher, and computed sonstiges.
            verbrauch_grouped.append({
                "datetime": dt,
                "filter_label": "Stromverbrauch: Residuallast",
                "region": region,
                "resolution": resolution,
                "value": residuallast,
                "category": "Stromverbrauch"
            })
            verbrauch_grouped.append({
                "datetime": dt,
                "filter_label": "Stromverbrauch: Pumpspeicher",
                "region": region,
                "resolution": resolution,
                "value": pumpspeicher,
                "category": "Stromverbrauch"
            })
            verbrauch_grouped.append({
                "datetime": dt,
                "filter_label": "Stromverbrauch: Sonstiges",
                "region": region,
                "resolution": resolution,
                "value": sonstiges,
                "category": "Stromverbrauch"
            })
        df_verbrauch = pd.DataFrame(verbrauch_grouped)
        
        # For Stromerzeugung, just use the existing data.
        erzeugung = agg_data[agg_data['category'] == "Stromerzeugung"]
        
                # Combine the transformed Stromverbrauch data with Stromerzeugung.
        agg_data_transformed = pd.concat([erzeugung, df_verbrauch], ignore_index=True)
        
        # Filter out rows with null or zero values.
        agg_data_transformed = agg_data_transformed[agg_data_transformed['value'].notnull() & (agg_data_transformed['value'] != 0)]
        
        # Create an aggregated chart with a shared y-axis.
        chart2 = alt.Chart(agg_data_transformed).mark_bar().encode(
            x=alt.X("datetime:T", title="Time", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("sum(value):Q", title="Total Energy Value"),
            color=alt.Color("filter_label:N", title="Filter Type"),
            tooltip=["datetime:T", "sum(value):Q", "filter_label:N"]
        ).facet(
            column=alt.Column("category:N", title="Category")
        ).resolve_scale(
            y="shared"
        )
        st.altair_chart(chart2, use_container_width=True)

        
        st.subheader("AI Data Description")
        prompt = st.text_area("Enter a prompt for the AI to describe the data", value="Please describe the following energy data:")
        if st.button("Get AI Description"):
            try:
                # Import the Ollama client.
                from ollama import Client

                def get_ai_response(prompt):
                    # Connect to the host's Ollama service.
                    client = Client(host="host.docker.internal")
                    response = client.chat(model='llama3:8b', messages=[
                        {'role': 'user', 'content': prompt}
                    ])
                    return response['message']['content']

                # Append a summary of the filtered data to the prompt if available.
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
