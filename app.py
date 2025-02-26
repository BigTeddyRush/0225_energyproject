import streamlit as st
import psycopg2
import pandas as pd
import altair as alt
import pdfplumber
from ollama import Client

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

def extract_text_from_pdf(pdf_file):
    """Extracts text from a PDF file using pdfplumber."""
    try:
        with pdfplumber.open(pdf_file) as pdf:
            text = "\n".join([page.extract_text() or "" for page in pdf.pages])
        return text.strip()
    except Exception as e:
        return f"Error extracting text from PDF: {e}"

def summarize_text_with_ollama(text, model="llama3:8b"):
    """Sends extracted text to Ollama for summarization."""
    try:
        client = Client(host="http://host.docker.internal:11434")  # Connect to host machine running Ollama
        prompt = f"Summarize the following text:\n\n{text[:3000]}..."  # Limit text length for response efficiency
        response = client.chat(model=model, messages=[{"role": "user", "content": prompt}])
        return response['message']['content']
    except Exception as e:
        return f"Error with Ollama: {e}"

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
        
        # Filter data based on sidebar selections.
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

        # --- New Section: PDF Summarization ---
        st.subheader("📄 Upload a PDF for AI Summarization")
        uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])

        if uploaded_file is not None:
            with st.spinner("Extracting text from PDF..."):
                pdf_text = extract_text_from_pdf(uploaded_file)
            
            if pdf_text:
                st.subheader("Extracted Text (Preview)")
                st.text_area("Extracted Text", pdf_text[:1000], height=200)  # Show first 1000 chars
                
                if st.button("Summarize with AI"):
                    with st.spinner("Generating AI Summary..."):
                        summary = summarize_text_with_ollama(pdf_text)
                    st.subheader("AI-Generated Summary")
                    st.write(summary)
            else:
                st.error("No text extracted from the PDF. Please try another file.")

        # AI Data Description Section
        st.subheader("AI Data Description")
        prompt = st.text_area("Enter a prompt for the AI to describe the data", value="Please describe the following energy data:")
        if st.button("Get AI Description"):
            try:
                client = Client(host="http://host.docker.internal:11434")

                def get_ai_response(prompt):
                    response = client.chat(model="llama3:8b", messages=[{"role": "user", "content": prompt}])
                    return response['message']['content']

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
