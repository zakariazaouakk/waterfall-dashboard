import streamlit as st
import io
import re
from detail_waterfall import generate_detail_waterfall
from utils import extract_week_from_filename
from item_waterfall import generate_item_waterfall

st.set_page_config(page_title="Waterfall Generator", page_icon="📊", layout="centered")

st.title("Waterfall Generator")
st.caption("Upload your Excel files and download the result.")

uploaded_files = st.file_uploader(
    "Excel files",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
    label_visibility="collapsed"
)

if uploaded_files:
    # Let user choose which report to generate
    report_type = st.radio(
        "Select Report Type:",
        ["Detail Waterfall (by Sales Order, Item, Customer Item)", 
         "Item Number Waterfall (aggregated by Item Number)"],
        horizontal=True
    )
    
    if st.button("Generate", use_container_width=True, type="primary"):
        with st.spinner("Processing..."):
            try:
                # Remove duplicates based on week number
                unique_files = {}
                duplicate_weeks = set()

                for f in uploaded_files:
                    week = extract_week_from_filename(f.name)

                    if week == 0:
                        st.error(f"File '{f.name}' does not contain a valid CW number.")
                        st.stop()

                    if week in unique_files:
                        duplicate_weeks.add(week)
                    else:
                        unique_files[week] = f

                # Warn user if duplicates found
                if duplicate_weeks:
                    st.warning(f"Duplicate weeks detected and ignored: {sorted(duplicate_weeks)}")

                # Convert to bytes list and sort by week
                files_bytes_list = [(io.BytesIO(f.read()), f.name) for f in unique_files.values()]
                files_bytes_list.sort(key=lambda x: extract_week_from_filename(x[1]))

                # Generate based on user selection
                if "Detail" in report_type:
                    result = generate_detail_waterfall(files_bytes_list)
                    filename = "waterfall_detail.xlsx"
                else:
                    result = generate_item_waterfall(files_bytes_list)
                    filename = "waterfall_by_item.xlsx"

                st.success("✅ Waterfall generated successfully!")
                
                st.download_button(
                    label="Download",
                    data=result,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary"
                )

            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
