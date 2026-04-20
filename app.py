import streamlit as st
import io
from detail_waterfall import generate_detail_waterfall
from item_waterfall import generate_item_waterfall
from utils import (
    extract_week_from_filename,
    load_excel_data,
    read_waterfall_snapshots,
    merge_excel_data,
)

st.set_page_config(page_title="Waterfall Generator", page_icon="📊", layout="centered")

st.title("Waterfall Generator")
st.caption("Upload your Excel files and download the result.")

# ── Report type ───────────────────────────────────────────────────────────────
report_type = st.radio(
    "Select Report Type:",
    ["Detail Waterfall (by Sales Order, Item, Customer Item)",
     "Item Number Waterfall (aggregated by Item Number)"],
    horizontal=True,
)
is_detail = "Detail" in report_type

# ── Optional: previous waterfall ─────────────────────────────────────────────
st.markdown("#### Previous Waterfall *(optional)*")
st.caption(
    "Upload a waterfall you generated before to continue from where you left off. "
    "Only upload new CW file(s) below — weeks already in the waterfall will be rejected."
)
prev_waterfall_file = st.file_uploader(
    "Previous waterfall Excel",
    type=["xlsx"],
    key="prev_waterfall",
    label_visibility="collapsed",
)

# ── New CW files ──────────────────────────────────────────────────────────────
st.markdown("#### New CW File(s)")
uploaded_files = st.file_uploader(
    "Excel files",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
    label_visibility="collapsed",
)

# ── Generate ──────────────────────────────────────────────────────────────────
if uploaded_files:
    if st.button("Generate", use_container_width=True, type="primary"):
        with st.spinner("Processing..."):
            try:
                # ── Deduplicate new files by CW week ─────────────────────────
                unique_files   = {}
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

                if duplicate_weeks:
                    st.warning(
                        f"Duplicate weeks among uploaded files — kept one of each: "
                        f"{sorted(duplicate_weeks)}"
                    )

                # Sort new files chronologically
                files_bytes_list = [
                    (io.BytesIO(f.read()), f.name)
                    for f in sorted(unique_files.values(),
                                    key=lambda f: extract_week_from_filename(f.name))
                ]

                # ── Load new CW data ──────────────────────────────────────────
                new_data = load_excel_data(files_bytes_list)

                # ── Merge with previous waterfall if provided ─────────────────
                if prev_waterfall_file is not None:
                    waterfall_type = "detail" if is_detail else "item"
                    prev_data = read_waterfall_snapshots(
                        io.BytesIO(prev_waterfall_file.read()), waterfall_type
                    )
                    # merge_excel_data raises ValueError on duplicate CW weeks
                    merged = merge_excel_data(prev_data, new_data)
                    excel_data, snapshot_weeks, all_weeks_set = merged
                    # Re-pack into the format the generators expect
                    files_bytes_list_final = None   # signal: use pre-loaded data
                else:
                    excel_data, snapshot_weeks, all_weeks_set = new_data
                    files_bytes_list_final = None

                # ── Generate report ───────────────────────────────────────────
                # Pass pre-loaded data by injecting it; generators accept
                # files_bytes_list OR pre_loaded tuple via keyword argument.
                if is_detail:
                    result = generate_detail_waterfall(
                        files_bytes_list,
                        pre_loaded=(excel_data, snapshot_weeks, all_weeks_set),
                    )
                    filename = "waterfall_detail.xlsx"
                else:
                    result = generate_item_waterfall(
                        files_bytes_list,
                        pre_loaded=(excel_data, snapshot_weeks, all_weeks_set),
                    )
                    filename = "waterfall_by_item.xlsx"

                st.success("✅ Waterfall generated successfully!")

                st.download_button(
                    label="Download",
                    data=result,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    type="primary",
                )

            except ValueError as e:
                st.error(str(e))
            except Exception as e:
                st.error(f"Error: {str(e)}")
                import traceback
                st.code(traceback.format_exc())
