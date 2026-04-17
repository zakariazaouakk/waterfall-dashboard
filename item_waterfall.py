import pandas as pd
import io
import re
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from collections import defaultdict

valid_sheets = ["Deljit QAD extraction", "Delfor QAD extraction"]
required_columns = ["Sales Order", "Item Number", "Customer Item", "Date", "Quantity"]

def extract_week_from_filename(filename):
    match = re.search(r"CW-?(\d+)", filename, re.IGNORECASE)
    return int(match.group(1)) if match else 0

def clean_columns(df):
    df.columns = df.columns.str.strip().str.replace('\n', '').str.replace('\r', '')
    return df

def clean_sheet(df):
    df = clean_columns(df)
    df = df[required_columns].copy()
    for col in ["Sales Order", "Item Number", "Quantity"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=["Sales Order", "Item Number", "Quantity", "Date"])
    return df

def year_week(date):
    iso = date.isocalendar()
    return f"W{iso[1]}-{iso[0]}"

def compute_variation(waterfall, row_file_indices, all_weeks, lookback):
    variation_col = []
    for row_idx, file_idx in enumerate(row_file_indices):
        if file_idx is None:
            variation_col.append('')
        elif file_idx < lookback:
            variation_col.append('')
        else:
            prev_row_idx = row_idx - lookback
            snapshot_week = all_weeks[file_idx]
            curr_val = waterfall.at[row_idx, snapshot_week]
            prev_val = waterfall.at[prev_row_idx, snapshot_week]
            try:
                curr_qty = float(curr_val)
                prev_qty = float(prev_val)
            except (ValueError, TypeError):
                variation_col.append('')
                continue
            if prev_qty == 0:
                variation_col.append('')
            else:
                pct = (curr_qty - prev_qty) / prev_qty
                variation_col.append(pct)
    return variation_col

def generate_item_waterfall(files_bytes_list):
    """Generate item-aggregated waterfall Excel file"""
    all_weeks_set = set()
    excel_data = []
    snapshot_weeks = []

    for file_bytes, file_name in files_bytes_list:
        week_num = extract_week_from_filename(file_name)
        snapshot_weeks.append(week_num)
        
        xl = pd.ExcelFile(file_bytes)
        excel_dict = {}
        for sheet_name in xl.sheet_names:
            if sheet_name in valid_sheets:
                df = xl.parse(sheet_name)
                df_type = 'Firm' if 'Deljit' in sheet_name else 'Forecast'
                df = clean_sheet(df)
                df['YearWeek'] = df['Date'].apply(year_week)
                df['DateStr'] = df['Date'].dt.strftime('%Y-%m-%d')
                df['SheetType'] = df_type
                excel_dict[df_type] = df
                all_weeks_set.update(df['YearWeek'].unique())
        excel_data.append(excel_dict)

    all_weeks = sorted(all_weeks_set, key=lambda x: (int(x.split('-')[1]), int(x[1:].split('-')[0])))

    item_rows = []
    item_row_file_indices = []

    # Get unique item numbers across all files
    all_items_sets = []
    for excel_dict in excel_data:
        df_firm = excel_dict.get('Firm', pd.DataFrame(columns=['Sales Order', 'Item Number', 'Customer Item', 'Date', 'Quantity', 'YearWeek', 'SheetType']))
        df_forecast = excel_dict.get('Forecast', pd.DataFrame(columns=['Sales Order', 'Item Number', 'Customer Item', 'Date', 'Quantity', 'YearWeek', 'SheetType']))
        items = pd.concat([df_firm[['Item Number']], df_forecast[['Item Number']]]).drop_duplicates()
        all_items_sets.append(items)

    unique_items_df = pd.concat(all_items_sets).drop_duplicates().sort_values('Item Number').reset_index(drop=True)

    for _, item_row in unique_items_df.iterrows():
        item_num = item_row['Item Number']

        for file_idx, excel_dict in enumerate(excel_data):
            row_dict = {
                'Item Number': item_num,
                'SnapshotWeek': f"CW{snapshot_weeks[file_idx]:02d}"
            }
            for w in all_weeks:
                row_dict[w] = 0

            df_firm = excel_dict.get('Firm', pd.DataFrame(columns=['Sales Order', 'Item Number', 'Customer Item', 'Date', 'Quantity', 'YearWeek', 'SheetType']))
            df_forecast = excel_dict.get('Forecast', pd.DataFrame(columns=['Sales Order', 'Item Number', 'Customer Item', 'Date', 'Quantity', 'YearWeek', 'SheetType']))

            firm_rows = df_firm[df_firm['Item Number'] == item_num].copy()
            forecast_rows = df_forecast[df_forecast['Item Number'] == item_num].copy()

            # Build set of (Sales Order, Customer Item, DateStr) combos with firm entries
            firm_keys = set(
                zip(firm_rows['Sales Order'], firm_rows['Customer Item'], firm_rows['DateStr'])
            )

            # Add firm quantities
            for _, r in firm_rows.groupby(['Sales Order', 'Customer Item', 'DateStr'], as_index=False)['Quantity'].sum().iterrows():
                week = year_week(pd.to_datetime(r['DateStr']))
                row_dict[week] += r['Quantity']

            # Add forecast only for combos without firm
            for _, r in forecast_rows.iterrows():
                key = (r['Sales Order'], r['Customer Item'], r['DateStr'])
                if key not in firm_keys:
                    week = year_week(pd.to_datetime(r['DateStr']))
                    row_dict[week] += r['Quantity']

            item_rows.append(row_dict)
            item_row_file_indices.append(file_idx)

        item_rows.append({col: '' for col in ['Item Number', 'SnapshotWeek'] + all_weeks})
        item_row_file_indices.append(None)

    if item_rows:
        item_rows.pop()
        item_row_file_indices.pop()

    waterfall_items = pd.DataFrame(item_rows)

    # Blank out weeks before snapshot
    for row_idx, file_idx in enumerate(item_row_file_indices):
        if file_idx is None:
            continue

        # Get snapshot week from filename (CW number)
        snapshot_week_num = snapshot_weeks[file_idx]

        # Find matching ISO week in all_weeks
        snapshot_week = next(
        (w for w in all_weeks if int(w.split('-')[0][1:]) == snapshot_week_num),
        None
    )

        if snapshot_week is None:
            continue  # safety

        snapshot_col_pos = all_weeks.index(snapshot_week)

        # Blank out previous weeks
        for col_pos in range(snapshot_col_pos):
            week_col = all_weeks[col_pos]
            waterfall_items.at[row_idx, week_col] = ''

    item_var_w1  = compute_variation(waterfall_items, item_row_file_indices, all_weeks, lookback=1)
    item_var_w2  = compute_variation(waterfall_items, item_row_file_indices, all_weeks, lookback=2)
    item_var_w4  = compute_variation(waterfall_items, item_row_file_indices, all_weeks, lookback=4)
    item_var_w13 = compute_variation(waterfall_items, item_row_file_indices, all_weeks, lookback=13)

    waterfall_items['W-1']  = item_var_w1
    waterfall_items['W-2']  = item_var_w2
    waterfall_items['W-4']  = item_var_w4
    waterfall_items['W-13'] = item_var_w13

    item_cols_order = ['Item Number', 'SnapshotWeek', 'W-1', 'W-2', 'W-4', 'W-13'] + all_weeks
    waterfall_items = waterfall_items[item_cols_order]

    # Create and format Excel
    output_buffer = io.BytesIO()
    waterfall_items.to_excel(output_buffer, index=False)
    output_buffer.seek(0)

    wb = load_workbook(output_buffer)
    ws = wb.active

    # Apply formatting
    HEADER_BG       = "1F4E79"
    HEADER_FONT     = "FFFFFF"
    VAR_COL_BG      = "BDD7EE"
    VAR_COL_BG_ALT  = "9DC3E6"
    ID_COL_BG       = "DEEAF1"
    ID_COL_BG_ALT   = "B8CCE4"
    WEEK_COL_BG     = "EBF3FB"
    WEEK_COL_BG_ALT = "D6E4F0"
    SEP_BG          = "F2F2F2"
    YELLOW          = "FFFF00"
    RED             = "FF0000"

    def solid(hex_color):
        return PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")

    header_fill  = solid(HEADER_BG)
    header_font  = Font(name="Arial", bold=True, color=HEADER_FONT, size=10)
    yellow_fill  = solid(YELLOW)
    red_fill     = solid(RED)
    white_font   = Font(name="Arial", color="FFFFFF", bold=True, size=10)
    sep_fill     = solid(SEP_BG)
    center_align = Alignment(horizontal="center", vertical="center")
    left_align   = Alignment(horizontal="left", vertical="center")

    header = [cell.value for cell in ws[1]]
    col_name_to_idx = {name: idx + 1 for idx, name in enumerate(header)}

    for col_idx, col_name in enumerate(header, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.fill      = header_fill
        cell.font      = header_font
        cell.alignment = center_align

    id_cols  = {'Item Number'}
    var_cols = {'W-1', 'W-2', 'W-4', 'W-13'}

    group_index   = []
    current_group = -1
    current_key   = None

    for row_idx, file_idx in enumerate(item_row_file_indices):
        if file_idx is None:
            group_index.append(None)
        else:
            key = waterfall_items.at[row_idx, 'Item Number']
            if key != current_key:
                current_key = key
                current_group += 1
            group_index.append(current_group)

    var_col_data  = {'W-1': item_var_w1, 'W-2': item_var_w2, 'W-4': item_var_w4, 'W-13': item_var_w13}
    red_threshold = {'W-1': 0.20, 'W-2': 0.20, 'W-4': 0.20, 'W-13': 0.20}

    for row_idx, file_idx in enumerate(item_row_file_indices):
        excel_row = row_idx + 2

        if file_idx is None:
            for col_idx in range(1, len(header) + 1):
                ws.cell(row=excel_row, column=col_idx).fill = sep_fill
            continue

        is_alt = (group_index[row_idx] % 2 == 1)

        for col_idx, col_name in enumerate(header, start=1):
            cell = ws.cell(row=excel_row, column=col_idx)
            if col_name in id_cols:
                cell.fill      = solid(ID_COL_BG_ALT if is_alt else ID_COL_BG)
                cell.alignment = left_align
                cell.font      = Font(name="Arial", size=10)
            elif col_name in var_cols:
                cell.fill      = solid(VAR_COL_BG_ALT if is_alt else VAR_COL_BG)
                cell.alignment = center_align
                cell.font      = Font(name="Arial", size=10)
            elif col_name == 'SnapshotWeek':
                cell.fill      = solid(YELLOW)
                cell.alignment = center_align
                cell.font      = Font(name="Arial", bold=True, size=10)
            else:
                cell.fill      = solid(WEEK_COL_BG_ALT if is_alt else WEEK_COL_BG)
                cell.alignment = center_align
                cell.font      = Font(name="Arial", size=10)

        if file_idx is not None and file_idx < len(snapshot_weeks):
            snapshot_week_num = snapshot_weeks[file_idx]
            target_week_col = None
            for week in all_weeks:
                try:
                    week_num = int(week.split('-')[0][1:])
                    if week_num == snapshot_week_num:
                        target_week_col = week
                        break
                except (ValueError, IndexError):
                    continue
            
            if target_week_col and target_week_col in col_name_to_idx:
                diag_cell = ws.cell(row=excel_row, column=col_name_to_idx[target_week_col])
                diag_cell.fill = yellow_fill

        for col_name, col_data in var_col_data.items():
            col_idx = col_name_to_idx.get(col_name)
            if not col_idx:
                continue
            raw_val = col_data[row_idx]
            cell = ws.cell(row=excel_row, column=col_idx)
            if isinstance(raw_val, float):
                cell.value         = raw_val
                cell.number_format = '0.0%'
                if abs(raw_val) >= red_threshold[col_name]:
                    cell.fill = red_fill
                    cell.font = white_font

    col_widths = {
        'Item Number':   14,
        'SnapshotWeek':  12,
        'W-1':           9,
        'W-2':           9,
        'W-4':           9,
        'W-13':          9,
    }
    for col_idx, col_name in enumerate(header, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = col_widths.get(col_name, 11)

    ws.freeze_panes = "C2"
    ws.row_dimensions[1].height = 22

    final_buffer = io.BytesIO()
    wb.save(final_buffer)
    final_buffer.seek(0)
    return final_buffer
