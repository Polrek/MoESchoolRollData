# ----------------------------- CHANGE LOG ------------------------------
"""
2025-11-27 - Rhys Edlin - Create
2025-12-01 - Rhys Edlin - Add School Roll Data processing and matching
"""

# ------------------------------- Imports -------------------------------
import os # for os.path, os.makedirs
import re # for sanitising filenames
import json # for parsing JSON responses
import logging # for logging (duh!)
import subprocess # for opening folders
from time import sleep, strftime # for API call pacing and timestamped log filenames
import tkinter as tk # for UI
from tkinter import ttk, filedialog, messagebox, scrolledtext # for UI widgets
import requests # for HTTP requests
import pandas as pd # for data manipulation and Excel export

# ----------------------------- Constants ------------------------------

# MoE resource
MOE_DATASTORE_URL = "https://catalogue.data.govt.nz/api/3/action/datastore_search"
RESOURCE_ID = "4b292323-9fcc-41f8-814b-3c7b19cf14b3"

# Columns that should be treated as numeric for proper sorting/calculation
NUMERIC_COLUMNS = ["School_Id"]

# Column type mappings (all columns default to string if not specified here)
COLUMN_TYPES = {col: "numeric" for col in NUMERIC_COLUMNS}

# Application version
VERSION_MAJOR = 1
VERSION_MINOR = 0

# Application short name used for default filenames
APP_NAME = "MoESchools"

# Build string for UI (format: major.minor, e.g., "1.0")
BUILD_STR = f"{VERSION_MAJOR}.{VERSION_MINOR:01d}"

# Default pagination settings (can be changed in the UI)
DEFAULT_LIMIT_PER_PAGE = 1000       # rows
DEFAULT_REQUEST_TIMEOUT = 30        # seconds
DEFAULT_PAUSE_BETWEEN_CALLS = 0.2   # seconds

# Roll data settings
ROLL_DATA_START_COLUMN = 'AY'       # First column with funding year level data
ROLL_DATA_END_COLUMN = 'BM'         # Last column with funding year level data
ROLL_DATA_START_YEAR = 2010         # First available year in roll data

FIELDS = [ # this is ordered
    "School_Id","Org_Name","Telephone","Fax","Email","Contact1_Name","URL",
    "Add1_Line1","Add1_Suburb","Add1_City","Add2_Line1","Add2_Suburb",
    "Add2_City","Add2_Postal_Code","Authority","Territorial_Authority",
    "Regional_Council","General_Electorate","Māori_Electorate","Ward",
    "Latitude","Longitude","EQi_Index","Roll_Date","Total","European",
    "Māori","Pacific","Asian","MELAA","Other","International","Status",
    "DateSchoolOpened",
]

# Field aliases for export (maps internal field name to export column name)
# Customize these to rename columns in CSV/Excel output. If not specified, original field name is used.
FIELD_ALIASES = {
    "School_Id": "MoEID",
}
 
# ------------------------------ Logging -----------------------------
# Custom logging handler to send logs to Tkinter Text widget
class TextHandler(logging.Handler):
    def __init__(self, widget: scrolledtext.ScrolledText):
        super().__init__()
        self.widget = widget

    def emit(self, record):
        msg = self.format(record)
        self.widget.configure(state="normal")
        self.widget.insert(tk.END, msg + "\n")
        self.widget.see(tk.END)
        self.widget.configure(state="disabled")
# Setup logger
def make_logger(text_widget, level_name="INFO"):
    # Create logger
    logger = logging.getLogger("app")
    # Set the max level of logging)
    logger.setLevel(logging.DEBUG)

    # Clear existing handlers
    for h in list(logger.handlers):
        logger.removeHandler(h)
    # Text widget handler
    fmt = logging.Formatter("%(asctime)s | %(levelname)s (%(levelno)s) | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    th = TextHandler(text_widget)
    th.setFormatter(fmt)

    # Map UI level to handler level
    level_map = {
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "DEBUG": logging.DEBUG,
    }
    th.setLevel(level_map.get(level_name, logging.INFO))
    logger.addHandler(th)

    return logger

# -------------------------- Fetch & transform -------------------------
# Makes any blank null missing values into '-' for SMS import requirement
def normalize_value(val):
    if val is None:
        return "-"
    if isinstance(val, str):
        s = val.strip()
        return s if s != "" else "-"
    if isinstance(val, (list, dict)):
        return "-" if len(val) == 0 else val
    return val

# Only get the columns we care about, in order
def project_record(rec: dict) -> dict:
    return {f: normalize_value(rec.get(f, None)) for f in FIELDS}

# Fetch all records with pagination, projecting to desired fields
def fetch_all_projected_records(resource_id: str,
                                limit_per_page: int,
                                timeout: float,
                                pause: float,
                                logger: logging.Logger) -> list:
    session = requests.Session()

    # Fetch a single page
    def fetch_page(offset: int, limit: int):
        params = {"resource_id": resource_id, "limit": limit, "offset": offset}
        logger.debug(f"GET {MOE_DATASTORE_URL} params={params}")
        resp = session.get(MOE_DATASTORE_URL, params=params, timeout=timeout)
        logger.debug(f"HTTP {resp.status_code}")
        resp.raise_for_status()
        return resp.json()

    # 
    logger.info("Starting fetch (resource_id=%s)", resource_id)
    first = fetch_page(offset=0, limit=limit_per_page)
    if not first.get("success"):
        raise RuntimeError("""MoE Schools Data returned failure (success=False) on initial request.
                                Check the following:
                                - Is the MoE Data API online?
                                - Is the ResourceID correct?
                                - Do you have access through the firewall or proxy?""")
                           
    result = first.get("result", {})
    total = result.get("total") or result.get("records_total") # total number of records returned
    records = result.get("records", []) or [] # number of records on first page
    projected = [project_record(r) for r in records]

    if total is None:
        total = len(records)

    fetched = len(records)
    offset = fetched
    logger.info("Reported total: %s | First page rows: %s", total, fetched)

    while fetched < total:
        sleep(pause)
        page = fetch_page(offset=offset, limit=limit_per_page)
        if not page.get("success"):
            raise RuntimeError(f"MoE returned success=False at offset {offset}")

        result_obj = page.get("result", {})
        page_records = result_obj.get("records", []) or []
        if not page_records:
            logger.info("No more records at offset %s. Stopping.", offset)
            break

        projected.extend(project_record(r) for r in page_records)
        fetched += len(page_records)
        offset += len(page_records)
        logger.info("Fetched %s / %s", fetched, total)

    logger.info("Fetch complete. Total projected rows: %s", len(projected))
    return projected

# ----------------------- Roll Data Processing -------------------------

def get_available_years(logger: logging.Logger) -> list:
    """Return list of years from ROLL_DATA_START_YEAR to current year."""
    from datetime import datetime
    current_year = datetime.now().year
    years = list(range(ROLL_DATA_START_YEAR, current_year + 1))
    logger.debug(f"Available roll data years: {years}")
    return years

def load_roll_data(roll_file: str, selected_year: int, logger: logging.Logger) -> pd.DataFrame:
    """
    Load roll data from the Excel file for the specified year.
    Extracts 'All Students by Funding Year Level' data from the worksheet for the year.
    
    Returns DataFrame with School_Id and roll columns.
    """
    try:
        logger.info(f"Loading roll data from {roll_file} for year {selected_year}")
        
        # Read the sheet for the specified year
        sheet_name = str(selected_year)
        df = pd.read_excel(roll_file, sheet_name=sheet_name)
        
        logger.debug(f"Loaded sheet '{sheet_name}' with shape {df.shape}")
        
        # Find the School_Id column (typically first column)
        school_id_col = None
        for col in df.columns:
            if 'school' in str(col).lower() and 'id' in str(col).lower():
                school_id_col = col
                break
        
        if school_id_col is None and not df.empty:
            # Assume first column is School_Id if not found by name
            school_id_col = df.columns[0]
        
        logger.debug(f"Identified School_Id column: {school_id_col}")
        
        # Extract 'All Students by Funding Year Level' columns (AY:BM)
        # These columns contain the roll data
        cols_to_extract = [school_id_col]
        
        # Get column indices for the range AY:BM
        all_cols = list(df.columns)
        start_idx = None
        end_idx = None
        
        for i, col in enumerate(all_cols):
            col_letter = ''.join([c for c in str(col) if c.isalpha()])
            if col_letter.upper() == ROLL_DATA_START_COLUMN.upper():
                start_idx = i
            if col_letter.upper() == ROLL_DATA_END_COLUMN.upper():
                end_idx = i
        
        if start_idx is not None and end_idx is not None:
            cols_to_extract.extend(all_cols[start_idx:end_idx+1])
            logger.debug(f"Extracting roll data from columns {start_idx} to {end_idx} ({len(cols_to_extract)-1} year level columns)")
        else:
            logger.warning(f"Could not find roll data columns ({ROLL_DATA_START_COLUMN}:{ROLL_DATA_END_COLUMN}), using all columns except School_Id")
            cols_to_extract = [school_id_col]
        
        # Create result dataframe
        df_roll = df[cols_to_extract].copy()
        
        # Rename first column to School_Id for consistent matching
        df_roll.rename(columns={school_id_col: 'School_Id'}, inplace=True)
        
        # Convert School_Id to numeric for matching
        df_roll['School_Id'] = pd.to_numeric(df_roll['School_Id'], errors='coerce')
        
        # Remove rows with null School_Id
        df_roll = df_roll.dropna(subset=['School_Id'])
        df_roll['School_Id'] = df_roll['School_Id'].astype(int)
        
        logger.info(f"Loaded {len(df_roll)} schools with roll data for year {selected_year}")
        return df_roll
    
    except Exception as e:
        raise RuntimeError(f"Error loading roll data: {e}")

def match_schools_with_rolls(df_schools: pd.DataFrame, df_rolls: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Match API school data with roll data.
    Logs mismatches and returns merged dataframe.
    """
    logger.info(f"Matching {len(df_schools)} API schools with {len(df_rolls)} roll records")
    
    # Track schools from roll data without matching API data
    api_school_ids = set(df_schools['School_Id'].astype(int))
    roll_school_ids = set(df_rolls['School_Id'].astype(int))
    
    schools_only_in_roll = roll_school_ids - api_school_ids
    schools_only_in_api = api_school_ids - roll_school_ids
    
    # Log mismatches
    if schools_only_in_roll:
        logger.warning(f"Roll data contains {len(schools_only_in_roll)} schools NOT in API: {sorted(schools_only_in_roll)}")
    
    if schools_only_in_api:
        logger.warning(f"API contains {len(schools_only_in_api)} schools with NO roll data: {sorted(schools_only_in_api)}")
    
    # Merge dataframes on School_Id
    df_merged = df_schools.merge(df_rolls, on='School_Id', how='left', indicator=True)
    
    matched_count = len(df_merged[df_merged['_merge'] == 'both'])
    unmatched_count = len(df_merged[df_merged['_merge'] == 'left_only'])
    
    logger.info(f"Matched: {matched_count} schools | Unmatched: {unmatched_count} schools")
    
    # Drop the merge indicator column
    df_merged = df_merged.drop('_merge', axis=1)
    
    return df_merged

# ----------------------- Excel export (formatted table) ----------------

def export_csv(df: pd.DataFrame, out_dir: str, base_name: str, logger: logging.Logger) -> str:
    os.makedirs(out_dir, exist_ok=True)
    base = re.sub(r"[\\/:*?\"<>|]", "_", base_name).strip() or "schools_filtered"
    csv_path = os.path.join(out_dir, f"{base}.csv")
    
    # Rename columns using aliases
    df_export = df.copy()
    df_export = df_export.rename(columns={col: FIELD_ALIASES.get(col, col) for col in df_export.columns})
    
    df_export.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("CSV saved: %s", csv_path)
    return csv_path

def export_excel_table(df: pd.DataFrame, out_dir: str, base_name: str,
                       logger: logging.Logger) -> str:
    try:
        import xlsxwriter  # noqa: F401
    except ImportError:
        raise ImportError("XlsxWriter is required for formatted Excel tables. Install with: pip install XlsxWriter")

    os.makedirs(out_dir, exist_ok=True)
    base = re.sub(r"[\\/:*?\"<>|]", "_", base_name).strip() or "schools_filtered"
    xlsx_path = os.path.join(out_dir, f"{base}.xlsx")

    # Rename columns using aliases
    df_export = df.copy()
    df_export = df_export.rename(columns={col: FIELD_ALIASES.get(col, col) for col in df_export.columns})
    
    # Get aliased field names for table columns
    aliased_fields = [FIELD_ALIASES.get(col, col) for col in FIELDS]

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Schools")
        workbook = writer.book
        worksheet = writer.sheets["Schools"]

        # Freeze header row
        worksheet.freeze_panes(1, 0)

        # Define table range
        nrows, ncols = df_export.shape
        start_row, start_col = 0, 0
        end_row, end_col = nrows, ncols - 1

        # Add formatted table
        columns = [{"header": col} for col in aliased_fields]
        worksheet.add_table(
            start_row, start_col, end_row, end_col,
            {"header_row": True, "style": "Table Style Medium 9", "columns": columns}
        )

    logger.info("Excel saved (formatted table): %s", xlsx_path)
    return xlsx_path

# -------------------------------- UI ----------------------------------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MoE Schools Data Exporter")

        # Try to set Fire & Emergency NZ icon
        self._set_icon()

        self.geometry("900x620")
        self.resizable(True, True)

        # Variables
        self.var_outdir   = tk.StringVar(value=r"C:\temp")
        self.var_csv      = tk.BooleanVar(value=True)
        self.var_excel    = tk.BooleanVar(value=False)
        self.var_loglevel = tk.StringVar(value="INFO")
        self.var_filename = tk.StringVar(value=f"{APP_NAME}_{strftime('%Y%m%d')}")
        self.var_roll_data_file = tk.StringVar(value="")  # School Roll Data file path
        self.var_roll_year = tk.StringVar(value="")  # Selected year from roll data

        self.var_limit    = tk.IntVar(value=DEFAULT_LIMIT_PER_PAGE)
        self.var_timeout  = tk.DoubleVar(value=DEFAULT_REQUEST_TIMEOUT)
        self.var_pause    = tk.DoubleVar(value=DEFAULT_PAUSE_BETWEEN_CALLS)

        # Layout
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)
        frm.grid_columnconfigure(2, minsize=110) ##make third column wider for buttons TODO: make this not hardcoded

        # Resource is hardcoded, show read-only label
        ttk.Label(frm, text="Resource ID").grid(row=0, column=0, sticky="w")
        ttk.Label(frm, text=RESOURCE_ID).grid(row=0, column=1, sticky="w")
        # Build number display
        ttk.Label(frm, text=f"Build {BUILD_STR}").grid(row=0, column=2, sticky="e")

        # Output folder + filename
        ttk.Label(frm, text="Output folder:").grid(row=1, column=0, sticky="w", pady=(6,0))
        ttk.Entry(frm, textvariable=self.var_outdir, width=60).grid(row=1, column=1, sticky="we", padx=6, pady=(6,0))
        ttk.Button(frm, text="Browse...", command=self.browse_outdir).grid(row=1, column=2, sticky="w", pady=(6,0))

        ttk.Label(frm, text="Output file name:").grid(row=2, column=0, sticky="w", pady=(6,0))
        ttk.Entry(frm, textvariable=self.var_filename, width=40).grid(row=2, column=1, sticky="w", padx=6, pady=(6,0))

        # School Roll Data file (required)
        ttk.Label(frm, text="School Roll Data file (required):").grid(row=3, column=0, sticky="w", pady=(6,0))
        ttk.Entry(frm, textvariable=self.var_roll_data_file, width=60).grid(row=3, column=1, sticky="w", padx=6, pady=(6,0))
        ttk.Button(frm, text="Browse...", command=self.browse_roll_data_file).grid(row=3, column=2, sticky="w", pady=(6,0))
        ttk.Button(frm, text="ℹ", command=self.show_roll_data_info, width=3).grid(row=3, column=2, sticky="e", pady=(6,0))

        # Roll data year selection
        ttk.Label(frm, text="Roll data year:").grid(row=4, column=0, sticky="w", pady=(6,0))
        self.cbo_roll_year = ttk.Combobox(frm, textvariable=self.var_roll_year, state="readonly", width=12)
        self.cbo_roll_year.grid(row=4, column=1, sticky="w", padx=6, pady=(6,0))

        # Pagination settings
        grp = ttk.LabelFrame(frm, text="Pagination settings")
        grp.grid(row=5, column=0, columnspan=3, sticky="we", pady=8)
        ttk.Label(grp, text="Limit per page:").grid(row=0, column=0, sticky="w")
        ttk.Entry(grp, textvariable=self.var_limit, width=10).grid(row=0, column=1, sticky="w", padx=6)
        ttk.Label(grp, text="Request timeout (s):").grid(row=0, column=2, sticky="w")
        ttk.Entry(grp, textvariable=self.var_timeout, width=10).grid(row=0, column=3, sticky="w", padx=6)
        ttk.Label(grp, text="Pause between calls (s):").grid(row=0, column=4, sticky="w")
        ttk.Entry(grp, textvariable=self.var_pause, width=10).grid(row=0, column=5, sticky="w", padx=6)

        # Options
        opts = ttk.Frame(frm)
        opts.grid(row=6, column=0, columnspan=3, sticky="w", pady=8)
        ttk.Checkbutton(opts, text="Export CSV", variable=self.var_csv).grid(row=0, column=0, padx=(0,10))
        ttk.Checkbutton(opts, text="Export Excel", variable=self.var_excel).grid(row=0, column=1, padx=(0,10))

        # Log level
        ttk.Label(frm, text="Log level:").grid(row=7, column=0, sticky="w")
        cbo = ttk.Combobox(frm, textvariable=self.var_loglevel,
                           values=["ERROR","WARNING","INFO","DEBUG"],
                           state="readonly", width=12)
        cbo.set("INFO")
        cbo.grid(row=7, column=1, sticky="w")

        # Action button + status
        self.btn_generate = ttk.Button(frm, text="Generate", command=self.on_generate)
        self.btn_generate.grid(row=8, column=0, sticky="w", pady=8)
        self.btn_info = ttk.Button(frm, text="ℹ", command=self.show_info, width=3)
        self.btn_info.grid(row=8, column=1, sticky="w", padx=6, pady=8)
        self.lbl_status = ttk.Label(frm, text="")
        self.lbl_status.grid(row=8, column=2, sticky="w")
        
        self.btn_open_folder = ttk.Button(frm, text="Open Output Folder", command=self.open_output_folder)
        self.btn_open_folder.grid(row=9, column=0, sticky="w", pady=8)

        ttk.Label(frm, text="Logs:").grid(row=10, column=0, sticky="w", pady=(6,0))
        self.txt_logs = scrolledtext.ScrolledText(frm, height=18, wrap="word", state="disabled")
        self.txt_logs.grid(row=11, column=0, columnspan=3, sticky="nsew", pady=(0,6))

        # Column weights for resizing
        frm.columnconfigure(1, weight=1)
        frm.rowconfigure(10, weight=1)

    def _set_icon(self):
        """Set window icon from assets/fenz.ico; fallback to PhotoImage if needed."""
        try:
            ico_path = os.path.join(os.path.dirname(__file__), "assets", "fenz.ico")
            if os.path.exists(ico_path):
                self.iconbitmap(ico_path)  # best on Windows title bar/message windows
            else:
                # Fallback: try PNG if provided
                png_path = os.path.join(os.path.dirname(__file__), "assets", "fenz.png")
                if os.path.exists(png_path):
                    img = tk.PhotoImage(file=png_path)
                    self.iconphoto(True, img)
        except Exception:
            # Silently ignore icon failures
            pass

    def browse_outdir(self):
        d = filedialog.askdirectory(initialdir=self.var_outdir.get() or os.getcwd())
        if d:
            self.var_outdir.set(d)

    def browse_roll_data_file(self):
        """Browse for School Roll Data file (CSV or ZIP)."""
        f = filedialog.askopenfilename(
            initialdir=os.getcwd(),
            filetypes=[("Data files", "*.csv *.zip"), ("All files", "*.*")]
        )
        if f:
            self.var_roll_data_file.set(f)
            self.update_year_combobox()

    def update_year_combobox(self):
        """Update year combobox with available years."""
        try:
            # Create a temporary logger for this operation
            logger = make_logger(self.txt_logs, level_name="INFO")
            years = get_available_years(logger)
            year_strs = [str(y) for y in years]
            self.cbo_roll_year['values'] = year_strs
            # Set to most recent year
            if year_strs:
                self.cbo_roll_year.set(year_strs[-1])
        except Exception as e:
            messagebox.showerror("Error", f"Could not populate year list: {e}")

    def show_roll_data_info(self):
        """Display information about the School Roll Data file."""
        info_text = """School Roll Data File

File Name: Time series: Student rolls by school 2010-2025

Description: This file contains time series data of student rolls by school from 2010 to 2025 from the Ministry of Education.

Source: https://www.educationcounts.govt.nz/statistics/school-rolls

Instructions:
1. Visit the link above
2. Download the CSV or ZIP file
3. If ZIP, extract to access the CSV
4. Use the Browse button to select the file in this application

This field is REQUIRED to generate exports."""
        messagebox.showinfo("School Roll Data", info_text)

    def show_info(self):
        """Display application information."""
        info_text = f"""This application is designed to simplfy the MoE->FENZ annual data load process. It accesses the publically available endpoint for Schools data, formats it for SMS, and outputs a csv (optional xlsx) file for import into SMS.

    Created by Rhys Edlin 2025
    Build: {BUILD_STR}"""
        messagebox.showinfo("About", info_text)

    def open_output_folder(self):
        """Open the output folder in Windows Explorer."""
        out_dir = self.var_outdir.get() or os.getcwd()
        if os.path.exists(out_dir):
            subprocess.Popen(f'explorer "{out_dir}"')
        else:
            messagebox.showerror("Error", f"Output folder does not exist: {out_dir}")

    def on_generate(self):
        # Validate that School Roll Data file is provided
        roll_data_file = self.var_roll_data_file.get().strip()
        if not roll_data_file:
            messagebox.showerror("Missing Required File", "School Roll Data file is required.\n\nPlease select a file using the Browse button or click the ℹ icon for more information.")
            return
        
        if not os.path.exists(roll_data_file):
            messagebox.showerror("File Not Found", f"The selected School Roll Data file does not exist:\n\n{roll_data_file}")
            return
        
        # Validate year selection
        selected_year_str = self.var_roll_year.get().strip()
        if not selected_year_str:
            messagebox.showerror("Missing Year", "Please select a year from the Roll data year dropdown.")
            return
        
        try:
            selected_year = int(selected_year_str)
        except ValueError:
            messagebox.showerror("Invalid Year", f"Invalid year value: {selected_year_str}")
            return
        
        # Check if output files exist first
        out_dir   = self.var_outdir.get() or os.getcwd()
        base_name = self.var_filename.get()
        base = re.sub(r"[\\/:*?\"<>|]", "_", base_name).strip() or "schools_filtered"
        
        files_to_check = []
        if self.var_csv.get():
            files_to_check.append(os.path.join(out_dir, f"{base}.csv"))
        if self.var_excel.get():
            files_to_check.append(os.path.join(out_dir, f"{base}.xlsx"))
        
        # Check if any files exist and handle accordingly
        existing_files = [f for f in files_to_check if os.path.exists(f)]
        if existing_files:
            # Try to open the files to see if they're locked
            locked_files = []
            for file_path in existing_files:
                try:
                    with open(file_path, 'a'):
                        pass
                except IOError:
                    locked_files.append(file_path)
            
            if locked_files:
                messagebox.showinfo("File In Use", f"The following file(s) are open:\n\n" + "\n".join(locked_files) + "\n\nPlease close them before proceeding.")
                return
            
            # File exists but not locked, ask for confirmation
            if not messagebox.askyesno("File Exists", f"The following file(s) already exist:\n\n" + "\n".join(existing_files) + "\n\nOverwrite?"):
                return
        
        # Setup logger
        logger = make_logger(self.txt_logs, level_name=self.var_loglevel.get())

        # Disable while running
        self.btn_generate.config(state="disabled")
        self.lbl_status.config(text="Working...")

        try:
            # Read pagination inputs
            limit   = int(self.var_limit.get())
            timeout = float(self.var_timeout.get())
            pause   = float(self.var_pause.get())

            # Fetch data
            rows = fetch_all_projected_records(
                resource_id=RESOURCE_ID,
                limit_per_page=limit,
                timeout=timeout,
                pause=pause,
                logger=logger,
            )
            df = pd.DataFrame(rows, columns=FIELDS)
            
            # Apply column type conversions (default is string for unspecified columns)
            for col in FIELDS:
                col_type = COLUMN_TYPES.get(col, "string")
                if col_type == "numeric" and col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(df[col])
            
            # Sort by School_Id numerically
            df = df.sort_values('School_Id', key=lambda x: pd.to_numeric(x, errors='coerce')).reset_index(drop=True)

            # Load and match roll data
            logger.info("=" * 60)
            logger.info("Processing School Roll Data")
            logger.info("=" * 60)
            
            df_rolls = load_roll_data(roll_data_file, selected_year, logger)
            df_merged = match_schools_with_rolls(df, df_rolls, logger)
            
            logger.info("=" * 60)

            # Export selections
            outputs = []
            if self.var_csv.get():
                outputs.append(export_csv(df_merged, out_dir, base_name, logger))
            if self.var_excel.get():
                outputs.append(export_excel_table(df_merged, out_dir, base_name, logger))

            if outputs:
                self.lbl_status.config(text="Done.")
                messagebox.showinfo("Success", "Files generated:\n\n" + "\n".join(outputs))
            else:
                self.lbl_status.config(text="No exports selected.")
                messagebox.showwarning("No output", "Please select CSV and/or Excel export.")

        except requests.HTTPError as e:
            self.lbl_status.config(text="Error.")
            logging.getLogger("app").error("HTTP error: %s - %s", e.response.status_code, e.response.text, exc_info=False)
            messagebox.showerror("HTTP error", f"{e.response.status_code} - {e.response.text}")
        except requests.ConnectionError as e:
            self.lbl_status.config(text="Error.")
            logging.getLogger("app").error("Connection error: %s", e, exc_info=False)
            messagebox.showerror("Connection Error", "Unable to connect to the MoE data service.\n\nPlease check:\n- Your internet connection\n- Your network/proxy settings\n- The MoE data service is online")
        except requests.Timeout as e:
            self.lbl_status.config(text="Error.")
            logging.getLogger("app").error("Timeout error: %s", e, exc_info=False)
            messagebox.showerror("Timeout Error", "Request to MoE data service timed out.\n\nPlease check your internet connection and try again.")
        except ImportError as e:
            self.lbl_status.config(text="Dependency error.")
            logging.getLogger("app").error("Dependency error: %s", e, exc_info=False)
            messagebox.showerror("Dependency error", str(e))
        except Exception as e:
            self.lbl_status.config(text="Error.")
            logging.getLogger("app").error("Error: %s", e, exc_info=False)
            messagebox.showerror("Error", str(e))
        finally:
            self.btn_generate.config(state="normal")

# ------------------------------ Main ----------------------------------

if __name__ == "__main__":
    app = App()
    app.mainloop()
    
# ---------------------- Package Instructions --------------------------
"""
To create a standalone executable using PyInstaller, run the following commands in your terminal: # \\ for escaped backslash
python -m venv .venv
.venv\\Scripts\\activate
python -m pip install --upgrade pip
pip install requests pandas XlsxWriter pyinstaller
# Example: include build number in the packaged executable name
# Replace {BUILD_NUMBER} with the build number you want (or script the name using the BUILD_NUMBER constant)
# Example: include build number in the packaged executable name
# Use the zero-padded build string (5 digits) when naming the package
# For example: pyinstaller --onefile --noconsole --name "MoeSchools_b00001" ...
pyinstaller --onefile --noconsole --name "MoeSchools_b{BUILD_STR}" --icon assets\\fenz.ico --add-data "assets\\fenz.ico;assets" MoESchools.py
"""
