import io
import json
import os
import re
import psycopg2
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv
from detail_waterfall import generate_detail_waterfall
from item_waterfall import generate_item_waterfall

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_CONFIG = dict(
    host     = os.getenv("DB_HOST", "localhost"),
    database = os.getenv("DB_NAME", "waterfall_db"),
    user     = os.getenv("DB_USER", "postgres"),
    password = os.getenv("DB_PASSWORD"),
    port     = int(os.getenv("DB_PORT", 5432)),
)

client = OpenAI(api_key=OPENAI_API_KEY)

# ── Database schema description for the AI ────────────────────────────────────
DB_SCHEMA = """
You have access to the following in PostgreSQL:

-- RAW TABLES (avoid querying these directly for analysis) --

Table: firm_orders
Description: Confirmed/firm delivery orders (from Deljit EDI messages)
Columns:
  - snapshot_week INT     -- CW week number, e.g. 11 for CW11
  - sales_order   BIGINT
  - item_number   BIGINT
  - customer_item TEXT
  - date          DATE
  - quantity      FLOAT

Table: forecasts
Description: Forecast/planned orders (from Delfor EDI messages)
Same columns as firm_orders.

-- PREFERRED VIEW (always use this for analysis questions) --

View: combined_orders
Description: Firm and forecast combined with correct deduplication.
             If a (sales_order, item_number, customer_item, date, snapshot_week)
             exists in both firm and forecast, ONLY the firm entry is kept.
             This matches the business logic used in the waterfall reports.
Columns:
  - snapshot_week INT
  - sales_order   BIGINT
  - item_number   BIGINT
  - customer_item TEXT
  - date          DATE
  - quantity      FLOAT
  - source        TEXT  -- 'firm' or 'forecast'

Important rules:
  - ALWAYS query combined_orders for any analysis or comparison question
  - Only query firm_orders directly if the user explicitly says "firm only" or "confirmed only"
  - Only query forecasts directly if the user explicitly says "forecast only" or "planned only"
  - snapshot_week is an integer (11, 12, 13...), not a string
  - Always use SUM(quantity) when aggregating
  - A drop or change between weeks means comparing SUM(quantity)
    for the same item across different snapshot_weeks
"""

# ── DB helper ─────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ── Tools (functions the AI can call) ─────────────────────────────────────────

def list_sales_orders() -> list:
    """Return all distinct sales orders in the DB."""
    conn = get_conn()
    df = pd.read_sql(
        "SELECT DISTINCT sales_order FROM firm_orders ORDER BY sales_order", conn
    )
    conn.close()
    return df["sales_order"].astype(str).tolist()


def list_items(sales_order: int = None) -> list:
    """Return distinct item numbers, optionally filtered by sales order."""
    conn = get_conn()
    if sales_order:
        df = pd.read_sql(
            """
            SELECT DISTINCT item_number FROM firm_orders
            WHERE sales_order = %s ORDER BY item_number
            """,
            conn,
            params=(sales_order,),
        )
    else:
        df = pd.read_sql(
            "SELECT DISTINCT item_number FROM firm_orders ORDER BY item_number", conn
        )
    conn.close()
    return df["item_number"].astype(str).tolist()


def list_weeks() -> list:
    """Return all snapshot weeks available in the DB."""
    conn = get_conn()
    df = pd.read_sql(
        "SELECT DISTINCT snapshot_week FROM firm_orders ORDER BY snapshot_week", conn
    )
    conn.close()
    return [f"CW{w:02d}" for w in df["snapshot_week"].tolist()]


def query_data(question: str) -> dict:
    """
    Convert a natural language question to SQL, execute it, and return
    both the SQL used and a formatted answer.
    """
    # ── Step 1: gpt-4o-mini writes the SQL ───────────────────────────────────
    sql_response = client.chat.completions.create(
        model       = "gpt-4o-mini",
        temperature = 0,
        messages    = [
            {
                "role": "system",
                "content": f"""
You are an expert PostgreSQL data analyst.
Given a question, write a single valid PostgreSQL SELECT query to answer it.
{DB_SCHEMA}
Rules:
- Return ONLY the raw SQL query, no explanation, no markdown, no backticks
- Never use DROP, DELETE, INSERT, UPDATE or any destructive statement
- Always LIMIT results to 50 rows maximum unless the question asks for all
- Use clear column aliases so results are readable
""",
            },
            {"role": "user", "content": question},
        ],
    )

    sql = sql_response.choices[0].message.content.strip()

    # ── Step 2: Safety check — block destructive queries ─────────────────────
    destructive_pattern = re.compile(
        r'\b(DROP|DELETE|INSERT|UPDATE|TRUNCATE|ALTER)\b',
        re.IGNORECASE,
    )
    sql_no_strings = re.sub(r"'[^']*'", "", sql)
    sql_no_aliases = re.sub(r'\bAS\s+\w+', "", sql_no_strings, flags=re.IGNORECASE)

    if destructive_pattern.search(sql_no_aliases):
        return {"error": "Query blocked for safety reasons.", "sql": sql}

    # ── Step 3: Execute the SQL ───────────────────────────────────────────────
    conn = get_conn()
    try:
        result_df = pd.read_sql(sql, conn)
    except Exception as e:
        return {"error": f"SQL execution failed: {str(e)}", "sql": sql}
    finally:
        conn.close()

    # ── Step 4: Format the result ─────────────────────────────────────────────
    if len(result_df) <= 5 and len(result_df.columns) <= 4:
        formatted_answer = result_df.to_string(index=False)
    else:
        format_response = client.chat.completions.create(
            model    = "gpt-4o-mini",
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a supply chain analyst. Format the query result "
                        "into a clear, concise answer. Use bullet points or a small "
                        "table if it helps. Be specific with numbers."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Question: {question}\n\n"
                        f"Query result:\n{result_df.to_string(index=False)}"
                    ),
                },
            ],
        )
        formatted_answer = format_response.choices[0].message.content

    return {
        "answer": formatted_answer,
        "sql":    sql,
        "rows":   len(result_df),
    }


def get_waterfall(
    waterfall_type: str,
    weeks: list,
    sales_orders: list = None,
    item_numbers: list = None,
) -> tuple:
    """Generate a waterfall Excel from the DB and return the file buffer."""
    from utils import year_week

    conn = get_conn()

    week_filter = f"AND snapshot_week = ANY(ARRAY{weeks})"
    so_filter   = f"AND sales_order = ANY(ARRAY{sales_orders})" if sales_orders else ""
    item_filter = f"AND item_number  = ANY(ARRAY{item_numbers})" if item_numbers else ""

    firm_df = pd.read_sql(
        f"""
        SELECT snapshot_week, sales_order, item_number, customer_item, date, quantity
        FROM firm_orders WHERE 1=1 {week_filter} {so_filter} {item_filter}
        """,
        conn,
    )
    fore_df = pd.read_sql(
        f"""
        SELECT snapshot_week, sales_order, item_number, customer_item, date, quantity
        FROM forecasts WHERE 1=1 {week_filter} {so_filter} {item_filter}
        """,
        conn,
    )
    conn.close()

    if firm_df.empty and fore_df.empty:
        return None, "No data found for those filters."

    excel_data          = []
    snapshot_weeks_list = sorted(weeks)
    all_weeks_set       = set()

    for week in snapshot_weeks_list:
        f = firm_df[firm_df["snapshot_week"] == week].copy()
        p = fore_df[fore_df["snapshot_week"] == week].copy()

        for df in [f, p]:
            df["Date"]      = pd.to_datetime(df["date"])
            df["YearWeek"]  = df["Date"].apply(year_week)
            df["DateStr"]   = df["Date"].dt.strftime("%Y-%m-%d")
            df["SheetType"] = "Firm"
            all_weeks_set.update(df["YearWeek"].unique())

        f = f.rename(columns={
            "sales_order": "Sales Order", "item_number": "Item Number",
            "customer_item": "Customer Item", "quantity": "Quantity",
        })
        p = p.rename(columns={
            "sales_order": "Sales Order", "item_number": "Item Number",
            "customer_item": "Customer Item", "quantity": "Quantity",
        })
        excel_data.append({"Firm": f, "Forecast": p})

    pre_loaded = (excel_data, snapshot_weeks_list, all_weeks_set)

    if waterfall_type == "detail":
        buf      = generate_detail_waterfall(pre_loaded)
        filename = "waterfall_detail.xlsx"
    else:
        buf      = generate_item_waterfall(pre_loaded)
        filename = "waterfall_by_item.xlsx"

    return buf, filename


# ── Tool definitions for OpenAI ───────────────────────────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name":        "list_sales_orders",
            "description": "List all sales orders available in the database.",
            "parameters":  {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name":        "list_items",
            "description": "List item numbers, optionally filtered by a sales order.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sales_order": {
                        "type":        "integer",
                        "description": "Sales order number to filter by (optional)",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name":        "list_weeks",
            "description": "List all snapshot CW weeks available in the database.",
            "parameters":  {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_waterfall",
            "description": (
                "Generate a waterfall Excel report from the database. "
                "Use this when the user asks for a waterfall, report, or export."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "waterfall_type": {
                        "type": "string",
                        "enum": ["detail", "item"],
                        "description": (
                            "'detail' for Sales Order/Item/Customer breakdown, "
                            "'item' for aggregated by item number"
                        ),
                    },
                    "weeks": {
                        "type":        "array",
                        "items":       {"type": "integer"},
                        "description": "List of CW week numbers to include, e.g. [11, 12, 13]",
                    },
                    "sales_orders": {
                        "type":        "array",
                        "items":       {"type": "integer"},
                        "description": "Optional list of sales order numbers to filter by",
                    },
                    "item_numbers": {
                        "type":        "array",
                        "items":       {"type": "integer"},
                        "description": "Optional list of item numbers to filter by",
                    },
                },
                "required": ["waterfall_type", "weeks"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_data",
            "description": (
                "Answer any question about the supply chain data using Text-to-SQL. "
                "Use this for questions like: which item dropped most, total quantities, "
                "comparing firm vs forecast, trends across weeks, top/bottom items, etc. "
                "Always use this instead of trying to answer data questions from memory."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type":        "string",
                        "description": "The full natural language question to answer from the database",
                    },
                },
                "required": ["question"],
            },
        },
    },
]

# ── Tool dispatch map ─────────────────────────────────────────────────────────
TOOL_MAP = {
    "list_sales_orders": list_sales_orders,
    "list_items":        list_items,
    "list_weeks":        list_weeks,
    "query_data":        query_data,
}


# ── Agent loop ────────────────────────────────────────────────────────────────
def run_agent(user_message: str, history: list) -> tuple:
    """
    Run one turn of the agent.
    Returns (text_response, file_buffer_or_None, filename_or_None)
    """
    messages = [
        {
            "role": "system",
            "content": (
                "You are a supply chain data assistant. You help users generate waterfall "
                "reports and answer questions about firm orders and forecasts stored in a "
                "PostgreSQL database. "
                "When the user asks for a waterfall, call get_waterfall with the right parameters. "
                "When the user asks a data question, always call query_data — never answer from memory. "
                "Always confirm what weeks/filters you are using before generating a waterfall."
            ),
        },
        *history,
        {"role": "user", "content": user_message},
    ]

    file_buffer = None
    filename    = None
    last_sql    = None
    last_rows   = None

    while True:
        response = client.chat.completions.create(
            model    = "gpt-4o-mini",
            messages = messages,
            tools    = TOOLS,
        )
        msg = response.choices[0].message

        # ── No tool call → final answer ───────────────────────────────────────
        if not msg.tool_calls:
            final_text = msg.content

            if last_sql:
                final_text = (
                    f"{msg.content}\n\n"
                    f"---\n"
                    f"```sql\n{last_sql}\n```\n"
                    f"*{last_rows} row(s) returned*"
                )

            return final_text, file_buffer, filename

        # ── Handle tool calls ─────────────────────────────────────────────────
        messages.append(msg)

        for tool_call in msg.tool_calls:
            fn_name = tool_call.function.name
            args    = json.loads(tool_call.function.arguments)

            if fn_name == "get_waterfall":
                file_buffer, filename = get_waterfall(**args)
                result = (
                    f"Waterfall generated successfully: {filename}"
                    if file_buffer
                    else filename
                )

            elif fn_name == "query_data":
                query_result = query_data(**args)

                if "error" in query_result:
                    last_sql  = query_result.get("sql")
                    last_rows = 0
                    result    = f"Error running query: {query_result['error']}"
                else:
                    last_sql  = query_result["sql"]
                    last_rows = query_result["rows"]
                    result    = query_result["answer"]

            else:
                fn     = TOOL_MAP[fn_name]
                result = fn(**args)

            messages.append({
                "role":         "tool",
                "tool_call_id": tool_call.id,
                "content":      json.dumps(result),
            })
