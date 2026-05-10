## Streamlit Dashboard Application

This project includes a Streamlit BI dashboard for analyzing financial transaction data from the live Neon PostgreSQL database.

### Dashboard Features

- Interactive date range filter
- Product category filter
- Account type filter
- Transaction channel filter
- KPI cards for total transactions, total transaction amount, and average transaction amount
- Monthly transaction amount visualization
- Product category transaction amount visualization
- Transaction channel distribution chart
- Recent transactions table
- Streamlit caching using `@st.cache_data`

### Run Locally

```bash
pip install -r requirements.txt
```

Create a local `.env` file or set the environment variable manually:

```bash
set DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
```

Then run:

```bash
streamlit run app.py
```

### Render Start Command

```bash
streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
```

### Required Render Environment Variable

```text
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DATABASE?sslmode=require
```
