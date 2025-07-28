import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from pathlib import Path
from dotenv import load_dotenv
import pickle
import io

# Load environment variables
load_dotenv()

# -------------- CONFIGURATION --------------
COLOR_PRIMARY = "#B38F00"
COLOR_SECONDARY = "#EEDC82"
COLOR_CARD_BG = "#B38F00"
COLOR_TEXT = "#000000"

st.set_page_config(page_title="FYT Analytics", layout="wide")
st.markdown(f"""
    <style>
        .stApp {{
            background-color: white;
            color: {COLOR_TEXT};
        }}
        
        [data-testid="metric-container"] {{
            background-color: {COLOR_CARD_BG} !important;
            padding: 15px !important;
            border-radius: 8px !important;
            border: 1px solid #ddd !important;
        }}
        
        [data-testid="metric-container"] > div {{
            color: white !important;
        }}
        
        [data-testid="metric-container"] [data-testid="metric-value"] {{
            color: white !important;
            font-weight: 700 !important;
        }}
        
        [data-testid="metric-container"] [data-testid="metric-label"] {{
            color: white !important;
            font-weight: 600 !important;
        }}
        
        .main > div {{
            padding-top: 1rem !important;
        }}
        
        .block-container {{
            padding-top: 2rem !important;
            padding-bottom: 1rem !important;
        }}
        
        @media (max-width: 768px) {{
            .stColumns > div {{
                margin-bottom: 1rem;
            }}
        }}
    </style>
""", unsafe_allow_html=True)

st.title("Forever Young Tours Analytics")

# -------------- DB CONNECTION --------------
@st.cache_resource
def connect_db():
    # Get and validate connection URL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        st.error("Database URL not found in environment variables")
        st.stop()
    
    # Ensure correct dialect
    db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    try:
        # Create engine with SSL
        engine = create_engine(
            db_url,
            connect_args={
                "sslmode": "require",
            }
        )
        
        # Test connection
        with engine.begin() as conn:
            result = conn.execute(text("SELECT 1"))
            if not result.scalar() == 1:
                raise ValueError("Connection test failed")
                
        return engine
        
    except Exception as e:
        st.error(f"""
        Database connection failed!
        Error: {str(e)}
        
        Please verify:
        1. Your IP is whitelisted in Aiven
        2. Database is running
        3. Credentials are correct
        """)
        st.stop()

engine = connect_db()

# -------------- DATA LOADING WITH PROPER CACHING --------------
def dataframe_to_bytes(df):
    """Convert DataFrame to bytes for caching"""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return buffer.getvalue()

def bytes_to_dataframe(bytes_data):
    """Convert bytes back to DataFrame"""
    buffer = io.BytesIO(bytes_data)
    return pd.read_excel(buffer)

@st.cache_data(ttl=3600)
def load_cached_data(_engine, query):
    """Load and cache data using Excel serialization"""
    try:
        df = pd.read_sql(text(query), _engine)
        return dataframe_to_bytes(df)
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def load_all_data():
    """Load all required data tables"""
    data = {}
    queries = {
        'bookings': "SELECT * FROM bookings",
        'payments': "SELECT * FROM payments",
        'commissions': "SELECT * FROM commissions",
        'tours': "SELECT * FROM tours",
        'clients': "SELECT * FROM clients",
        'advisors': "SELECT * FROM advisors",
        'revenues': "SELECT * FROM revenues",
        'users': "SELECT * FROM users"
    }
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, (name, query) in enumerate(queries.items()):
        status_text.text(f"Loading {name} data...")
        bytes_data = load_cached_data(engine, query)
        if bytes_data is not None:
            data[name] = bytes_to_dataframe(bytes_data)
        progress_bar.progress((i + 1) / len(queries))
    
    status_text.text("Data loading complete!")
    return data

# Load all data
data = load_all_data()
df_bookings = data['bookings']
df_payments = data['payments']
df_commissions = data['commissions']
df_tours = data['tours']
df_clients = data['clients']
df_advisors = data['advisors']
df_revenues = data['revenues']
df_users = data['users']

# -------------- FORMAT DATES --------------
date_columns = {
    'bookings': ['booking_date', 'travel_date'],
    'commissions': ['comm_pay_date'],
    'payments': ['payment_date'],
    'revenues': ['date'],
    'users': ['approved_on']
}

for df_name in ['df_bookings', 'df_commissions', 'df_payments', 'df_revenues', 'df_users']:
    df = globals()[df_name]
    cols = date_columns.get(df_name.replace('df_', ''), [])
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

# -------------- FILTERS --------------
with st.container():
    col1, col2, col3, col4 = st.columns(4)

    role_mapping = {
        'R001': 'admin',
        'R002': 'agent', 
        'R003': 'advisor',
        'R004': 'client'
    }
    
    reverse_role_mapping = {v: k for k, v in role_mapping.items()}
    
    roles = ["All"] + list(role_mapping.values())
    countries = ["All"] + df_users['country'].dropna().unique().tolist()
    agents_df = df_advisors.merge(df_users[['user_id', 'name']], on='user_id', how='left')
    agents = ["All"] + agents_df['name'].dropna().unique().tolist()

    months = df_bookings['booking_date'].dt.month_name().dropna().unique().tolist()
    months = sorted(months, key=lambda x: pd.to_datetime(x, format='%B').month)

    selected_role = col1.selectbox("Role", roles)
    selected_country = col2.selectbox("Country", countries)
    selected_month = col3.selectbox("Month", ["All"] + months)
    selected_agent = col4.selectbox("Agent", agents)

df_filtered = df_bookings.copy()

if selected_month != "All":
    df_filtered = df_filtered[df_filtered['booking_date'].dt.month_name() == selected_month]

if selected_agent != "All":
    agent_id = agents_df[agents_df['name'] == selected_agent]['advisor_id'].unique()
    df_filtered = df_filtered[df_filtered['advisor_id'].isin(agent_id)]

if selected_country != "All":
    user_ids = df_users[df_users['country'] == selected_country]['user_id'].unique()
    client_ids = df_clients[df_clients['user_id'].isin(user_ids)]['client_id'].unique()
    df_filtered = df_filtered[df_filtered['client_id'].isin(client_ids)]

if selected_role != "All":
    role_id = reverse_role_mapping[selected_role]
    role_user_ids = df_users[df_users['role_id'] == role_id]['user_id'].unique()
    client_ids = df_clients[df_clients['user_id'].isin(role_user_ids)]['client_id'].unique()
    df_filtered = df_filtered[df_filtered['client_id'].isin(client_ids)]

# -------------- KPI METRICS --------------
with st.container():
    filtered_user_ids = set(df_users['user_id'].unique())
    
    if selected_country != "All":
        filtered_user_ids &= set(df_users[df_users['country'] == selected_country]['user_id'].unique())
    
    if selected_role != "All":
        role_id = reverse_role_mapping[selected_role]
        filtered_user_ids &= set(df_users[df_users['role_id'] == role_id]['user_id'].unique())
    
    if selected_agent != "All":
        agent_id = agents_df[agents_df['name'] == selected_agent]['advisor_id'].unique()
        client_ids_for_agent = df_filtered[df_filtered['advisor_id'].isin(agent_id)]['client_id'].unique()
        user_ids_for_agent = df_clients[df_clients['client_id'].isin(client_ids_for_agent)]['user_id'].unique()
        filtered_user_ids &= set(user_ids_for_agent)
    
    if selected_month != "All":
        client_ids_for_month = df_filtered['client_id'].unique()
        user_ids_for_month = df_clients[df_clients['client_id'].isin(client_ids_for_month)]['user_id'].unique()
        filtered_user_ids &= set(user_ids_for_month)
    
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Advisors", df_filtered['advisor_id'].nunique())
    c2.metric("Total Revenue", f"${df_payments['amount'].sum():,.0f}")
    c3.metric("Bookings", df_filtered['booking_id'].nunique())
    c4.metric("Commissions", f"${df_commissions['commission_amount'].sum():,.0f}")
    c5.metric("Users", len(filtered_user_ids))

# -------------- AGENT BOOKINGS TABLE & BOOKINGS BY MONTH SIDE BY SIDE --------------
col_agent, col_month = st.columns(2)

with col_agent:
    st.subheader("Bookings by Agent")
    agent_summary = (
        df_filtered.groupby('advisor_id')
        .agg(count=('booking_id', 'count'), amount=('total_amount', 'sum'))
        .reset_index()
        .merge(df_advisors[['advisor_id', 'advisorcode']], on='advisor_id', how='left')
    )
    agent_summary = agent_summary.rename(columns={'advisorcode': 'Agent Code'})
    st.dataframe(
        agent_summary[['Agent Code', 'count', 'amount']]
        .rename(columns={'count': 'Bookings', 'amount': 'Total Amount'})
        .style.format({"Total Amount": "${:,.0f}"}),
        height=400,
        use_container_width=True
    )

with col_month:
    st.subheader("Bookings by Travel Month")
    status_options = ["All"] + df_filtered['status'].dropna().unique().tolist()
    selected_status = st.selectbox("Filter by Booking Status", status_options, key="status_filter")

    if selected_status != "All":
        monthly_counts = df_filtered[df_filtered['status'] == selected_status]
    else:
        monthly_counts = df_filtered.copy()

    monthly_counts = monthly_counts['travel_date'].dt.month_name().value_counts().reset_index()
    monthly_counts.columns = ['Month', 'Booking Count']

    months_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                    'July', 'August', 'September', 'October', 'November', 'December']
    monthly_counts['Month'] = pd.Categorical(monthly_counts['Month'], categories=months_order, ordered=True)
    monthly_counts = monthly_counts.sort_values('Month')

    fig_month = px.bar(
        monthly_counts, 
        x='Month', 
        y='Booking Count', 
        color_discrete_sequence=[COLOR_PRIMARY],
        title=f"Bookings by Travel Month {f'(Status: {selected_status})' if selected_status != 'All' else ''}"
    )
    fig_month.update_layout(
        xaxis_title='Travel Month',
        yaxis_title='Number of Bookings',
        height=400
    )
    st.plotly_chart(fig_month, use_container_width=True)

# -------------- BOOKINGS BY STATUS & PAYMENT STATUS --------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("✅ Bookings by Status")
    status_counts = df_filtered['status'].value_counts().reset_index()
    status_counts.columns = ['status', 'count']
    fig_status = px.pie(
        status_counts, 
        values='count', 
        names='status', 
        hole=0.5,
        color_discrete_sequence=px.colors.sequential.Plasma_r
    )
    st.plotly_chart(fig_status, use_container_width=True)

with col_right:
    st.subheader("💳 Bookings by Payment Status")
    payment_status = df_filtered['payment_status'].value_counts().reset_index()
    payment_status.columns = ['payment_status', 'count']
    fig_payment = px.pie(
        payment_status, 
        values='count', 
        names='payment_status', 
        hole=0,
        color_discrete_sequence=px.colors.sequential.Plasma_r
    )
    st.plotly_chart(fig_payment, use_container_width=True)

# -------------- CLIENT ORIGIN TABLE --------------
st.subheader("🌍 Top 5 Client Origin Countries")
client_ids = df_filtered['client_id'].unique()
user_ids = df_clients[df_clients['client_id'].isin(client_ids)]['user_id'].unique()
top_countries = df_users[df_users['user_id'].isin(user_ids)]['country'].value_counts().head(5).reset_index()
top_countries.columns = ['Country', 'Client Count']
st.dataframe(
    top_countries,
    column_config={
        "Country": st.column_config.TextColumn("Country"),
        "Client Count": st.column_config.NumberColumn("Client Count")
    },
    hide_index=True,
    use_container_width=True
)

# -------------- REVENUE VS COMMISSION LINE --------------
st.subheader("Revenue vs Commission vs Net Income")
fig_rev = go.Figure()
fig_rev.add_trace(go.Scatter(
    x=df_revenues['date'], 
    y=df_revenues['total_income'],
    mode='lines+markers',
    name='Total Income',
    line=dict(color=COLOR_PRIMARY, width=2),
    hovertemplate="Date: %{x}<br>Amount: $%{y:,.2f}<extra></extra>"
))
fig_rev.add_trace(go.Scatter(
    x=df_revenues['date'], 
    y=df_revenues['total_commissions'],
    mode='lines+markers',
    name='Commissions',
    line=dict(color='#FFAA00', width=2),
    hovertemplate="Date: %{x}<br>Amount: $%{y:,.2f}<extra></extra>"
))
fig_rev.add_trace(go.Scatter(
    x=df_revenues['date'], 
    y=df_revenues['net_income'],
    mode='lines+markers',
    name='Net Income',
    line=dict(color='#004d00', width=2),
    hovertemplate="Date: %{x}<br>Amount: $%{y:,.2f}<extra></extra>"
))
fig_rev.update_layout(
    xaxis_title='Date',
    yaxis_title='Amount (USD)',
    template='plotly_white',
    hovermode='x unified',
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    )
)
st.plotly_chart(fig_rev, use_container_width=True)

# -------------- FOOTER --------------
st.divider()
st.caption(f"Forever Young Tours Analytics Dashboard | Last Updated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}")