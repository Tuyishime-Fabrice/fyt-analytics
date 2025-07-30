import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import os
from pathlib import Path
from dotenv import load_dotenv
import io
from datetime import datetime, date

# Load environment variables
load_dotenv()

# ---------------------------- CONFIGURATION ----------------------------
COLOR_PRIMARY = "#003366"  # Deep blue
COLOR_SECONDARY = "#FFA500"  # Orange accent
COLOR_BACKGROUND = "#F8F9FA"
COLOR_TEXT = "#333333"
COLOR_CARD = "#FFFFFF"
COLOR_BORDER = "#E0E0E0"

# Set page config
st.set_page_config(
    page_title="FYT Admin Dashboard",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium look
st.markdown(f"""
    <style>
        .stApp {{
            background-color: {COLOR_BACKGROUND};
            color: {COLOR_TEXT};
        }}
        
        .sidebar .sidebar-content {{
            background-color: {COLOR_PRIMARY};
            color: white;
        }}
        
        [data-testid="metric-container"] {{
            background-color: {COLOR_CARD} !important;
            padding: 20px !important;
            border-radius: 10px !important;
            border: 1px solid {COLOR_BORDER} !important;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        
        [data-testid="metric-container"] > div {{
            color: {COLOR_TEXT} !important;
        }}
        
        [data-testid="metric-container"] [data-testid="metric-value"] {{
            color: {COLOR_PRIMARY} !important;
            font-weight: 700 !important;
            font-size: 1.8rem !important;
        }}
        
        [data-testid="metric-container"] [data-testid="metric-label"] {{
            color: {COLOR_TEXT} !important;
            font-weight: 500 !important;
            font-size: 1rem !important;
        }}
        
        .stDataFrame {{
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        
        .stAlert {{
            border-radius: 10px;
        }}
        
        .css-1aumxhk {{
            background-color: {COLOR_PRIMARY};
            color: white;
        }}
        
        h1, h2, h3, h4, h5, h6 {{
            color: {COLOR_PRIMARY} !important;
        }}
        
        @media (max-width: 768px) {{
            .stColumns > div {{
                margin-bottom: 1rem;
            }}
            
            [data-testid="metric-container"] {{
                margin-bottom: 1rem;
            }}
        }}
    </style>
""", unsafe_allow_html=True)

# ---------------------------- DB CONNECTION ----------------------------
@st.cache_resource
def connect_db():
    # Try Streamlit secrets first (production)
    if 'database' in st.secrets:
        db_url = st.secrets.database.url
    # Fallback to .env (local development)
    else:
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            st.error("Database URL not found. Check secrets or .env file.")
            st.stop()
    
    # Force PostgreSQL protocol
    db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    try:
        engine = create_engine(db_url, connect_args={"sslmode": "require"})
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Connection failed: {e}")
        st.stop()

engine = connect_db()

# ---------------------------- DATA LOADING ----------------------------
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
    """Load all required data tables with correct schema"""
    queries = {
        'bookings': """
            SELECT b.*, t.destination, t.country as tour_country, 
                   t.titlename as tour_name, t.cost as tour_cost,
                   a.advisorcode, a.status as advisor_status,
                   c.client_type, c.organization, u.country, u.phonenumber
            FROM bookings b
            LEFT JOIN tours t ON b.tour_id = t.tour_id
            LEFT JOIN advisors a ON b.advisor_id = a.advisor_id
            LEFT JOIN clients c ON b.client_id = c.client_id
            LEFT JOIN users u ON c.user_id = u.user_id
        """,
        'payments': "SELECT * FROM payments",
        'commissions': "SELECT * FROM commissions",
        'revenues': "SELECT * FROM revenues",
        'advisors': """
            SELECT a.*, u.name, u.email, u.phonenumber, u.country
            FROM advisors a
            LEFT JOIN users u ON a.user_id = u.user_id
        """,
        'clients': """
            SELECT c.*, u.name, u.email, u.phonenumber, u.country, u.status as user_status
            FROM clients c
            LEFT JOIN users u ON c.user_id = u.user_id
        """,
        'tours': "SELECT * FROM tours",
        'users': "SELECT * FROM users",
        'tourspackage': "SELECT * FROM tourspackage"
    }
    
    data = {}
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, (name, query) in enumerate(queries.items()):
        status_text.text(f"Loading {name.replace('_', ' ').title()}...")
        bytes_data = load_cached_data(engine, query)
        if bytes_data is not None:
            data[name] = bytes_to_dataframe(bytes_data)
        progress_bar.progress((i + 1) / len(queries))
    
    status_text.text("Data loading complete!")
    progress_bar.empty()
    
    return data

data = load_all_data()

# Process data
df_bookings = data['bookings']
df_payments = data['payments']
df_commissions = data['commissions']
df_revenues = data['revenues']
df_advisors = data['advisors']
df_clients = data['clients']
df_tours = data['tours']
df_users = data['users']
df_tourspackage = data['tourspackage']

# Convert dates and handle NaT values
date_columns = {
    'bookings': ['booking_date', 'travel_date'],
    'payments': ['payment_date'],
    'commissions': ['comm_pay_date'],
    'revenues': ['date'],
    'tours': ['duration'],
    'users': ['approved_on'],
    'tourspackage': ['booked_on']
}

for df_name in data:
    df = data[df_name]
    cols = date_columns.get(df_name, [])
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Replace NaT with None for safety
            df[col] = df[col].where(pd.notnull(df[col]), None)

# ---------------------------- SIDEBAR FILTERS ----------------------------
with st.sidebar:
    st.title("🔍 Dashboard Filters")
    
    # Date range - Fixed with proper date handling
    st.subheader("Date Range")
    
    # Get min and max dates safely
    valid_dates = df_bookings['booking_date'].dropna()
    if not valid_dates.empty:
        min_date = valid_dates.min().to_pydatetime().date()
        max_date = valid_dates.max().to_pydatetime().date()
    else:
        min_date = date(2023, 1, 1)
        max_date = date.today()
    
    date_range = st.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Payment status
    st.subheader("Payment Status")
    payment_status = st.multiselect(
        "Filter by Payment Status",
        options=df_bookings['payment_status'].dropna().unique(),
        default=df_bookings['payment_status'].dropna().unique()
    )
    
    # Booking status
    st.subheader("Booking Status")
    booking_status = st.multiselect(
        "Filter by Booking Status",
        options=df_bookings['status'].dropna().unique(),
        default=df_bookings['status'].dropna().unique()
    )
    
    # Client type
    st.subheader("Client Type")
    client_type = st.selectbox(
        "Filter by Client Type",
        options=["All"] + df_clients['client_type'].dropna().unique().tolist()
    )
    
    # Country filter
    st.subheader("Country")
    country = st.selectbox(
        "Filter by Country",
        options=["All"] + df_users['country'].dropna().unique().tolist()
    )
    
    # Advisor search
    st.subheader("Advisor Search")
    advisor = st.text_input("Search by Advisor Code or Name")
    
    # Destination search
    st.subheader("Destination Search")
    destination = st.text_input("Search by Destination")
    
    # Download button
    st.subheader("Export Data")
    if st.button("📊 Export Dashboard Data"):
        with st.spinner("Preparing data for export..."):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for name, df in data.items():
                    df.to_excel(writer, sheet_name=name, index=False)
            st.download_button(
                label="⬇️ Download Excel Report",
                data=output.getvalue(),
                file_name="fyt_dashboard_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# Apply filters with proper date comparison
def apply_filters(df):
    # Date range filter - fixed date comparison
    if len(date_range) == 2:
        start_date, end_date = date_range
        # Convert dates to datetime for proper comparison
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        mask = (df['booking_date'].notna() & 
                (df['booking_date'] >= start_dt) & 
                (df['booking_date'] <= end_dt))
        df = df[mask]
    
    # Payment status filter
    if payment_status:
        df = df[df['payment_status'].isin(payment_status)]
    
    # Booking status filter
    if booking_status:
        df = df[df['status'].isin(booking_status)]
    
    # Client type filter
    if client_type != "All":
        df = df[df['client_type'] == client_type]
    
    # Country filter
    if country != "All":
        df = df[df['country'] == country]
    
    # Advisor search
    if advisor:
        advisor_mask = (
            df['advisorcode'].astype(str).str.contains(advisor, case=False) | 
            df['name'].astype(str).str.contains(advisor, case=False)
        )
        df = df[advisor_mask]
    
    # Destination search
    if destination:
        df = df[df['destination'].astype(str).str.contains(destination, case=False)]
    
    return df

df_filtered = apply_filters(df_bookings)

# ---------------------------- DASHBOARD LAYOUT ----------------------------
st.title("Forever Young Tours - Admin Dashboard")

# Fixed HTML template using f-string instead of .format()
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
st.markdown(f"""
    <style>
        .title-wrapper {{
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .last-updated {{
            font-size: 0.9rem;
            color: #666;
        }}
    </style>
    <div class="title-wrapper">
        <div>
            <h1>Forever Young Tours - Admin Dashboard</h1>
        </div>
        <div class="last-updated">
            Last updated: {current_time}
        </div>
    </div>
""", unsafe_allow_html=True)

# ---------------------------- ALERTS SECTION ----------------------------
with st.container():
    col1, col2, col3 = st.columns(3)
    
    # Unapproved users
    unapproved_users = df_users[df_users['approved_on'].isna()].shape[0]
    if unapproved_users > 0:
        with col1:
            st.warning(f"⚠️ {unapproved_users} users awaiting approval", icon="⚠️")
    
    # Pending payments
    pending_payments = df_payments[df_payments['status'] == 'Pending'].shape[0]
    if pending_payments > 0:
        with col2:
            st.warning(f"⚠️ {pending_payments} pending payments", icon="⚠️")
    
    # Inactive advisors - check if advisor_status column exists
    if 'advisor_status' in df_advisors.columns:
        inactive_advisors = df_advisors[df_advisors['advisor_status'] == 'Inactive'].shape[0]
    elif 'status' in df_advisors.columns:
        inactive_advisors = df_advisors[df_advisors['status'] == 'Inactive'].shape[0]
    else:
        inactive_advisors = 0
    
    if inactive_advisors > 0:
        with col3:
            st.warning(f"⚠️ {inactive_advisors} inactive advisors", icon="⚠️")

# ---------------------------- KPI METRICS ----------------------------
with st.container():
    st.subheader("📊 Key Performance Indicators")
    
    # Calculate metrics
    total_bookings = df_filtered['booking_id'].nunique()
    total_income = df_payments['amount'].sum()
    total_commissions = df_commissions['commission_amount'].sum()
    net_income = total_income - total_commissions
    avg_group_size = df_filtered['number_of_travelers'].mean()
    upcoming_trips = df_filtered[df_filtered['travel_date'] > datetime.now()].shape[0]
    
    # Display metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Bookings", f"{total_bookings:,}")
    col2.metric("Total Revenue", f"${total_income:,.0f}")
    col3.metric("Total Commissions", f"${total_commissions:,.0f}")
    col4.metric("Net Income", f"${net_income:,.0f}")
    col5.metric("Upcoming Trips", f"{upcoming_trips:,}")

# ---------------------------- REVENUE & PAYMENTS ----------------------------
with st.container():
    st.subheader("💰 Revenue & Payments Overview")
    
    # Revenue over time
    revenue_over_time = df_revenues.groupby('date')['net_income'].sum().reset_index()
    fig_revenue = px.line(
        revenue_over_time,
        x='date',
        y='net_income',
        title="Net Income Over Time",
        labels={'net_income': 'Net Income ($)', 'date': 'Date'},
        color_discrete_sequence=[COLOR_PRIMARY]
    )
    fig_revenue.update_layout(
        height=400,
        plot_bgcolor=COLOR_BACKGROUND,
        paper_bgcolor=COLOR_BACKGROUND
    )
    st.plotly_chart(fig_revenue, use_container_width=True)
    
    # Payment methods breakdown
    payment_methods = df_payments['method_used'].value_counts().reset_index()
    payment_methods.columns = ['method_used', 'count']
    fig_payments = px.bar(
        payment_methods,
        x='method_used',
        y='count',
        title="Payment Methods Distribution",
        labels={'method_used': 'Payment Method', 'count': 'Count'},
        color_discrete_sequence=[COLOR_SECONDARY]
    )
    fig_payments.update_layout(
        height=400,
        plot_bgcolor=COLOR_BACKGROUND,
        paper_bgcolor=COLOR_BACKGROUND
    )
    st.plotly_chart(fig_payments, use_container_width=True)

# ---------------------------- BOOKINGS ANALYSIS ----------------------------
with st.container():
    st.subheader("📦 Bookings Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Bookings by status
        booking_status_counts = df_filtered['status'].value_counts().reset_index()
        booking_status_counts.columns = ['status', 'count']
        fig_status = px.pie(
            booking_status_counts,
            values='count',
            names='status',
            title="Bookings by Status",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Bookings by month
        bookings_by_month = df_filtered.groupby(
            df_filtered['booking_date'].dt.to_period('M')
        ).size().reset_index()
        bookings_by_month.columns = ['month', 'count']
        bookings_by_month['month'] = bookings_by_month['month'].dt.to_timestamp()
        
        fig_monthly = px.bar(
            bookings_by_month,
            x='month',
            y='count',
            title="Bookings by Month",
            labels={'month': 'Month', 'count': 'Number of Bookings'},
            color_discrete_sequence=[COLOR_PRIMARY]
        )
        st.plotly_chart(fig_monthly, use_container_width=True)

# ---------------------------- AGENT PERFORMANCE ----------------------------
with st.container():
    st.subheader("🧑‍💼 Agent Performance")
    
    # Top advisors by bookings
    top_advisors = df_filtered.groupby(['advisorcode']).agg(
        bookings=('booking_id', 'count'),
        total_amount=('total_amount', 'sum'),
        avg_group_size=('number_of_travelers', 'mean')
    ).reset_index().sort_values('bookings', ascending=False).head(10)
    
    st.dataframe(
        top_advisors.style
        .format({'total_amount': '${:,.0f}', 'avg_group_size': '{:.1f}'})
        .background_gradient(cmap='Blues', subset=['bookings', 'total_amount']),
        use_container_width=True
    )
    
    # Advisor status - check which status column exists
    if 'advisor_status' in df_advisors.columns:
        advisor_status = df_advisors['advisor_status'].value_counts().reset_index()
        advisor_status.columns = ['status', 'count']
    elif 'status' in df_advisors.columns:
        advisor_status = df_advisors['status'].value_counts().reset_index()
        advisor_status.columns = ['status', 'count']
    else:
        # Create empty dataframe if no status column exists
        advisor_status = pd.DataFrame({'status': ['No Status Data'], 'count': [0]})
    
    fig_status = px.bar(
        advisor_status,
        x='status',
        y='count',
        title="Advisor Status Distribution",
        labels={'status': 'Advisor Status', 'count': 'Number of Advisors'},
        color_discrete_sequence=[COLOR_SECONDARY]
    )
    st.plotly_chart(fig_status, use_container_width=True)

# ---------------------------- CLIENT ANALYSIS ----------------------------
with st.container():
    st.subheader("👥 Client Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Client types
        client_types = df_clients['client_type'].value_counts().reset_index()
        client_types.columns = ['type', 'count']
        fig_client_types = px.pie(
            client_types,
            values='count',
            names='type',
            title="Client Types Distribution",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Blues_r
        )
        st.plotly_chart(fig_client_types, use_container_width=True)
    
    with col2:
        # Client countries
        client_countries = df_users['country'].value_counts().reset_index().head(10)
        client_countries.columns = ['country', 'count']
        fig_countries = px.bar(
            client_countries,
            x='country',
            y='count',
            title="Top 10 Client Countries",
            labels={'country': 'Country', 'count': 'Number of Clients'},
            color_discrete_sequence=[COLOR_PRIMARY]
        )
        st.plotly_chart(fig_countries, use_container_width=True)

# ---------------------------- TOUR DESTINATIONS ----------------------------
with st.container():
    st.subheader("🌍 Tour Destinations")
    
    # Top destinations
    top_destinations = df_filtered['destination'].value_counts().reset_index().head(10)
    top_destinations.columns = ['destination', 'bookings']
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.dataframe(
            top_destinations.style
            .background_gradient(cmap='Blues', subset=['bookings']),
            use_container_width=True
        )
    
    with col2:
        # Map visualization
        if 'latitude' in df_tours.columns and 'longitude' in df_tours.columns:
            map_data = df_tours.dropna(subset=['latitude', 'longitude'])[['latitude', 'longitude']]
            if not map_data.empty:
                st.map(map_data)

# ---------------------------- FOOTER ----------------------------
st.divider()
st.caption("© 2024 Forever Young Tours | Admin Dashboard v1.0")
