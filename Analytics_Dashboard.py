import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

st.set_page_config(
    page_title='Housing Analytics Dashboard',
    page_icon='📊',
    layout='wide'
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .metric-card {
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        color: white;
    }
    .filter-section {
        background-color: #2c3e50;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
    }
    .property-card {
        border: 1px solid #34495e;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
        background-color: #2c3e50;
        box-shadow: 0 2px 4px rgba(0,0,0,0.3);
        transition: transform 0.2s;
        color: white;
    }
    .property-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.4);
        background-color: #34495e;
    }
    .property-image {
        border-radius: 8px;
        width: 100%;
        max-width: 300px;
        height: 200px;
        object-fit: cover;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    try:
        data = pd.read_csv('final_data.csv')
        # Clean price column - remove $ and commas, convert to numeric
        if 'price' in data.columns:
            if data['price'].dtype == 'object':
                data['price'] = data['price'].astype(str).str.replace('$', '').str.replace(',', '')
                data['price'] = pd.to_numeric(data['price'], errors='coerce')
        return data
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def create_price_map(data):
    """Create an interactive map showing property prices by location"""
    if data.empty:
        return None
    
    # Create a price map using zip codes as proxies for coordinates
    # In a real application, you'd geocode the addresses
    zip_price_avg = data.groupby(['zip_code', 'region', 'county']).agg({
        'price': ['mean', 'count'],
        'sqft': 'mean'
    }).reset_index()
    
    zip_price_avg.columns = ['zip_code', 'region', 'county', 'avg_price', 'property_count', 'avg_sqft']
    zip_price_avg = zip_price_avg[zip_price_avg['property_count'] >= 3]  # Filter for statistical significance
    
    fig = px.scatter(
        zip_price_avg,
        x='zip_code',
        y='avg_price',
        size='property_count',
        color='county',
        hover_data=['region', 'property_count', 'avg_sqft'],
        title='Average Property Prices by ZIP Code',
        labels={
            'avg_price': 'Average Price ($)',
            'zip_code': 'ZIP Code',
            'property_count': 'Number of Properties'
        }
    )
    
    fig.update_layout(height=500)
    return fig

def create_price_distribution_chart(data):
    """Create price distribution charts"""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Price Distribution', 'Price by Region', 'Price by Property Type', 'Price vs Square Feet'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Price histogram
    fig.add_trace(
        go.Histogram(x=data['price'], nbinsx=30, name='Price Distribution', showlegend=False),
        row=1, col=1
    )
    
    # Price by region
    region_avg = data.groupby('region')['price'].mean().sort_values(ascending=True)
    fig.add_trace(
        go.Bar(x=region_avg.values, y=region_avg.index, orientation='h', name='Avg Price', showlegend=False),
        row=1, col=2
    )
    
    # Price by property type
    prop_type_avg = data.groupby('property_type')['price'].mean().sort_values(ascending=False)
    fig.add_trace(
        go.Bar(x=prop_type_avg.index, y=prop_type_avg.values, name='Avg Price', showlegend=False),
        row=2, col=1
    )
    
    # Price vs sqft scatter
    fig.add_trace(
        go.Scatter(
            x=data['sqft'], 
            y=data['price'], 
            mode='markers',
            marker=dict(color=data['price'], colorscale='viridis', size=5),
            name='Properties',
            showlegend=False
        ),
        row=2, col=2
    )
    
    fig.update_layout(height=800, showlegend=False)
    return fig

def create_market_insights_charts(data):
    """Create additional market insight charts"""
    col1, col2 = st.columns(2)
    
    with col1:
        # Walk Score vs Price
        fig1 = px.scatter(
            data, 
            x='walk_score', 
            y='price',
            color='county',
            title='Walk Score vs Property Price',
            trendline="ols"
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Property Size Distribution
        fig2 = px.box(
            data, 
            x='property_type', 
            y='sqft',
            title='Property Size Distribution by Type'
        )
        fig2.update_layout(xaxis_tickangle=45)
        st.plotly_chart(fig2, use_container_width=True)

def display_property_cards(filtered_data, max_properties=20):
    """Display property cards with images and clickable links"""
    if filtered_data.empty:
        st.warning("No properties found matching the selected filters.")
        return
    
    # Limit the number of properties displayed for performance
    display_data = filtered_data.head(max_properties)
    
    st.markdown(f"### 🏠 Properties ({len(filtered_data)} total, showing {len(display_data)})")
    
    for idx, row in display_data.iterrows():
        with st.container():
            col1, col2 = st.columns([1, 2])
            
            with col1:
                if pd.notna(row['image_url']) and row['image_url']:
                    st.markdown(f"""
                    <div class="property-card">
                        <a href="{row['url']}" target="_blank">
                            <img src="{row['image_url']}" class="property-image" alt="Property Image">
                        </a>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="property-card">
                        <div style="width:300px;height:200px;background-color:#f0f0f0;display:flex;align-items:center;justify-content:center;border-radius:8px;">
                            <span style="color:#666;">No Image Available</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="property-card">
                    <h4 style="margin-top:0;color:#3498db;">
                        <a href="{row['url']}" target="_blank" style="text-decoration:none;color:#3498db;">
                            ${row['price']:,.0f} - {row['address']}
                        </a>
                    </h4>
                    <p style="color:white;"><strong>🛏️ Beds:</strong> {row['beds']} | <strong>🛁 Baths:</strong> {row['baths']} | <strong>📐 Sqft:</strong> {row['sqft']:,.0f}</p>
                    <p style="color:white;"><strong>🏘️ Type:</strong> {row['property_type'].title()} | <strong>📍 Region:</strong> {row['region']}</p>
                    <p style="color:white;"><strong>🚗 Parking:</strong> {row['parking_total_spaces']} spaces | <strong>🚶 Walk Score:</strong> {row['walk_score']}/100</p>
                    <p style="color:white;"><strong>💰 Price/Sqft:</strong> ${row['price']/row['sqft']:.0f}</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")

# Main App
st.markdown('<h1 class="main-header">📊 Massachusetts Housing Analytics Dashboard</h1>', unsafe_allow_html=True)

# Load data
data = load_data()

if data.empty:
    st.error("No data available. Please ensure the final_data.csv file exists and is properly formatted.")
    st.stop()

# Sidebar filters
st.sidebar.markdown("## 🔍 Filters")

# Clear filters button
if st.sidebar.button("🔄 Clear All Filters", type="secondary"):
    st.session_state.clear()
    st.rerun()

# Region filter
regions = ['All'] + sorted(data['region'].unique().tolist())
selected_region = st.sidebar.selectbox("Select Region", regions, key="region_filter")

# County filter  
counties = ['All'] + sorted(data['county'].unique().tolist())
selected_county = st.sidebar.selectbox("Select County", counties, key="county_filter")

# Price range filter
min_price, max_price = int(data['price'].min()), int(data['price'].max())
price_range = st.sidebar.slider(
    "Price Range", 
    min_price, 
    max_price, 
    (min_price, max_price),
    format="$%d",
    key="price_range_filter"
)

# Property type filter
property_types = ['All'] + sorted(data['property_type'].unique().tolist())
selected_property_type = st.sidebar.selectbox("Property Type", property_types, key="property_type_filter")

# Bedrooms filter
bedrooms = ['All'] + sorted(data['beds'].unique().tolist())
selected_bedrooms = st.sidebar.selectbox("Bedrooms", bedrooms, key="bedrooms_filter")

# Apply filters
filtered_data = data.copy()

if selected_region != 'All':
    filtered_data = filtered_data[filtered_data['region'] == selected_region]

if selected_county != 'All':
    filtered_data = filtered_data[filtered_data['county'] == selected_county]

filtered_data = filtered_data[
    (filtered_data['price'] >= price_range[0]) & 
    (filtered_data['price'] <= price_range[1])
]

if selected_property_type != 'All':
    filtered_data = filtered_data[filtered_data['property_type'] == selected_property_type]

if selected_bedrooms != 'All':
    filtered_data = filtered_data[filtered_data['beds'] == selected_bedrooms]

# Display summary metrics
st.markdown("## 📈 Market Overview")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="margin:0;color:#ecf0f1;">Total Properties</h3>
        <h2 style="margin:10px 0 0 0;color:white;">{len(filtered_data):,}</h2>
    </div>
    """, unsafe_allow_html=True)

with col2:
    avg_price = filtered_data['price'].mean()
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="margin:0;color:#ecf0f1;">Average Price</h3>
        <h2 style="margin:10px 0 0 0;color:white;">${avg_price:,.0f}</h2>
    </div>
    """, unsafe_allow_html=True)

with col3:
    median_price = filtered_data['price'].median()
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="margin:0;color:#ecf0f1;">Median Price</h3>
        <h2 style="margin:10px 0 0 0;color:white;">${median_price:,.0f}</h2>
    </div>
    """, unsafe_allow_html=True)

with col4:
    avg_price_per_sqft = (filtered_data['price'] / filtered_data['sqft']).mean()
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="margin:0;color:#ecf0f1;">Avg $/Sqft</h3>
        <h2 style="margin:10px 0 0 0;color:white;">${avg_price_per_sqft:.0f}</h2>
    </div>
    """, unsafe_allow_html=True)

# Create tabs for different visualizations
tab1, tab2, tab3 = st.tabs(["🗺️ Geographic Analysis", "📊 Market Insights", "🏠 Property Listings"])

with tab1:
    st.markdown("## 🗺️ Geographic Price Analysis")
    
    # Price map
    price_map = create_price_map(filtered_data)
    if price_map:
        st.plotly_chart(price_map, use_container_width=True)
    
    # Regional comparison
    st.markdown("### 📍 Regional Price Comparison")
    if len(filtered_data) > 0:
        regional_stats = filtered_data.groupby(['region', 'county']).agg({
            'price': ['mean', 'median', 'count'],
            'sqft': 'mean',
            'walk_score': 'mean'
        }).round(0)
        
        regional_stats.columns = ['Avg Price', 'Median Price', 'Properties', 'Avg Sqft', 'Avg Walk Score']
        st.dataframe(regional_stats, use_container_width=True)

with tab2:
    st.markdown("## 📊 Market Insights & Analytics")
    
    # Price distribution charts
    if len(filtered_data) > 0:
        price_dist_chart = create_price_distribution_chart(filtered_data)
        st.plotly_chart(price_dist_chart, use_container_width=True)
        
        # Additional insights
        create_market_insights_charts(filtered_data)
        
        # Correlation matrix
        st.markdown("### 🔗 Feature Correlations")
        numeric_cols = ['price', 'beds', 'baths', 'sqft', 'parking_total_spaces', 'walk_score', 'wind_risk']
        correlation_data = filtered_data[numeric_cols].corr()
        
        fig_corr = px.imshow(
            correlation_data, 
            text_auto=True,
            aspect="auto",
            title="Feature Correlation Matrix",
            color_continuous_scale="RdBu"
        )
        st.plotly_chart(fig_corr, use_container_width=True)

with tab3:
    st.markdown("## 🏠 Property Listings")
    
    # Sort options
    sort_options = {
        'Price: Low to High': ('price', True),
        'Price: High to Low': ('price', False),
        'Size: Large to Small': ('sqft', False),
        'Walk Score: High to Low': ('walk_score', False)
    }
    
    selected_sort = st.selectbox("Sort by:", list(sort_options.keys()))
    sort_column, ascending = sort_options[selected_sort]
    
    # Sort the data
    sorted_data = filtered_data.sort_values(by=sort_column, ascending=ascending)
    
    # Display properties
    display_property_cards(sorted_data)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; margin-top: 2rem;'>
    <p><em>Massachusetts Housing Analytics Dashboard • Real Estate Market Insights</em></p>
    <p>Click on property images or titles to view full listings on Zillow</p>
</div>
""", unsafe_allow_html=True)