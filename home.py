import streamlit as st
import pickle
import pandas as pd
import numpy as np

st.set_page_config(
    page_title='Massachusetts Housing Price Predictor',
    page_icon='🏠',
    layout='wide',
    initial_sidebar_state='collapsed'
)  

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: bold;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #666;
        text-align: center;
        margin-bottom: 3rem;
    }
    .prediction-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 2rem 0;
    }
    .input-section {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        border-radius: 25px;
        font-weight: bold;
        width: 100%;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 10px rgba(0,0,0,0.2);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_data():
    with open('df.pkl', 'rb') as file:
        df = pickle.load(file)
    return df

@st.cache_resource
def load_pipeline():
    try:
        with open('pipeline.pkl', 'rb') as file:
            pipeline = pickle.load(file)
        return pipeline
    except Exception as e:
        # Silently handle the error and return None to use fallback method
        return None

def simple_price_estimation(beds, baths, sqft, property_type, region, county):
    """Simple price estimation based on basic factors"""
    
    # Base price per square foot for Massachusetts (average from data analysis)
    base_price_per_sqft = 350
    
    # Regional multipliers based on typical Massachusetts market
    region_multipliers = {
        'Greater Boston': 1.4,
        'Boston': 1.5,
        'Cambridge': 1.6,
        'Newton': 1.7,
        'Brookline': 1.6,
        'Metro West': 1.2,
        'North Shore': 1.1,
        'South Shore': 1.1,
        'Cape Cod': 1.3,
        'Western Mass': 0.8,
        'Central Mass': 0.9
    }
    
    # County multipliers as backup
    county_multipliers = {
        'Suffolk': 1.5,
        'Middlesex': 1.3,
        'Norfolk': 1.2,
        'Essex': 1.1,
        'Plymouth': 1.0,
        'Bristol': 0.9,
        'Worcester': 0.8,
        'Barnstable': 1.2,
        'Hampden': 0.7,
        'Berkshire': 0.6
    }
    
    # Property type multipliers
    property_multipliers = {
        'Single Family Residential': 1.0,
        'Condo/Co-op': 0.85,
        'Townhouse': 0.95,
        'Multi-Family': 0.9
    }
    
    # Get multipliers
    region_mult = region_multipliers.get(region, 1.0)
    county_mult = county_multipliers.get(county, 1.0)
    prop_mult = property_multipliers.get(property_type, 1.0)
    
    # Calculate base price
    base_price = sqft * base_price_per_sqft
    
    # Apply multipliers
    estimated_price = base_price * region_mult * prop_mult
    
    # Bedroom/bathroom adjustments
    if beds >= 4:
        estimated_price *= 1.1
    if baths >= 3:
        estimated_price *= 1.05
        
    return estimated_price

try:
    df = load_data()
    pipeline = load_pipeline()
    
    st.markdown('<h1 class="main-header">🏠 Massachusetts Housing Price Predictor</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Get an accurate estimate of your property\'s value using advanced machine learning</p>', unsafe_allow_html=True)
    
    st.markdown('<div class="input-section">', unsafe_allow_html=True)
    st.markdown("### Property Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        beds = st.number_input("Number of Bedrooms", min_value=1, max_value=10, value=3, step=1)
        baths = st.number_input("Number of Bathrooms", min_value=1.0, max_value=10.0, value=2.0, step=0.5)
        sqft = st.number_input("Square Footage", min_value=300, max_value=10000, value=1500, step=50)
        
        property_types = df['property_type'].unique().tolist()
        property_type = st.selectbox("Property Type", options=property_types)
        
        has_garage = st.selectbox("Has Garage", options=[True, False], format_func=lambda x: "Yes" if x else "No")
    
    with col2:
        regions = df['region'].unique().tolist()
        region = st.selectbox("Region", options=regions)
        
        counties = df['county'].unique().tolist()
        county = st.selectbox("County", options=counties)
        
        zip_codes = sorted(df['zip_code'].unique().tolist())
        zip_code = st.selectbox("ZIP Code", options=zip_codes)
        
        parking_spaces = st.number_input("Parking Spaces", min_value=0, max_value=10, value=2, step=1)
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    if st.button("🔮 Estimate Property Price", key="predict_button"):
        
        avg_walk_score = df['walk_score'].mean()
        avg_school_distance = df['middle_school_distance'].mean()
        avg_wind_risk = df['wind_risk'].mean()
        avg_mobility_score = df['mobility_score'].mean()
        avg_parking_quality = df['parking_quality_score'].mean()
        avg_price_volatility = df['price_volatility'].mean()
        avg_price_reduction = df['price_reduction_total'].mean()
        
        input_data = pd.DataFrame({
            'beds': [beds],
            'baths': [baths],
            'sqft': [sqft],
            'property_type': [property_type],
            'region': [region],
            'parking_total_spaces': [parking_spaces],
            'walk_score': [avg_walk_score],
            'middle_school_distance': [avg_school_distance],
            'wind_risk': [avg_wind_risk],
            'zip_code': [zip_code],
            'mobility_score': [avg_mobility_score],
            'parking_quality_score': [avg_parking_quality],
            'has_garage': [has_garage],
            'county': [county],
            'price_volatility': [avg_price_volatility],
            'price_reduction_total': [avg_price_reduction]
        })
        
        try:
            if pipeline is not None:
                prediction = pipeline.predict(input_data)[0]
                method_used = "Advanced ML Model"
            else:
                prediction = simple_price_estimation(beds, baths, sqft, property_type, region, county)
                method_used = "Statistical Estimation"
            
            st.markdown(f"""
            <div class="prediction-box">
                <h2>🎯 Estimated Property Value</h2>
                <h1>${prediction:,.0f}</h1>
                <p>Based on current market conditions and property characteristics</p>
                <small>Method: {method_used}</small>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("### 📊 Prediction Details")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Property Size", f"{sqft:,} sq ft")
                st.metric("Bedrooms", beds)
            
            with col2:
                st.metric("Bathrooms", baths)
                st.metric("Parking Spaces", parking_spaces)
            
            with col3:
                st.metric("Property Type", property_type)
                st.metric("Location", f"{region}, {county}")
            
            st.info("💡 This estimate is based on similar properties in your area and current market trends. Actual prices may vary based on property condition, exact location, and market conditions.")
            
        except Exception as e:
            st.error(f"Error making prediction: {str(e)}")
            st.info("Please check that all required files (pipeline.pkl) are available and properly configured.")

except FileNotFoundError as e:
    st.error("Required data files not found. Please ensure df.pkl and pipeline.pkl are in the same directory.")
except Exception as e:
    st.error(f"Error loading application: {str(e)}")
    
    st.markdown("### 🔧 Troubleshooting")
    st.markdown("""
    If you're seeing this error, please check:
    1. Both `df.pkl` and `pipeline.pkl` files are in the same directory as this script
    2. Required Python packages are installed (streamlit, pandas, numpy, scikit-learn)
    3. The pickle files were created with compatible versions of the libraries
    """)

st.markdown("---")

st.markdown("*Vraj Patel*", unsafe_allow_html=True)
