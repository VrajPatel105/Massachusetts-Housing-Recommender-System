import streamlit as st
import pandas as pd
import pickle
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

st.set_page_config(
    page_title='Property Recommendations',
    page_icon='🏠',
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
    .sub-header {
        font-size: 1.3rem;
        color: #666;
        text-align: center;
        margin-bottom: 3rem;
    }
    .input-section {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        margin: 2rem 0;
        color: white;
    }
    .recommendation-card {
        border: 1px solid #34495e;
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        transition: all 0.3s ease;
        color: white;
        position: relative;
        overflow: hidden;
    }
    .recommendation-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 16px rgba(0,0,0,0.4);
        background: linear-gradient(135deg, #34495e 0%, #2c3e50 100%);
    }
    .recommendation-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #3498db, #e74c3c, #f39c12, #27ae60);
    }
    .property-image {
        border-radius: 10px;
        width: 100%;
        max-width: 350px;
        height: 250px;
        object-fit: cover;
        transition: transform 0.3s ease;
    }
    .property-image:hover {
        transform: scale(1.05);
    }
    .similarity-badge {
        background: linear-gradient(45deg, #3498db, #2ecc71);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
        display: inline-block;
        margin-bottom: 1rem;
        font-size: 0.9rem;
    }
    .feature-tag {
        background-color: rgba(52, 152, 219, 0.2);
        color: #3498db;
        padding: 0.3rem 0.8rem;
        border-radius: 15px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
        border: 1px solid #3498db;
    }
    .stButton > button {
        background: linear-gradient(135deg, #3498db 0%, #2ecc71 100%);
        color: white;
        border: none;
        padding: 1rem 3rem;
        border-radius: 25px;
        font-weight: bold;
        font-size: 1.1rem;
        transition: all 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(0,0,0,0.3);
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def load_recommender_components():
    """Load the recommendation model components"""
    try:
        with open('property_recommender_components.pkl', 'rb') as f:
            components = pickle.load(f)
        return components
    except Exception as e:
        st.error(f"Error loading recommender components: {str(e)}")
        return None

@st.cache_data
def load_property_data():
    """Load the complete property dataset"""
    try:
        data = pd.read_csv('final_data.csv')
        # Clean price column if needed
        if 'price' in data.columns and data['price'].dtype == 'object':
            data['price'] = data['price'].astype(str).str.replace('$', '').str.replace(',', '')
            data['price'] = pd.to_numeric(data['price'], errors='coerce')
        return data
    except Exception as e:
        st.error(f"Error loading property data: {str(e)}")
        return pd.DataFrame()

def recommend_properties_with_features(desired_features, components, property_data, top_n=10):
    """
    Recommend properties based on desired features using the loaded model components
    """
    try:
        # Extract components
        tfidf_vectorizer = components['tfidf_vectorizer']
        tfidf_matrix = components['tfidf_matrix']
        tfidf_vectorizer_interior = components['tfidf_vectorizer_interior']
        tfidf_matrix_interior = components['tfidf_matrix_interior']
        tfidf_vectorizer_cities = components['tfidf_vectorizer_cities']
        tfidf_matrix_cities = components['tfidf_matrix_cities']
        
        # Set weights
        weight_utilities = 30
        weight_interior = 20
        weight_cities = 8
        
        # Vectorize the input string for each category
        input_vector_utilities = tfidf_vectorizer.transform([desired_features])
        input_vector_interior = tfidf_vectorizer_interior.transform([desired_features])
        input_vector_cities = tfidf_vectorizer_cities.transform([desired_features])
        
        # Calculate similarity scores for each category
        sim_scores_utilities = cosine_similarity(input_vector_utilities, tfidf_matrix).flatten()
        sim_scores_interior = cosine_similarity(input_vector_interior, tfidf_matrix_interior).flatten()
        sim_scores_cities = cosine_similarity(input_vector_cities, tfidf_matrix_cities).flatten()
        
        # Combine the similarity scores with the specified weights
        combined_sim_scores = (weight_utilities * sim_scores_utilities +
                             weight_interior * sim_scores_interior +
                             weight_cities * sim_scores_cities)
        
        # Get the indices of the top_n most similar properties
        top_indices = combined_sim_scores.argsort()[-top_n:][::-1]
        top_scores = combined_sim_scores[top_indices]
        
        # Get property details from the main dataset
        recommended_properties = property_data.iloc[top_indices].copy()
        recommended_properties['similarity_score'] = top_scores
        
        return recommended_properties.reset_index(drop=True)
        
    except Exception as e:
        st.error(f"Error generating recommendations: {str(e)}")
        return pd.DataFrame()

def display_recommendation_cards(recommendations):
    """Display property recommendations as attractive cards"""
    if recommendations.empty:
        st.warning("No recommendations found. Please try different search criteria.")
        return
    
    st.markdown(f"### 🎯 Top {len(recommendations)} Recommended Properties")
    st.markdown("*Click on images or property titles to view full listings*")
    
    for idx, property_data in recommendations.iterrows():
        with st.container():
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Property image
                if pd.notna(property_data['image_url']) and property_data['image_url']:
                    st.markdown(f"""
                    <div style="text-align: center;">
                        <a href="{property_data['url']}" target="_blank">
                            <img src="{property_data['image_url']}" class="property-image" alt="Property Image">
                        </a>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div style="width:350px;height:250px;background: linear-gradient(45deg, #f0f0f0, #ddd);display:flex;align-items:center;justify-content:center;border-radius:10px;margin:auto;">
                        <span style="color:#666;font-size:1.1rem;">📷 No Image Available</span>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col2:
                similarity_percentage = (property_data['similarity_score'] / recommendations['similarity_score'].max()) * 100
                
                # Create a styled container using Streamlit components
                with st.container():
                    # Similarity badge
                    st.markdown(f"""
                    <div style="background: linear-gradient(45deg, #3498db, #2ecc71); color: white; padding: 0.5rem 1rem; border-radius: 20px; display: inline-block; margin-bottom: 1rem;">
                        🎯 {similarity_percentage:.1f}% Match
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Property title and price
                    st.markdown(f"""
                    <h3 style="margin:0 0 1rem 0;color:#3498db;">
                        <a href="{property_data['url']}" target="_blank" style="text-decoration:none;color:#3498db;">
                            ${property_data['price']:,.0f} - {property_data['address']}
                        </a>
                    </h3>
                    """, unsafe_allow_html=True)
                    
                    # Property details using columns
                    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
                    with detail_col1:
                        st.metric("🛏️ Beds", f"{property_data['beds']}")
                    with detail_col2:
                        st.metric("🛁 Baths", f"{property_data['baths']}")
                    with detail_col3:
                        st.metric("📐 Sqft", f"{property_data['sqft']:,.0f}")
                    with detail_col4:
                        st.metric("🚗 Parking", f"{property_data['parking_total_spaces']}")
                    
                    # Additional details
                    st.markdown(f"**🏘️ Property Type:** {property_data['property_type'].title()}")
                    st.markdown(f"**📍 Location:** {property_data['region']}, {property_data['county']}")
                    st.markdown(f"**🚶 Walk Score:** {property_data['walk_score']}/100")
                    st.markdown(f"**💰 Price/Sqft:** ${property_data['price']/property_data['sqft']:.0f}")
                    
                    # Tags and scores
                    tag_col1, tag_col2, tag_col3 = st.columns(3)
                    with tag_col1:
                        st.markdown(f"🎯 **Score:** {property_data['similarity_score']:.2f}")
                    with tag_col2:
                        st.markdown(f"📮 **ZIP:** {int(property_data['zip_code'])}")
                    with tag_col3:
                        if property_data['has_garage']:
                            st.markdown("🏠 **Has Garage**")
                    
                    # Add some styling to the container
                    st.markdown("""
                    <style>
                    .stContainer > div {
                        background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
                        padding: 1.5rem;
                        border-radius: 15px;
                        margin: 1rem 0;
                        color: white;
                        border: 1px solid #34495e;
                    }
                    </style>
                    """, unsafe_allow_html=True)
            
            st.markdown("---")

# Main App
st.markdown('<h1 class="main-header">🏠 Massachusetts Property Recommendation System  ~ Vraj</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Find your perfect home based on your preferences</p>', unsafe_allow_html=True)

# Load components and data
components = load_recommender_components()
property_data = load_property_data()

if components is None or property_data.empty:
    st.error("Unable to load recommendation system. Please ensure all required files are available.")
    st.stop()

# Input form
st.markdown('<div class="input-section">', unsafe_allow_html=True)
st.markdown("### 🔍 What are you looking for in your ideal home?")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🏠 Interior Features")
    interior_features = st.multiselect(
        "Select desired interior features:",
        [
            "fireplace", "hardwood floors", "granite countertops", "stainless steel", 
            "marble", "walk-in closet", "updated kitchen", "renovated", "crown molding",
            "high ceilings", "wood flooring", "tile flooring", "carpet", "laminate"
        ],
        help="Choose interior features you'd like in your property"
    )
    
    st.markdown("#### 🔧 Utilities & Appliances")
    utilities_appliances = st.multiselect(
        "Select preferred utilities and appliances:",
        [
            "washer", "dryer", "dishwasher", "range", "oven", "microwave",
            "refrigerator", "central air", "heating", "public water", "public sewer",
            "gas", "electric", "solar panels", "smart home"
        ],
        help="Choose utilities and appliances you want"
    )

with col2:
    st.markdown("#### 📍 Nearby Cities/Areas")
    nearby_cities = st.multiselect(
        "Preferred nearby cities or areas:",
        [
            "Boston", "Cambridge", "Newton", "Brookline", "Somerville", "Arlington",
            "Medford", "Quincy", "Lynn", "Salem", "Lowell", "Worcester", "Springfield",
            "Plymouth", "Cape Cod", "Framingham", "Waltham", "Lexington", "Needham"
        ],
        help="Select cities or areas you'd like to be near"
    )
    
    st.markdown("#### 🏘️ Additional Preferences")
    additional_prefs = st.multiselect(
        "Any additional preferences:",
        [
            "detached", "attached", "private", "pool", "deck", "patio", "garden",
            "basement", "attic", "garage", "parking", "quiet neighborhood", "walkable",
            "near schools", "near shopping", "near public transport"
        ],
        help="Any other features or location preferences"
    )

# Number of recommendations
num_recommendations = st.slider("Number of recommendations:", 5, 20, 10)

st.markdown('</div>', unsafe_allow_html=True)

# Generate recommendations button
if st.button("🔮 Find My Perfect Properties", type="primary"):
    # Combine all selected features into a single string
    all_features = []
    all_features.extend(interior_features)
    all_features.extend(utilities_appliances)
    all_features.extend(nearby_cities)
    all_features.extend(additional_prefs)
    
    if not all_features:
        st.warning("⚠️ Please select at least some preferences to get personalized recommendations.")
    else:
        # Create feature string for the model
        feature_string = "; ".join(all_features)
        
        # Show what we're searching for
        st.markdown("### 🔍 Searching for properties with:")
        st.markdown(f"*{feature_string}*")
        
        # Generate recommendations
        with st.spinner("🔍 Finding the perfect properties for you..."):
            recommendations = recommend_properties_with_features(
                feature_string, components, property_data, num_recommendations
            )
            
            if not recommendations.empty:
                # Display recommendations
                display_recommendation_cards(recommendations)
                
                # Show search statistics
                st.markdown("### 📊 Search Statistics")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    avg_price = recommendations['price'].mean()
                    st.metric("Average Price", f"${avg_price:,.0f}")
                
                with col2:
                    price_range = recommendations['price'].max() - recommendations['price'].min()
                    st.metric("Price Range", f"${price_range:,.0f}")
                
                with col3:
                    avg_similarity = recommendations['similarity_score'].mean()
                    st.metric("Avg Match Score", f"{avg_similarity:.2f}")
                
                with col4:
                    unique_regions = recommendations['region'].nunique()
                    st.metric("Regions Found", unique_regions)
            else:
                st.error("No properties found matching your criteria. Try adjusting your preferences.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; margin-top: 2rem;'>
    <p><em>🏠 Intelligent Property Recommendations • Powered by Machine Learning</em></p>
    <p>Our recommendation system analyzes property features, utilities, and location preferences to find your perfect match</p>
</div>
""", unsafe_allow_html=True)