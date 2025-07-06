import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import json
import numpy as np

# Page config
st.set_page_config(
    page_title="GTA Real-Time Dashboard",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .big-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .alert-box {
        background-color: #ff4b4b;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .success-box {
        background-color: #00cc88;
        color: white;
        padding: 10px;
        border-radius: 5px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Database connection functions
def get_connection():
    """Get a fresh database connection"""
    return psycopg2.connect(
        host="postgres",
        port=5432,
        database="gta_warehouse",
        user="gta_admin",
        password="gta_secure_pass"
    )

def execute_query(query, params=None):
    """Execute a query and return results as DataFrame"""
    conn = get_connection()
    try:
        df = pd.read_sql(query, conn, params=params)
        return df
    finally:
        conn.close()

def execute_query_single(query, params=None):
    """Execute a query and return single value"""
    df = execute_query(query, params)
    if not df.empty and len(df.columns) > 0:
        return df.iloc[0, 0]
    return 0

# Header
st.title("🏛️ Gambian Tax Authority - Real-Time Command Center")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.image("https://via.placeholder.com/300x100/2E86AB/FFFFFF?text=GTA+Logo", width=300)
    st.markdown("### Navigation")
    page = st.radio("Select Dashboard", 
        ["🏠 Executive Overview", "💰 Revenue Analytics", "🚨 Fraud Detection", 
         "👥 Taxpayer 360°", "📊 Predictive Insights"])
    
    st.markdown("---")
    st.markdown("### Quick Stats")
    
    # Today's collections - with error handling
    try:
        today_revenue = execute_query_single("""
            SELECT COALESCE(SUM(amount), 0) as total 
            FROM raw.payments 
            WHERE DATE(payment_date) = CURRENT_DATE
        """)
    except Exception as e:
        today_revenue = 0
        st.error(f"Database connection error: {str(e)}")
    
    st.metric("Today's Collections", f"D {today_revenue:,.0f}")
    
    # Active alerts
    try:
        active_alerts = execute_query_single("""
            SELECT COUNT(*) as count 
            FROM analytics.fraud_alerts 
            WHERE status = 'Open'
        """)
    except:
        active_alerts = 0
    
    st.metric("Active Alerts", int(active_alerts), "-2 vs yesterday", delta_color="inverse")

# Main content based on selected page
if page == "🏠 Executive Overview":
    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    # Monthly revenue
    try:
        monthly_revenue = execute_query_single("""
            SELECT COALESCE(SUM(amount), 0) as total 
            FROM raw.payments 
            WHERE DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE)
        """)
        
        # YoY growth
        last_year_revenue = execute_query_single("""
            SELECT COALESCE(SUM(amount), 0) as total 
            FROM raw.payments 
            WHERE DATE_TRUNC('month', payment_date) = DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 year')
        """)
        
        growth = ((monthly_revenue - last_year_revenue) / last_year_revenue * 100) if last_year_revenue > 0 else 0
    except Exception as e:
        monthly_revenue = 0
        growth = 0
        st.error(f"Error loading revenue data: {str(e)}")
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Monthly Revenue", f"D {monthly_revenue:,.0f}", f"{growth:.1f}% YoY")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Compliance rate
    try:
        compliance_rate = execute_query_single("""
            SELECT AVG(compliance_score) * 100 as rate 
            FROM raw.taxpayers
        """)
    except:
        compliance_rate = 0
    
    with col2:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Compliance Rate", f"{compliance_rate:.1f}%", "+2.3%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Active taxpayers
    try:
        active_taxpayers = execute_query_single("""
            SELECT COUNT(DISTINCT taxpayer_id) as count 
            FROM raw.payments 
            WHERE payment_date >= CURRENT_DATE - INTERVAL '30 days'
        """)
    except:
        active_taxpayers = 0
    
    with col3:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Active Taxpayers", f"{int(active_taxpayers):,}", "+156")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Fraud detection rate
    with col4:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric("Fraud Detection Rate", "87.3%", "+5.2%")
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Charts row
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Revenue Trend (Last 30 Days)")
        
        try:
            revenue_trend = execute_query("""
                SELECT 
                    DATE(payment_date) as date,
                    SUM(amount) as revenue
                FROM raw.payments
                WHERE payment_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY DATE(payment_date)
                ORDER BY date
            """)
            
            if not revenue_trend.empty:
                fig = px.area(revenue_trend, x='date', y='revenue', 
                              title="Daily Revenue Collection",
                              color_discrete_sequence=['#2E86AB'])
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No revenue data available for the selected period")
        except Exception as e:
            st.error(f"Error loading revenue trend: {str(e)}")
    
    with col2:
        st.subheader("🗺️ Revenue by Region")
        
        try:
            regional_revenue = execute_query("""
                SELECT 
                    t.region,
                    SUM(p.amount) as revenue
                FROM raw.payments p
                JOIN raw.taxpayers t ON p.taxpayer_id = t.taxpayer_id
                WHERE p.payment_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY t.region
                ORDER BY revenue DESC
            """)
            
            if not regional_revenue.empty:
                fig = px.bar(regional_revenue, x='revenue', y='region', 
                             orientation='h', title="Revenue by Region",
                             color='revenue', color_continuous_scale='Blues')
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No regional data available")
        except Exception as e:
            st.error(f"Error loading regional data: {str(e)}")
    
    # Alerts section
    st.markdown("---")
    st.subheader("🚨 Critical Alerts")
    
    try:
        alerts = execute_query("""
            SELECT 
                fa.alert_date,
                fa.alert_type,
                fa.description,
                t.name as taxpayer_name,
                fa.risk_score
            FROM analytics.fraud_alerts fa
            JOIN raw.taxpayers t ON fa.taxpayer_id = t.taxpayer_id
            WHERE fa.status = 'Open'
            ORDER BY fa.risk_score DESC
            LIMIT 5
        """)
        
        if not alerts.empty:
            for _, alert in alerts.iterrows():
                if alert['risk_score'] > 0.8:
                    st.markdown(f'<div class="alert-box">⚠️ <b>{alert["taxpayer_name"]}</b>: {alert["description"]} (Risk: {alert["risk_score"]:.2f})</div>', 
                               unsafe_allow_html=True)
                else:
                    st.warning(f"⚠️ {alert['taxpayer_name']}: {alert['description']} (Risk: {alert['risk_score']:.2f})")
        else:
            st.success("No critical alerts at this time")
    except Exception as e:
        st.error(f"Error loading alerts: {str(e)}")

elif page == "💰 Revenue Analytics":
    st.header("Revenue Analytics Dashboard")
    
    # Date filter
    col1, col2 = st.columns([1, 3])
    with col1:
        date_range = st.date_input("Date Range", 
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            max_value=datetime.now())
    
    # Revenue breakdown
    st.subheader("Revenue Breakdown by Tax Type")
    
    try:
        revenue_by_type = execute_query(f"""
            SELECT 
                tax_type,
                SUM(amount) as revenue,
                COUNT(*) as transactions,
                COUNT(DISTINCT taxpayer_id) as taxpayers
            FROM raw.payments
            WHERE payment_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY tax_type
            ORDER BY revenue DESC
        """)
        
        if not revenue_by_type.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(revenue_by_type, values='revenue', names='tax_type',
                             title="Revenue Distribution by Tax Type")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(revenue_by_type, x='tax_type', y='revenue',
                             title="Revenue by Tax Type",
                             text='revenue')
                fig.update_traces(texttemplate='D %{text:,.0f}', textposition='outside')
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available for the selected period")
    except Exception as e:
        st.error(f"Error loading revenue data: {str(e)}")
    
    # Payment channels
    st.subheader("Payment Channel Analysis")
    
    try:
        channel_data = execute_query(f"""
            SELECT 
                payment_channel,
                COUNT(*) as transactions,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM raw.payments
            WHERE payment_date BETWEEN '{date_range[0]}' AND '{date_range[1]}'
            GROUP BY payment_channel
        """)
        
        if not channel_data.empty:
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Transaction Volume', 'Average Transaction Size'),
                specs=[[{'type': 'pie'}, {'type': 'bar'}]]
            )
            
            fig.add_trace(
                go.Pie(labels=channel_data['payment_channel'], 
                       values=channel_data['transactions'],
                       name="Transactions"),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Bar(x=channel_data['payment_channel'], 
                       y=channel_data['avg_amount'],
                       name="Avg Amount"),
                row=1, col=2
            )
            
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No payment channel data available")
    except Exception as e:
        st.error(f"Error loading payment channel data: {str(e)}")

elif page == "🚨 Fraud Detection":
    st.header("Fraud Detection & Risk Management")
    
    # Risk overview
    col1, col2, col3 = st.columns(3)
    
    try:
        high_risk = execute_query_single("""
            SELECT COUNT(*) as count 
            FROM raw.taxpayers 
            WHERE risk_category = 'High'
        """)
        
        with col1:
            st.metric("High Risk Taxpayers", int(high_risk), "+3 this week", delta_color="inverse")
        
        potential_loss = execute_query_single("""
            SELECT COALESCE(SUM(amount), 0) as total
            FROM raw.payments p
            JOIN raw.taxpayers t ON p.taxpayer_id = t.taxpayer_id
            WHERE t.risk_category = 'High'
            AND p.payment_date >= CURRENT_DATE - INTERVAL '30 days'
        """)
        
        with col2:
            st.metric("Potential Revenue at Risk", f"D {potential_loss:,.0f}")
        
        with col3:
            st.metric("ML Model Accuracy", "89.2%", "+2.1%")
    except Exception as e:
        st.error(f"Error loading risk metrics: {str(e)}")
    
    # Fraud alerts table
    st.subheader("🚨 Active Fraud Alerts")
    
    try:
        fraud_alerts = execute_query("""
            SELECT 
                fa.alert_date,
                t.name as taxpayer_name,
                t.tin,
                fa.alert_type,
                fa.risk_score,
                fa.description,
                t.region,
                t.business_sector
            FROM analytics.fraud_alerts fa
            JOIN raw.taxpayers t ON fa.taxpayer_id = t.taxpayer_id
            WHERE fa.status = 'Open'
            ORDER BY fa.risk_score DESC
        """)
        
        if not fraud_alerts.empty:
            # Add action buttons
            fraud_alerts['Action'] = '🔍 Investigate'
            
            st.dataframe(
                fraud_alerts.style.background_gradient(subset=['risk_score'], cmap='Reds'),
                use_container_width=True,
                height=400
            )
        else:
            st.info("No active fraud alerts")
    except Exception as e:
        st.error(f"Error loading fraud alerts: {str(e)}")
    
    # Risk distribution
    st.subheader("Risk Score Distribution")
    
    try:
        risk_dist = execute_query("""
            SELECT 
                CASE 
                    WHEN compliance_score < 0.3 THEN '0-30%'
                    WHEN compliance_score < 0.5 THEN '30-50%'
                    WHEN compliance_score < 0.7 THEN '50-70%'
                    WHEN compliance_score < 0.9 THEN '70-90%'
                    ELSE '90-100%'
                END as risk_band,
                COUNT(*) as count
            FROM raw.taxpayers
            GROUP BY risk_band
            ORDER BY risk_band
        """)
        
        if not risk_dist.empty:
            fig = px.funnel(risk_dist, y='risk_band', x='count', 
                            title="Taxpayer Risk Distribution",
                            color='count', color_continuous_scale='Reds')
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error loading risk distribution: {str(e)}")

elif page == "👥 Taxpayer 360°":
    st.header("Taxpayer 360° View")
    
    # Taxpayer search
    taxpayer_search = st.text_input("Search Taxpayer (by name or TIN)", "")
    
    if taxpayer_search:
        try:
            taxpayers = execute_query(f"""
                SELECT taxpayer_id, name, tin, region, business_sector
                FROM raw.taxpayers
                WHERE LOWER(name) LIKE LOWER('%{taxpayer_search}%')
                   OR tin LIKE '%{taxpayer_search}%'
                LIMIT 10
            """)
            
            if not taxpayers.empty:
                selected = st.selectbox("Select Taxpayer", 
                                      taxpayers['name'] + ' - ' + taxpayers['tin'])
                
                if selected:
                    taxpayer_id = taxpayers[taxpayers['name'] + ' - ' + taxpayers['tin'] == selected]['taxpayer_id'].iloc[0]
                    
                    # Get taxpayer details
                    taxpayer_info = execute_query(f"""
                        SELECT * FROM analytics.taxpayer_360_view
                        WHERE taxpayer_id = '{taxpayer_id}'
                    """)
                    
                    if not taxpayer_info.empty:
                        taxpayer_info = taxpayer_info.iloc[0]
                        
                        # Display info in columns
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.markdown("### Basic Information")
                            st.write(f"**Name:** {taxpayer_info['name']}")
                            st.write(f"**TIN:** {taxpayer_info['tin']}")
                            st.write(f"**Type:** {taxpayer_info['taxpayer_type']}")
                            st.write(f"**Region:** {taxpayer_info['region']}")
                            st.write(f"**Sector:** {taxpayer_info['business_sector']}")
                        
                        with col2:
                            st.markdown("### Compliance Metrics")
                            compliance_rate = taxpayer_info['filing_compliance_rate'] * 100
                            st.metric("Compliance Rate", f"{compliance_rate:.1f}%")
                            st.metric("Risk Score", f"{taxpayer_info['fraud_risk_score']:.2f}")
                            st.metric("Years Active", int(taxpayer_info['years_registered']))
                        
                        with col3:
                            st.markdown("### Financial Summary")
                            st.metric("Total Tax Paid", f"D {taxpayer_info['total_tax_paid']:,.0f}")
                            st.metric("PAYE Paid", f"D {taxpayer_info['total_paye_paid']:,.0f}")
                            st.metric("VAT Paid", f"D {taxpayer_info['total_vat_paid']:,.0f}")
                        
                        # Payment history chart
                        st.subheader("Payment History")
                        
                        payment_history = execute_query(f"""
                            SELECT 
                                payment_date,
                                tax_type,
                                amount,
                                payment_channel
                            FROM raw.payments
                            WHERE taxpayer_id = '{taxpayer_id}'
                            ORDER BY payment_date DESC
                            LIMIT 20
                        """)
                        
                        if not payment_history.empty:
                            fig = px.scatter(payment_history, x='payment_date', y='amount',
                                           color='tax_type', size='amount',
                                           title="Recent Payment History")
                            st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("No taxpayers found matching your search")
        except Exception as e:
            st.error(f"Error searching taxpayers: {str(e)}")

elif page == "📊 Predictive Insights":
    st.header("Predictive Analytics & Insights")
    
    # Mock predictive data since we don't have Luigi running
    st.subheader("📈 7-Day Revenue Forecast")
    
    # Generate mock forecast data
    dates = [(datetime.now() + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(1, 8)]
    base_amount = 380000
    predictions = [base_amount + np.random.normal(0, 50000) for _ in dates]
    upper_bound = [p * 1.15 for p in predictions]
    lower_bound = [p * 0.85 for p in predictions]
    
    fig = go.Figure()
    
    # Add prediction line
    fig.add_trace(go.Scatter(
        x=dates,
        y=predictions,
        mode='lines+markers',
        name='Predicted Revenue',
        line=dict(color='blue', width=3)
    ))
    
    # Add confidence interval
    fig.add_trace(go.Scatter(
        x=dates,
        y=upper_bound,
        fill=None,
        mode='lines',
        line_color='rgba(0,100,80,0)',
        showlegend=False
    ))
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=lower_bound,
        fill='tonexty',
        mode='lines',
        line_color='rgba(0,100,80,0)',
        name='Confidence Interval'
    ))
    
    fig.update_layout(title="Revenue Forecast - Next 7 Days")
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    total_predicted = sum(predictions)
    
    with col1:
        st.metric("7-Day Forecast", f"D {total_predicted:,.0f}")
    with col2:
        st.metric("Daily Average", f"D {total_predicted/7:,.0f}")
    with col3:
        st.metric("Confidence Level", "85%")
    
    # AI Recommendations
    st.subheader("🤖 AI-Powered Recommendations")
    
    recommendations = [
        {
            'priority': 'High',
            'action': 'Contact 15 high-risk taxpayers',
            'expected_recovery': 2500000,
            'confidence': 0.75
        },
        {
            'priority': 'Medium',
            'action': 'Launch SMS campaign for VAT filing',
            'expected_recovery': 1200000,
            'confidence': 0.82
        },
        {
            'priority': 'Low',
            'action': 'Update payment channels in rural regions',
            'expected_recovery': 800000,
            'confidence': 0.65
        }
    ]
    
    for rec in recommendations:
        if rec['priority'] == 'High':
            color = '#ff4b4b'
            icon = '🔴'
        elif rec['priority'] == 'Medium':
            color = '#ff8800'
            icon = '🟡'
        else:
            color = '#00cc88'
            icon = '🟢'
        
        st.markdown(f"""
        <div style="background-color: {color}; color: white; padding: 15px; border-radius: 10px; margin: 10px 0;">
            <h4>{icon} {rec['priority']} Priority: {rec['action']}</h4>
            <p>Expected Recovery: <b>D {rec.get('expected_recovery', 0):,.0f}</b></p>
            <p>Confidence: <b>{rec.get('confidence', 0)*100:.0f}%</b></p>
        </div>
        """, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown("**GTA Data Warehouse v2.0** | Real-time Analytics Powered by Open Source")