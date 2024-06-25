import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import pandas as pd
import altair as alt
from snowflake.cortex import Complete

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Tasty Bytes - Enhance Customer Experience")

if 'selected_truck' not in st.session_state:
   st.session_state.selected_truck = 'Select Truck'

# Establishing session
session = get_active_session()

def on_truck_selection():
  st.session_state.selected_truck = st.session_state.new_truck

def get_trucks_list(): 
  if 'truck_list' not in st.session_state:
    st.session_state.truck_list = session.sql("""select distinct concat(TRUCK_ID, ' - ', TRUCK_BRAND_NAME) as TRUCK
                                                  from analytics.inspection_reports_v;""").to_pandas()
  return st.session_state.truck_list


col1, col2 = st.columns(2)
city = col1.selectbox('City',
                      ['San Mateo'])

trucks_list = get_trucks_list()["TRUCK"].to_list()
trucks_list.insert(0, 'Select Truck')
st.session_state.selected_truck = col2.selectbox('Truck',
                                                trucks_list,
                                                index=trucks_list.index(st.session_state.selected_truck),
                                                on_change=on_truck_selection,
                                                key="new_truck")

truck_id = st.session_state.selected_truck.split(' - ')[0].strip()

if st.session_state.selected_truck != 'Select Truck': 
  reviews, inspections = st.tabs(["Reviews", "Inspections"])
  with reviews:
    col1, col2 = st.columns(2, gap="large")
    no_of_reviews = session.sql(f"""select count(*) as REVIEW_COUNT,
                                    ROUND(AVG(SENTIMENT), 2) as AVG_SENTIMENT_SCORE
                                    from analytics.review_analysis_output
                                    where truck_id = {truck_id};""").to_pandas()
    col3, col4 = col1.columns(2)
    col3.markdown(f'''**{str(no_of_reviews['REVIEW_COUNT'][0])}**  
                  # of Reviews''')
    col4.markdown(f'''**{str(no_of_reviews['AVG_SENTIMENT_SCORE'][0])}**  
                  Avg. Reviews Sentiment Score''')
    st.write("")
    
    ratings_data = session.sql(f"""select CLEAN_RATING, 
                                  count(*) as COUNT
                                  from analytics.review_analysis_output
                                  where clean_rating is not null
                                  and truck_id = {truck_id}
                                  group by clean_rating;""").to_pandas()
    rating_count = []
    ratings = []
    if len(ratings_data) > 0:
        index = 0 
        while index < len(ratings_data):
            rating_count.append(ratings_data['COUNT'][index])
            ratings.append(ratings_data['CLEAN_RATING'][index])
            index += 1
    else:
        st.markdown("None") 
    rating_df = pd.DataFrame({
        "RatingCount": rating_count,
        "Ratings": ratings
        })
    bar_chart = alt.Chart(rating_df, title="Reviews by Rating").mark_bar().encode(
        y="Ratings:O",
        x="RatingCount:Q"
    )
    col2.altair_chart(bar_chart, use_container_width=True)

    recommendations_data = session.sql(f"""select clean_recommend,
                                          count(*) as COUNT
                                          from analytics.review_analysis_output
                                          where clean_recommend is not null
                                          and truck_id = {truck_id}
                                          group by clean_recommend;""").to_pandas()
    recommendation_count = []
    recommendations = []
    if len(recommendations_data) > 0:
        index = 0 
        while index < len(recommendations_data):
            recommendation_count.append(recommendations_data['COUNT'][index])
            recommendations.append(recommendations_data['CLEAN_RECOMMEND'][index])
            index += 1
    else:
        st.markdown("None") 
    recommendation_df = pd.DataFrame({
        "RecommendationCount": recommendation_count,
        "Recommendations": recommendations
        })
    bar_chart = alt.Chart(recommendation_df, title="Reviews by Recommendation").mark_bar().encode(
        y="Recommendations:O",
        x="RecommendationCount:Q"
    )
    col2.altair_chart(bar_chart, use_container_width=True)

    col3.markdown("**:black[Top Positive Reviews Category]**")
    positive_categories_data = session.sql(f"""SELECT TOP 5 INITCAP(category) AS CATEGORY,
                                              SUM(CASE WHEN category_sentiment = 'positive' THEN 1 ELSE NULL END) AS positive_count
                                              FROM analytics.review_analysis_output_v
                                              where truck_id = {truck_id}
                                              GROUP BY category 
                                              HAVING 1=1
                                              AND positive_count IS NOT NULL
                                              AND category NOT IN ('price','menu options','food quality','overall experience')
                                              ORDER BY positive_count DESC;""").to_pandas()
    for i, row in positive_categories_data.iterrows():
        col3.write(f"{i+1}. {row['CATEGORY']}")


    col4.markdown("**:black[Top Negative Reviews Category]**")
    st.session_state.negative_categories_data = session.sql(f"""SELECT TOP 5 INITCAP(category) AS CATEGORY,
                                                                SUM(CASE WHEN category_sentiment = 'negative' THEN 1 ELSE NULL END) AS negative_count
                                                                FROM analytics.review_analysis_output_v
                                                                where truck_id = {truck_id}
                                                                GROUP BY category
                                                                HAVING 1=1
                                                                AND negative_count IS NOT NULL
                                                                AND category NOT IN ('price','menu options','food quality','overall experience')
                                                                ORDER BY negative_count DESC;""").to_pandas()
    for i, row in st.session_state.negative_categories_data.iterrows():
        col4.write(f"{i+1}. {row['CATEGORY']}")

  with inspections:
    col1, col2, col3 = st.columns([3,3,6])
    inspection_count = session.sql(f"""select count(*) as COUNT
                                      from analytics.inspection_reports_v
                                      where truck_id = {truck_id};""").to_pandas()
    col1.markdown(f'''**{str(inspection_count['COUNT'][0])}**  
                # of Inspections''')

    pass_percentage = session.sql(f"""SELECT ROUND(((COUNT(CASE WHEN overall_result = 'Pass' THEN 1 END) * 100.0) / COUNT(*)), 2) AS pass_percentage
                                      FROM analytics.inspection_reports_v
                                      where truck_id = {truck_id};""").to_pandas()
    col2.markdown(f'''**{str(pass_percentage['PASS_PERCENTAGE'][0])}%**  
                Pass''')
    col3.markdown("**:black[Top Failure Categories]**")
    st.session_state.failure_reasons = session.sql(f"""select description, 
                                                      (COUNT(CASE WHEN result = 'Fail' THEN 1 END)) AS FAILED_COUNT
                                                      from analytics.inspection_reports_unpivot_v
                                                      where truck_id = {truck_id}
                                                      group by description
                                                      order by FAILED_COUNT desc
                                                      limit 3;""").to_pandas()
    for i, row in st.session_state.failure_reasons.iterrows():
        col3.write(f"{i+1}. {row['DESCRIPTION']}")
    st.subheader("Reports")
    reports = session.sql(f"""select date, 
                              overall_result, 
                              presigned_url
                              from analytics.inspection_reports_v
                              where truck_id = {truck_id}
                              order by date desc;""").to_pandas()
    for i, row in reports.iterrows():
      with st.expander(f"{(datetime.strptime(str(row['DATE']), '%Y-%m-%d')).strftime('%m/%d/%Y')} -  {row['OVERALL_RESULT']}"):
        st.image(row['PRESIGNED_URL'], width=1000) 

  col1, col2, col3, col4 = st.columns(4)
  with st.expander("**:red[Email the Owner for Improvement Suggestions]**"):
    negative_review_categories = ', '.join(st.session_state.negative_categories_data['CATEGORY'].to_list())
    negative_inspection_categories = ', '.join(st.session_state.failure_reasons['DESCRIPTION'].to_list())
    prompt =f"""[INST]### Write me survey report email to the franchise owner summarizing the issues mentioned in following 
                aggregated customer reviews and inspections with three concise bullet points under 50 words each such that each bullet 
                point also has a heading along with recommendations to remedy those issues. Negative Reviews Category:
                {negative_review_categories}. Inspection Reports Category: {negative_inspection_categories}[/INST]"""
    email_suggestion = Complete('mistral-large', prompt.replace("'", "''"))
    st.write(email_suggestion)
    if st.button("Send"):
      st.success("Email Sent Successfully")

    

  
  



  
