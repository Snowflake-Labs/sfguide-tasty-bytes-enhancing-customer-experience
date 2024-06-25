import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
st.title("Simulate New Email")

session = get_active_session()

sender = st.text_input('Sender', '')
subject = st.text_input('Subject', '')
body = st.text_area("**:black[Body:]**", 
               value="", 
               placeholder="",
               height=300)

email_content = {
  "body": body if body is not None else "Unknown Body",
  "sender": sender if sender is not None else "Unknown Sender",
  "subject": subject if subject is not None else "Unknown Subject",
  "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
}

email_content_str = json.dumps(email_content)
if st.button("**:blue[Send]**"):
  try :
    insert_email = session.sql("CALL RAW_SUPPORT.INSERT_NEW_EMAIL_APP(?)", [email_content_str]).collect()
    st.success(insert_email[0]['INSERT_NEW_EMAIL_APP'], icon="✅")
    st.info('Processing the Email', icon="ℹ️")
    process_email = session.sql("CALL RAW_SUPPORT.PROCESS_AUTO_RESPONSES_APP()").collect()
    st.success(process_email[0]['PROCESS_AUTO_RESPONSES_APP'], icon="✅")
  except Exception as e:
    st.exception(e)