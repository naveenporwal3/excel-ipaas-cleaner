import streamlit as st
import pandas as pd
import io  # Added for Excel export
from excel_cleaner import DataCleaningPipeline, VALIDATION_SCHEMA  # Import from your previous file

st.set_page_config(page_title="Custom iPaaS - Excel Cleaner", layout="wide")

st.title("🚀 Custom iPaaS: Excel Data Cleaner GUI")
st.markdown("Upload your messy Excel file, clean it with your schema, and download the results!")

# Sidebar for schema tweaks (optional extensibility)
st.sidebar.header("Cleaning Schema")
schema_display = "\n".join([f"- {k}: {v}" for k, v in VALIDATION_SCHEMA.items()])
st.sidebar.text_area("Current Rules", schema_display, height=200, disabled=True)
st.sidebar.info("Edit `VALIDATION_SCHEMA` in `excel_cleaner.py` to customize.")

# Main app
uploaded_file = st.file_uploader("Choose an Excel file", type=["xlsx", "xls"])

if uploaded_file is not None:
    # Load and preview
    @st.cache_data
    def load_data(file):
        return pd.read_excel(file)

    df = load_data(uploaded_file)
    st.subheader("📊 Original Data Preview")
    st.dataframe(df.head(10), use_container_width=True)  # Show first 10 rows

    # Cleaning button
    if st.button("🧹 Clean Data", type="primary"):
        with st.spinner("Running pipeline..."):
            pipeline = DataCleaningPipeline(VALIDATION_SCHEMA)
            cleaned_df = pipeline.clean_and_validate(df)

        # Results
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Rows Retained", f"{pipeline.cleaned_rows}/{pipeline.total_rows}")
        with col2:
            st.metric("Errors Fixed", len(pipeline.errors))

        st.subheader("✅ Cleaned Data Preview")
        st.dataframe(cleaned_df.head(10), use_container_width=True)

        # Error report
        if pipeline.errors:
            st.subheader("⚠️ Cleaning Log")
            for error in pipeline.errors:
                st.warning(error)

        # Download as Excel (fixed version)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            cleaned_df.to_excel(writer, index=False)
        output.seek(0)
        data = output.read()
        st.download_button(
            label="💾 Download Cleaned Excel",
            data=data,
            file_name="cleaned_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # Optional: Show full cleaned data
        if st.checkbox("Show full cleaned dataset"):
            st.dataframe(cleaned_df, use_container_width=True)
else:
    st.info("👆 Upload a file to get started!")

# Footer
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit. Extend for more integrations!")
