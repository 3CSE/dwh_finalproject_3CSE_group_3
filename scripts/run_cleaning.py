"""
Script to execute all cleaning SQL scripts in the staging layer.
This script creates views that clean and transform data from staging tables.
"""

import os
import logging
from database_connection import get_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_cleaning_scripts():
    """Execute all cleaning SQL scripts in order."""
    
    # Define the order of cleaning scripts
    cleaning_scripts = [
        'clean_product_list.sql',
        'clean_line_item_products.sql',
        'clean_line_item_prices.sql',
        'clean_order.sql',
        'clean_order_delays.sql',
        'clean_user_data.sql',
        'clean_user_job.sql',
        'clean_user_credit_card.sql',
        'clean_merchant_data.sql',
        'clean_order_merchant.sql',
        'clean_staff.sql',
        'clean_campaign.sql',
        'clean_campaign_transactions.sql'
    ]
    
    sql_clean_dir = '/opt/airflow/sql/clean'
    
    # Get database connection
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        for script_name in cleaning_scripts:
            script_path = os.path.join(sql_clean_dir, script_name)
            
            if not os.path.exists(script_path):
                logger.warning(f"Script not found: {script_path}")
                continue
            
            logger.info(f"Executing cleaning script: {script_name}")
            
            try:
                # Read and execute the SQL script
                with open(script_path, 'r') as f:
                    sql_content = f.read()
                
                cursor.execute(sql_content)
                conn.commit()
                logger.info(f"✓ Successfully executed: {script_name}")
                
            except Exception as e:
                logger.error(f"✗ Error executing {script_name}: {str(e)}")
                conn.rollback()
                raise
        
        cursor.close()
        logger.info("All cleaning scripts executed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during cleaning process: {str(e)}")
        return False
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Starting data cleaning process...")
    success = run_cleaning_scripts()
    
    if success:
        logger.info("Data cleaning completed successfully!")
        exit(0)
    else:
        logger.error("Data cleaning failed!")
        exit(1)
