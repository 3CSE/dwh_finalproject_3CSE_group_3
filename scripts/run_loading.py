"""
Script to execute all loading SQL scripts to populate the data warehouse.
This script loads data from cleaned staging views into dimension and fact tables.
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

def run_loading_scripts():
    """Execute all loading SQL scripts in the correct order (dimensions first, then facts)."""
    
    # Define the order of loading scripts
    # Dimensions must be loaded before facts due to foreign key constraints
    loading_scripts = [
        # Load dimensions first
        'load_dim_date.sql',
        'load_dim_product.sql',
        'load_dim_customer.sql',
        'load_dim_merchant.sql',
        'load_dim_staff.sql',
        'load_dim_campaign.sql',
        # Load facts last (they reference dimensions)
        'load_fact_order.sql',
        'load_fact_order_line_item.sql'
    ]
    
    sql_load_dir = '/opt/airflow/sql/load'
    
    # Get database connection
    conn = get_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return False
    
    try:
        cursor = conn.cursor()
        
        for script_name in loading_scripts:
            script_path = os.path.join(sql_load_dir, script_name)
            
            if not os.path.exists(script_path):
                logger.warning(f"Script not found: {script_path}")
                continue
            
            logger.info(f"Executing loading script: {script_name}")
            
            try:
                # Read and execute the SQL script
                with open(script_path, 'r') as f:
                    sql_content = f.read()
                
                cursor.execute(sql_content)
                rows_affected = cursor.rowcount
                conn.commit()
                
                logger.info(f"✓ Successfully executed: {script_name} (rows affected: {rows_affected})")
                
            except Exception as e:
                logger.error(f"✗ Error executing {script_name}: {str(e)}")
                conn.rollback()
                raise
        
        # Log summary of loaded data
        logger.info("\n" + "="*50)
        logger.info("DATA WAREHOUSE LOADING SUMMARY")
        logger.info("="*50)
        
        summary_queries = {
            'DimProduct': 'SELECT COUNT(*) FROM warehouse.DimProduct',
            'DimCustomer': 'SELECT COUNT(*) FROM warehouse.DimCustomer',
            'DimMerchant': 'SELECT COUNT(*) FROM warehouse.DimMerchant',
            'DimStaff': 'SELECT COUNT(*) FROM warehouse.DimStaff',
            'DimCampaign': 'SELECT COUNT(*) FROM warehouse.DimCampaign',
            'DimDate': 'SELECT COUNT(*) FROM warehouse.DimDate',
            'FactOrder': 'SELECT COUNT(*) FROM warehouse.FactOrder',
            'FactOrderLineItem': 'SELECT COUNT(*) FROM warehouse.FactOrderLineItem'
        }
        
        for table_name, query in summary_queries.items():
            try:
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logger.info(f"{table_name}: {count:,} rows")
            except Exception as e:
                logger.warning(f"Could not get count for {table_name}: {str(e)}")
        
        logger.info("="*50)
        
        cursor.close()
        logger.info("\nAll loading scripts executed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error during loading process: {str(e)}")
        return False
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Starting data warehouse loading process...")
    success = run_loading_scripts()
    
    if success:
        logger.info("Data warehouse loading completed successfully!")
        exit(0)
    else:
        logger.error("Data warehouse loading failed!")
        exit(1)
