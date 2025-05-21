"""
Astra DB Test Utilities

This module provides testing utilities for Astra DB features:
- Lightweight Transactions (LWT)
- Large Partitions

These functions can be used to test and demonstrate Astra DB capabilities
directly from within the backend code.
"""

from typing import Dict, Any, List, Optional, Tuple
import json
import time
from datetime import datetime, timedelta
import random
import uuid

# LWT Testing Functions
def test_lwt_operations(session, keyspace: str, table_name: str = "users_lwt") -> Dict[str, Any]:
    """
    Test Lightweight Transaction operations and return results
    
    Args:
        session: Cassandra session
        keyspace: Keyspace to use
        table_name: Table name to use for testing
        
    Returns:
        Dictionary with test results
    """
    results = {
        "success": True,
        "operations": [],
        "errors": []
    }
    
    try:
        # Create the test table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
            username text PRIMARY KEY,
            email text,
            created_at timestamp
        )
        """
        session.execute(create_table_query)
        results["operations"].append("Table created successfully")
        
        # Insert with LWT - should succeed
        insert_query = f"""
        INSERT INTO {keyspace}.{table_name} (username, email, created_at)
        VALUES (?, ?, ?)
        IF NOT EXISTS
        """
        prepared = session.prepare(insert_query)
        
        # Generate unique username to avoid test conflicts
        test_id = uuid.uuid4().hex[:8]
        username = f"user_{test_id}"
        email = f"{username}@example.com"
        created_at = datetime.now()
        
        result = session.execute(prepared, (username, email, created_at))
        first_row = result[0]
        
        results["operations"].append({
            "operation": "INSERT IF NOT EXISTS",
            "username": username,
            "applied": first_row.applied
        })
        
        # Try to insert the same user again - should fail
        result2 = session.execute(prepared, (username, email, created_at))
        first_row2 = result2[0]
        
        results["operations"].append({
            "operation": "INSERT IF NOT EXISTS (duplicate)",
            "username": username,
            "applied": first_row2.applied
        })
        
        # Test conditional update
        update_query = f"""
        UPDATE {keyspace}.{table_name}
        SET email = ?
        WHERE username = ?
        IF email = ?
        """
        prepared_update = session.prepare(update_query)
        
        # Update with correct condition
        new_email = f"{username}_updated@example.com"
        result3 = session.execute(prepared_update, (new_email, username, email))
        first_row3 = result3[0]
        
        results["operations"].append({
            "operation": "UPDATE IF email matches",
            "username": username,
            "applied": first_row3.applied
        })
        
        # Update with incorrect condition - should fail
        result4 = session.execute(prepared_update, ("another_email@example.com", username, email))
        first_row4 = result4[0]
        
        results["operations"].append({
            "operation": "UPDATE IF email doesn't match",
            "username": username,
            "applied": first_row4.applied
        })
        
        # Delete with condition
        delete_query = f"""
        DELETE FROM {keyspace}.{table_name}
        WHERE username = ?
        IF email = ?
        """
        prepared_delete = session.prepare(delete_query)
        
        # Delete with correct condition
        result5 = session.execute(prepared_delete, (username, new_email))
        first_row5 = result5[0]
        
        results["operations"].append({
            "operation": "DELETE IF email matches",
            "username": username,
            "applied": first_row5.applied
        })
        
    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
    
    return results

# Large Partition Testing Functions
def test_large_partition(
    session, 
    keyspace: str, 
    table_name: str = "user_activity",
    partition_key: str = "user_id",
    num_rows: int = 1000
) -> Dict[str, Any]:
    """
    Test creating and querying a large partition
    
    Args:
        session: Cassandra session
        keyspace: Keyspace to use
        table_name: Table name to use
        partition_key: Partition key column name
        num_rows: Number of rows to insert
        
    Returns:
        Dictionary with test results
    """
    results = {
        "success": True,
        "operations": [],
        "metrics": {},
        "errors": []
    }
    
    try:
        # Create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
            {partition_key} text,
            activity_timestamp timestamp,
            activity_type text,
            details text,
            PRIMARY KEY ({partition_key}, activity_timestamp)
        ) WITH CLUSTERING ORDER BY (activity_timestamp DESC)
        """
        session.execute(create_table_query)
        results["operations"].append("Table created successfully")
        
        # Use a unique partition key value
        test_id = uuid.uuid4().hex[:8]
        partition_value = f"test_user_{test_id}"
        
        # Prepare the insert statement
        insert_query = f"""
        INSERT INTO {keyspace}.{table_name} 
        ({partition_key}, activity_timestamp, activity_type, details)
        VALUES (?, ?, ?, ?)
        """
        prepared = session.prepare(insert_query)
        
        # Activity types for random selection
        activity_types = ["login", "click", "view", "purchase", "logout", "scroll", "search"]
        
        # Start timing
        start_time = time.time()
        
        # Insert rows
        batch_size = min(100, num_rows)  # Use smaller batch for testing
        for i in range(num_rows):
            # Create timestamp in the past 7 days
            random_minutes = random.randint(1, 7 * 24 * 60)
            timestamp = datetime.now() - timedelta(minutes=random_minutes)
            
            # Generate random activity data
            activity_type = random.choice(activity_types)
            details = f"Test activity {i} at {timestamp}"
            
            # Execute insert
            session.execute(prepared, (partition_value, timestamp, activity_type, details))
            
            # Update progress every batch_size rows
            if (i + 1) % batch_size == 0 or i == num_rows - 1:
                percent_complete = ((i + 1) / num_rows) * 100
                results["operations"].append(f"Inserted {i + 1}/{num_rows} rows ({percent_complete:.1f}%)")
        
        # Record insertion time
        insertion_time = time.time() - start_time
        results["metrics"]["insertion_time_seconds"] = insertion_time
        results["metrics"]["rows_per_second"] = num_rows / insertion_time
        
        # Test querying: Bad practice (full partition scan)
        query_start_time = time.time()
        count_query = f"""
        SELECT COUNT(*) FROM {keyspace}.{table_name} 
        WHERE {partition_key} = ?
        """
        count_result = session.execute(session.prepare(count_query), (partition_value,))
        count = count_result.one().count
        full_query_time = time.time() - query_start_time
        
        results["metrics"]["full_partition_scan"] = {
            "count": count,
            "query_time_seconds": full_query_time
        }
        
        # Test querying: Good practice (time slice)
        query_start_time = time.time()
        now = datetime.now()
        one_day_ago = now - timedelta(days=1)
        
        time_slice_query = f"""
        SELECT COUNT(*) FROM {keyspace}.{table_name} 
        WHERE {partition_key} = ? 
        AND activity_timestamp >= ? 
        AND activity_timestamp <= ?
        """
        prepared_slice = session.prepare(time_slice_query)
        slice_result = session.execute(prepared_slice, (partition_value, one_day_ago, now))
        slice_count = slice_result.one().count
        slice_query_time = time.time() - query_start_time
        
        results["metrics"]["time_slice_query"] = {
            "count": slice_count,
            "query_time_seconds": slice_query_time,
            "start_time": str(one_day_ago),
            "end_time": str(now)
        }
        
        # Comparison
        if full_query_time > 0 and slice_query_time > 0:
            results["metrics"]["speedup_factor"] = full_query_time / slice_query_time
            
    except Exception as e:
        results["success"] = False
        results["errors"].append(str(e))
    
    return results

def run_all_tests(session, keyspace: str) -> Dict[str, Any]:
    """
    Run all Astra DB tests
    
    Args:
        session: Cassandra session
        keyspace: Keyspace to use
        
    Returns:
        Dictionary with all test results
    """
    results = {
        "lwt_tests": test_lwt_operations(session, keyspace),
        "large_partition_tests": test_large_partition(session, keyspace)
    }
    
    return results 