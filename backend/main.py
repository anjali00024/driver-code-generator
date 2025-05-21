from typing_extensions import Literal
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, Union
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra
import uuid
import datetime
import tempfile
import os
import base64
import re
from cassandra.cluster import NoHostAvailable, AuthenticationFailed
import random
import astra_db_tests  # Use absolute import instead of relative import

app = FastAPI(
    title="Astra DB Query Interface",
    description="API for executing CQL queries on Astra DB",
    version="1.0.0"
)

# Enable CORS with more specific configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:3002"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600
)

VALID_MODES = ["driver", "execute", "natural_language"]
VALID_DRIVER_TYPES = ["java", "python"]

class ConnectionConfig(BaseModel):
    database_id: Optional[str] = ""
    token: str
    keyspace: str
    region: str
    secure_bundle: str

    @validator('database_id')
    def validate_database_id(cls, v):
        # No validation needed if it's empty
        if not v or not v.strip():
            return ""
        return v.strip()

    @validator('token')
    def validate_token(cls, v):
        if not v or not v.strip():
            raise ValueError("Token cannot be empty")
        return v.strip()

    @validator('keyspace')
    def validate_keyspace(cls, v):
        if not v or not v.strip():
            raise ValueError("Keyspace cannot be empty")
        return v.strip()

    @validator('region')
    def validate_region(cls, v):
        if not v or not v.strip():
            raise ValueError("Region cannot be empty")
        return v.strip()

    @validator('secure_bundle')
    def validate_secure_bundle(cls, v):
        if not v or not v.strip():
            raise ValueError("Secure bundle cannot be empty")
        try:
            base64.b64decode(v)
        except Exception:
            raise ValueError("Secure bundle must be a valid base64 encoded string")
        return v.strip()

class QueryMode(BaseModel):
    mode: str
    driver_type: Optional[str] = None
    consistency_level: Optional[str] = "LOCAL_QUORUM"
    retry_policy: Optional[str] = "DEFAULT_RETRY_POLICY"
    load_balancing_policy: Optional[str] = "TOKEN_AWARE"
    
    @validator('mode')
    def validate_mode(cls, v):
        if v not in VALID_MODES:
            raise ValueError(f"Mode must be one of {VALID_MODES}")
        return v

    @validator('driver_type')
    def validate_driver_type(cls, v, values):
        if values.get('mode') in ['driver', 'natural_language']:
            if not v or v not in VALID_DRIVER_TYPES:
                raise ValueError(f"Driver type must be one of {VALID_DRIVER_TYPES} when mode is 'driver' or 'natural_language'")
        elif v is not None and values.get('mode') == 'execute':
            raise ValueError("Driver type should not be specified when mode is 'execute'")
        return v
        
    @validator('consistency_level')
    def validate_consistency_level(cls, v):
        valid_consistency_levels = [
            "LOCAL_ONE", "LOCAL_QUORUM", "ALL", "QUORUM", "ONE"
        ]
        if v and v not in valid_consistency_levels:
            raise ValueError(f"Consistency level must be one of {valid_consistency_levels}")
        return v
        
    @validator('retry_policy')
    def validate_retry_policy(cls, v):
        valid_retry_policies = [
            "DEFAULT_RETRY_POLICY", "DOWNGRADING_CONSISTENCY_RETRY_POLICY", 
            "FALLTHROUGH_RETRY_POLICY", "NEVER_RETRY_POLICY"
        ]
        if v and v not in valid_retry_policies:
            raise ValueError(f"Retry policy must be one of {valid_retry_policies}")
        return v
        
    @validator('load_balancing_policy')
    def validate_load_balancing_policy(cls, v):
        valid_load_balancing_policies = [
            "TOKEN_AWARE", "ROUND_ROBIN", "DC_AWARE_ROUND_ROBIN"
        ]
        if v and v not in valid_load_balancing_policies:
            raise ValueError(f"Load balancing policy must be one of {valid_load_balancing_policies}")
        return v

class QueryRequest(BaseModel):
    query: str
    config: ConnectionConfig
    mode: Dict[str, str]

    @validator('query')
    def validate_query(cls, v):
        if not v or not v.strip():
            raise ValueError("Query cannot be empty")
        return v.strip()

def generate_driver_code(query_request: QueryRequest) -> dict:
    """Generate driver code based on the selected driver type."""
    driver_type = query_request.mode.get('driver_type', 'python')
    config = query_request.config
    query = query_request.query
    consistency_level = query_request.mode.get('consistency_level', 'LOCAL_QUORUM')
    retry_policy = query_request.mode.get('retry_policy', 'DEFAULT_RETRY_POLICY')
    load_balancing_policy = query_request.mode.get('load_balancing_policy', 'TOKEN_AWARE')
    
    if driver_type == "python":
        # Convert policy names to actual Python code
        retry_policy_code = {
            'DEFAULT_RETRY_POLICY': 'RetryPolicy()',
            'DOWNGRADING_CONSISTENCY_RETRY_POLICY': 'DowngradingConsistencyRetryPolicy()',
            'FALLTHROUGH_RETRY_POLICY': 'FallthroughRetryPolicy()',
            'NEVER_RETRY_POLICY': 'NeverRetryPolicy()'
        }.get(retry_policy, 'RetryPolicy()')
        
        load_balancing_policy_code = {
            'TOKEN_AWARE': 'TokenAwarePolicy(DCAwareRoundRobinPolicy())',
            'ROUND_ROBIN': 'RoundRobinPolicy()',
            'DC_AWARE_ROUND_ROBIN': 'DCAwareRoundRobinPolicy()'
        }.get(load_balancing_policy, 'TokenAwarePolicy(DCAwareRoundRobinPolicy())')
        
        code = f'''
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import (
    TokenAwarePolicy, DCAwareRoundRobinPolicy, RoundRobinPolicy,
    RetryPolicy, DowngradingConsistencyRetryPolicy, FallthroughRetryPolicy,
    NeverRetryPolicy
)

# Initialize the connection
cloud_config = {{
    'secure_connect_bundle': 'path_to_secure_connect_bundle.zip'
}}

auth_provider = PlainTextAuthProvider(
    'token',  # Username is always 'token' for Astra DB
    '{config.token}'   # Token
)

# Configure policies
retry_policy = {retry_policy_code}
load_balancing_policy = {load_balancing_policy_code}

# Create an execution profile with the policies
profile = ExecutionProfile(
    load_balancing_policy=load_balancing_policy,
    retry_policy=retry_policy,
    consistency_level=ConsistencyLevel.{consistency_level}
)

# Create the cluster with the execution profile
cluster = Cluster(
    cloud=cloud_config,
    auth_provider=auth_provider,
    protocol_version=4,
    execution_profiles={{EXEC_PROFILE_DEFAULT: profile}}
)

session = cluster.connect('{config.keyspace}')

# Execute the query
query = """{query}"""
rows = session.execute(query)

# Process results
for row in rows:
    print(row)

# Clean up
session.shutdown()
cluster.shutdown()
'''
    else:  # Java driver
        # Convert policy names to Java code
        consistency_level_java = {
            'LOCAL_ONE': 'ConsistencyLevel.LOCAL_ONE',
            'LOCAL_QUORUM': 'ConsistencyLevel.LOCAL_QUORUM',
            'ALL': 'ConsistencyLevel.ALL',
            'QUORUM': 'ConsistencyLevel.QUORUM',
            'ONE': 'ConsistencyLevel.ONE'
        }.get(consistency_level, 'ConsistencyLevel.LOCAL_QUORUM')
        
        retry_policy_java = {
            'DEFAULT_RETRY_POLICY': 'new RetryPolicy()',
            'DOWNGRADING_CONSISTENCY_RETRY_POLICY': 'new DowngradingConsistencyRetryPolicy()',
            'FALLTHROUGH_RETRY_POLICY': 'new FallthroughRetryPolicy()',
            'NEVER_RETRY_POLICY': 'new NeverRetryPolicy()'
        }.get(retry_policy, 'new RetryPolicy()')
        
        load_balancing_policy_java = {
            'TOKEN_AWARE': 'new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().build())',
            'ROUND_ROBIN': 'new RoundRobinPolicy()',
            'DC_AWARE_ROUND_ROBIN': 'new DCAwareRoundRobinPolicy.Builder().build()'
        }.get(load_balancing_policy, 'new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().build())')
        
        code = f'''
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.retry.*;
import com.datastax.oss.driver.api.core.loadbalancing.*;
import java.nio.file.Paths;

public class AstraDBConnection {{
    public static void main(String[] args) {{
        // Create the cluster and session
        CqlSession session = CqlSession.builder()
            .withCloudSecureConnectBundle(Paths.get("path_to_secure_connect_bundle.zip"))
            .withAuthCredentials("token", "{config.token}")
            .withKeyspace("{config.keyspace}")
            .withLocalDatacenter("{config.region}")
            .withRetryPolicy({retry_policy_java})
            .withLoadBalancingPolicy({load_balancing_policy_java})
            .build();
            
        try {{
            // Execute the query
            String query = "{query}";
            ResultSet rs = session.execute(query);
            
            // Process results
            for (Row row : rs) {{
                System.out.println(row.toString());
            }}
        }} finally {{
            // Clean up resources
            if (session != null) {{
                session.close();
            }}
        }}
    }}
}}'''
    
    return {
        "driver_code": code,
        "instructions": f"Generated {driver_type} driver code for your query with {consistency_level} consistency level."
    }

def parse_natural_language_query(query: str, keyspace: str, driver_type: str) -> str:
    """
    Convert natural language statements into CQL queries and/or code templates.
    """
    import re  # Import re module locally to ensure it's available
    query = query.lower().strip()
    
    # Detect Lightweight Transactions (LWT) requests
    if "lightweight" in query or "lwt" in query or "if not exists" in query or "conditional" in query or "create lwt" in query or "lwt example" in query or query.strip() == "lwt" or "use lwt" in query:
        # Default to a users table if not specified
        table_name = "users"
        table_match = re.search(r'table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1)
            
        # Determine operation type
        operation_type = "insert"
        if "update" in query:
            operation_type = "update"
            
        if driver_type == "python":
            if operation_type == "insert":
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import uuid
from datetime import datetime

# Create the users table if it doesn't exist
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    username text PRIMARY KEY,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# LWT operation: Insert only if the user doesn't already exist
insert_query = \"\"\"
    INSERT INTO {keyspace}.{table_name} (username, email, created_at)
    VALUES (?, ?, ?)
    IF NOT EXISTS
\"\"\"
prepared = session.prepare(insert_query)

# User data
username = "alice"
email = "alice@example.com"
created_at = datetime.utcnow()

# Execute the LWT operation
result = session.execute(prepared, (username, email, created_at))

# Check if the condition was applied
first_row = result[0]
if first_row.applied:
    print(f"LWT operation succeeded: User {{username}} was inserted.")
else:
    print(f"LWT operation failed: User already exists.")
    print("Current values:", {{col: getattr(first_row, col) for col in first_row._fields if col != 'applied'}})

# Display the [applied] flag - this is how LWTs report success/failure
print(f"LWT [applied] flag: {{first_row.applied}}")

# Demonstrate another LWT insert with a different user
username2 = "bob"
email2 = "bob@example.com"
created_at2 = datetime.utcnow()

result2 = session.execute(prepared, (username2, email2, created_at2))
first_row2 = result2[0]
print(f"Second LWT operation [applied]: {{first_row2.applied}}")
"""
            else:  # update operation
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

from datetime import datetime

# Create the users table if it doesn't exist and insert a sample user
setup_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    username text PRIMARY KEY,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(setup_table_query)

# First, insert a user if it doesn't exist (using LWT)
insert_query = \"\"\"
INSERT INTO {keyspace}.{table_name} (username, email, created_at)
VALUES (?, ?, ?)
IF NOT EXISTS
\"\"\"
session.execute(insert_query, ("alice", "alice@example.com", datetime.utcnow()))
print("Initial user created for demonstration")

# Now, demonstrate a conditional update with LWT
update_query = \"\"\"
UPDATE {keyspace}.{table_name}
SET email = ?
WHERE username = ?
IF email = ?
\"\"\"
prepared = session.prepare(update_query)

# Attempt to update with the correct expected current value
username = "alice"
new_email = "alice_new@example.com"
expected_old_email = "alice@example.com"  # This should match what's in the database

result = session.execute(prepared, (new_email, username, expected_old_email))
first_row = result[0]

# Check if the LWT condition was met
if first_row.applied:
    print(f"LWT update succeeded: Email for {{username}} updated to {{new_email}}")
else:
    print(f"LWT update failed: Current values don't match expectations")
    print("Current values:", {{col: getattr(first_row, col) for col in first_row._fields if col != 'applied'}})

# Now try an update that should fail the LWT condition
wrong_update_query = \"\"\"
UPDATE {keyspace}.{table_name}
SET email = ?
WHERE username = ?
IF email = ?
\"\"\"
prepared_wrong = session.prepare(wrong_update_query)

# This should fail because the email is no longer 'alice@example.com'
result_wrong = session.execute(prepared_wrong, ("alice_third@example.com", username, "alice@example.com"))
first_row_wrong = result_wrong[0]

print(f"Second LWT update [applied]: {{first_row_wrong.applied}}")
if not first_row_wrong.applied:
    print("Failed update current values:", {{col: getattr(first_row_wrong, col) for col in first_row_wrong._fields if col != 'applied'}})
"""
        else:  # Java driver
            if operation_type == "insert":
                return f"""
import java.time.Instant;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

// Create the users table if it doesn't exist
String createTableQuery = 
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "   username text PRIMARY KEY," +
    "   email text," +
    "   created_at timestamp" +
    ")";
    
session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created or verified successfully");

// LWT operation: Insert only if the user doesn't already exist
String insertQuery = 
    "INSERT INTO {keyspace}.{table_name} (username, email, created_at) " +
    "VALUES (?, ?, ?) " +
    "IF NOT EXISTS";
    
PreparedStatement prepared = session.prepare(insertQuery);

// User data
String username = "alice";
String email = "alice@example.com";
Instant createdAt = Instant.now();

// Execute the LWT operation
BoundStatement bound = prepared.bind(username, email, createdAt);
ResultSet result = session.execute(bound);

// Check if the condition was applied - first row always contains LWT result
Row firstRow = result.one();
boolean applied = firstRow.getBoolean("[applied]");

if (applied) {{
    System.out.println("LWT operation succeeded: User " + username + " was inserted.");
}} else {{
    System.out.println("LWT operation failed: User already exists.");
    System.out.println("Current values: " + firstRow.toString());
}}

// Display the [applied] flag - this is how LWTs report success/failure
System.out.println("LWT [applied] flag: " + applied);

// Demonstrate another LWT insert with a different user
String username2 = "bob";
String email2 = "bob@example.com";
Instant createdAt2 = Instant.now();

BoundStatement bound2 = prepared.bind(username2, email2, createdAt2);
ResultSet result2 = session.execute(bound2);
Row firstRow2 = result2.one();
boolean applied2 = firstRow2.getBoolean("[applied]");

System.out.println("Second LWT operation [applied]: " + applied2);
"""
            else:  # update operation
                return f"""
import java.time.Instant;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

// Create the users table if it doesn't exist
String createTableQuery = 
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "   username text PRIMARY KEY," +
    "   email text," +
    "   created_at timestamp" +
    ")";
    
session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created or verified successfully");

// First, insert a user if it doesn't exist (using LWT)
String insertQuery = 
    "INSERT INTO {keyspace}.{table_name} (username, email, created_at) " +
    "VALUES (?, ?, ?) " +
    "IF NOT EXISTS";
    
session.execute(insertQuery, "alice", "alice@example.com", Instant.now());
System.out.println("Initial user created for demonstration");

// Now, demonstrate a conditional update with LWT
String updateQuery = 
    "UPDATE {keyspace}.{table_name} " +
    "SET email = ? " +
    "WHERE username = ? " +
    "IF email = ?";
    
PreparedStatement prepared = session.prepare(updateQuery);

// Attempt to update with the correct expected current value
String username = "alice";
String newEmail = "alice_new@example.com";
String expectedOldEmail = "alice@example.com";  // This should match what's in the database

BoundStatement bound = prepared.bind(newEmail, username, expectedOldEmail);
ResultSet result = session.execute(bound);
Row firstRow = result.one();
boolean applied = firstRow.getBoolean("[applied]");

if (applied) {{
    System.out.println("LWT update succeeded: Email for " + username + " updated to " + newEmail);
}} else {{
    System.out.println("LWT update failed: Current values don't match expectations");
    System.out.println("Current values: " + firstRow.toString());
}}

// Now try an update that should fail the LWT condition
String wrongUpdateQuery = 
    "UPDATE {keyspace}.{table_name} " +
    "SET email = ? " +
    "WHERE username = ? " +
    "IF email = ?";
    
PreparedStatement preparedWrong = session.prepare(wrongUpdateQuery);

// This should fail because the email is no longer 'alice@example.com'
BoundStatement boundWrong = preparedWrong.bind("alice_third@example.com", username, "alice@example.com");
ResultSet resultWrong = session.execute(boundWrong);
Row firstRowWrong = resultWrong.one();
boolean appliedWrong = firstRowWrong.getBoolean("[applied]");

System.out.println("Second LWT update [applied]: " + appliedWrong);
if (!appliedWrong) {{
    System.out.println("Failed update current values: " + firstRowWrong.toString());
}}
"""
            
    # Basic pattern matching for common operations
    if "large partition" in query or "big partition" in query or "test partition" in query or ("partition" in query and "size" in query) or ("generate" in query and "partition" in query) or "create a large partition" in query or "large partition example" in query or query.strip() == "create large partition":
        # Extract number of rows if specified
        import re
        num_rows_match = re.search(r'(\d+)\s+rows', query)
        num_rows = int(num_rows_match.group(1)) if num_rows_match else 1000000
        
        # Extract table name if specified or use default
        table_name = "user_activity"
        table_match = re.search(r'table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1)
            
        # Extract partition key if specified or use default
        partition_key = "user_id"
        key_match = re.search(r'key\s+(\w+)', query)
        if key_match:
            partition_key = key_match.group(1)
            
        # Use a fixed partition key value to ensure all rows go to same partition
        partition_value = "large_partition_user"
        
        if driver_type == "python":
            return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import uuid
import random
import time
from datetime import datetime, timedelta

# Create a table with a simple schema for the large partition test
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    {partition_key} text,
    activity_timestamp timestamp,
    activity_type text,
    details text,
    PRIMARY KEY ({partition_key}, activity_timestamp)
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Define activity types for random selection
activity_types = ["login", "click", "view", "purchase", "logout", "scroll", "search"]
detail_templates = [
    "Viewed product: {{}}",
    "Clicked on: {{}}",
    "Searched for: {{}}",
    "Added to cart: {{}}",
    "Purchased item: {{}}",
    "Visited page: {{}}"
]
products = ["laptop", "phone", "headphones", "keyboard", "mouse", "monitor", "tablet", "camera", "speaker", "watch"]

# Use a fixed partition key value to ensure all rows go to same partition
partition_value = "large_partition_user"

# Prepare the insert statement for better performance
prepared_stmt = session.prepare(\"\"\"
    INSERT INTO {keyspace}.{table_name} ({partition_key}, activity_timestamp, activity_type, details)
    VALUES (?, ?, ?, ?)
\"\"\")

# Track timing
start_time = time.time()
batch_size = 1000  # Insert in batches for better performance
total_batches = {num_rows} // batch_size + (1 if {num_rows} % batch_size != 0 else 0)

print(f"Generating {num_rows} rows in the same partition ('{partition_value}')...")
print(f"This will create a large partition to test partition size limitations.")

for batch in range(total_batches):
    # Show progress periodically
    if batch % 10 == 0 or batch == total_batches - 1:
        print(f"Processing batch {{batch+1}}/{{total_batches}} ({{(batch+1)*100/total_batches:.1f}}%)")
    
    batch_rows = min(batch_size, {num_rows} - batch * batch_size)
    
    # Process each row in the current batch
    for i in range(batch_rows):
        # Create a random timestamp in the past 30 days
        random_minutes = random.randint(1, 30 * 24 * 60)  # 30 days in minutes
        timestamp = datetime.now() - timedelta(minutes=random_minutes)
        
        # Generate random activity data
        activity_type = random.choice(activity_types)
        detail_template = random.choice(detail_templates)
        detail = detail_template.format(random.choice(products))
        
        # Execute the prepared statement
        session.execute(prepared_stmt, [partition_value, timestamp, activity_type, detail])
    
    # Small delay to avoid overwhelming the system
    if batch < total_batches - 1:
        time.sleep(0.01)

elapsed_time = time.time() - start_time
print(f"Successfully inserted {num_rows} rows into the same partition.")
print(f"All rows have '{partition_key}' = '{partition_value}'")
print(f"Total time: {{elapsed_time:.2f}} seconds")
print(f"To query this large partition, use: SELECT * FROM {keyspace}.{table_name} WHERE {partition_key} = '{partition_value}';")
"""
        else:  # Java
            return f"""
import java.util.UUID;
import java.util.Random;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;

// Create a table with a simple schema for the large partition test
String createTableQuery = String.format(
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "{partition_key} text, " +
    "activity_timestamp timestamp, " +
    "activity_type text, " +
    "details text, " +
    "PRIMARY KEY ({partition_key}, activity_timestamp)" +
    ")");

session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created or verified successfully");

// Define activity types and details for random selection
Random random = new Random();
List<String> activityTypes = Arrays.asList("login", "click", "view", "purchase", "logout", "scroll", "search");
List<String> detailTemplates = Arrays.asList(
    "Viewed product: %s",
    "Clicked on: %s",
    "Searched for: %s",
    "Added to cart: %s", 
    "Purchased item: %s",
    "Visited page: %s"
);
List<String> products = Arrays.asList("laptop", "phone", "headphones", "keyboard", "mouse", "monitor", "tablet", "camera", "speaker", "watch");

// Use a fixed partition key value to ensure all rows go to same partition
String partitionValue = "large_partition_user";

// Prepare the insert statement for better performance
PreparedStatement preparedStmt = session.prepare(
    "INSERT INTO {keyspace}.{table_name} ({partition_key}, activity_timestamp, activity_type, details) " +
    "VALUES (?, ?, ?, ?)");

// Track timing
long startTime = System.currentTimeMillis();
int batchSize = 1000;  // Insert in batches for better performance
int totalBatches = {num_rows} / batchSize + ({num_rows} % batchSize != 0 ? 1 : 0);

System.out.println("Generating {num_rows} rows in the same partition ('" + partitionValue + "')...");
System.out.println("This will create a large partition to test partition size limitations.");

for (int batch = 0; batch < totalBatches; batch++) {{
    // Show progress periodically
    if (batch % 10 == 0 || batch == totalBatches - 1) {{
        System.out.printf("Processing batch %d/%d (%.1f%%)%n", 
            batch+1, totalBatches, (batch+1)*100.0/totalBatches);
    }}
    
    int batchRows = Math.min(batchSize, {num_rows} - batch * batchSize);
    
    // Process each row in the current batch
    for (int i = 0; i < batchRows; i++) {{
        // Create a random timestamp in the past 30 days
        int randomMinutes = random.nextInt(30 * 24 * 60); // 30 days in minutes
        Instant timestamp = Instant.now().minus(randomMinutes, ChronoUnit.MINUTES);
        
        // Generate random activity data
        String activityType = activityTypes.get(random.nextInt(activityTypes.size()));
        String detailTemplate = detailTemplates.get(random.nextInt(detailTemplates.size()));
        String product = products.get(random.nextInt(products.size()));
        String detail = String.format(detailTemplate, product);
        
        // Execute the prepared statement
        BoundStatement bound = preparedStmt.bind(partitionValue, timestamp, activityType, detail);
        session.execute(bound);
    }}
    
    // Small delay to avoid overwhelming the system
    if (batch < totalBatches - 1) {{
        try {{
            Thread.sleep(10);
        }} catch (InterruptedException e) {{
            e.printStackTrace();
        }}
    }}
}}

long elapsedTime = System.currentTimeMillis() - startTime;
System.out.println("Successfully inserted {num_rows} rows into the same partition.");
System.out.println("All rows have '{partition_key}' = '" + partitionValue + "'");
System.out.println("Total time: " + (elapsedTime / 1000.0) + " seconds");
System.out.println("To query this large partition, use: SELECT * FROM {keyspace}.{table_name} WHERE {partition_key} = '" + partitionValue + "';");
"""
            
    if "insert" in query and "rows" in query:
        # Extract number of rows if specified
        import re
        num_rows_match = re.search(r'(\d+)\s+rows', query)
        num_rows = int(num_rows_match.group(1)) if num_rows_match else 10
        
        # Generate a table name based on context or use a placeholder
        table_name = "users"
        if "table" in query and "into" in query:
            table_match = re.search(r'into\s+(\w+)', query)
            if table_match:
                table_name = table_match.group(1)
        
        # Always use random data by default
        use_random_data = not ("non-random" in query or "not random" in query)
        
        # Generate CQL for creating a table and inserting rows
        if driver_type == "python":
            if use_random_data:
                # Python code template for random data insertion
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

import random
import uuid
from datetime import datetime, timedelta

# First, create the table if it doesn't exist
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Define lists for generating random data
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", 
              "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah", "Thomas", "Karen", "Charles", "Nancy"]
last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
             "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]
domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com", "company.com", "mail.org"]

# Insert random rows
print(f"Inserting {num_rows} random rows into {keyspace}.{table_name}...")
for i in range({num_rows}):
    # Generate random data
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    name = f"{{first_name}} {{last_name}}"
    
    # Create email from name
    email_name = name.lower().replace(" ", ".")
    email = f"{{email_name}}@{{random.choice(domains)}}"
    
    # Generate random date in the past year
    days_back = random.randint(0, 365)
    random_date = datetime.now() - timedelta(days=days_back)
    created_at = random_date.strftime("%Y-%m-%d %H:%M:%S")
    
    # Insert the row
    insert_query = \"\"\"
    INSERT INTO {keyspace}.{table_name} (id, name, email, created_at)
    VALUES (uuid(), '{{name}}', '{{email}}', '{{created_at}}')
    \"\"\"
    session.execute(insert_query)
    print(f"Inserted row {{i+1}}/{num_rows}")

print(f"Successfully inserted {num_rows} random rows into {keyspace}.{table_name}")
"""
            else:
                # Python code template for sequential data
                return f"""
# Connect to your Cassandra cluster
# ... connection code will be included in the driver code template ...

# First, create the table if it doesn't exist
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created or verified successfully")

# Insert rows with sequential data
print(f"Inserting {num_rows} rows into {keyspace}.{table_name}...")
for i in range({num_rows}):
    insert_query = \"\"\"
    INSERT INTO {keyspace}.{table_name} (id, name, email, created_at)
    VALUES (uuid(), 'User {{i}}', 'user{{i}}@example.com', toTimestamp(now()))
    \"\"\"
    session.execute(insert_query)
    print(f"Inserted row {{i+1}}/{num_rows}")

print(f"Successfully inserted {num_rows} rows into {keyspace}.{table_name}")
"""
        else:  # Java
            if use_random_data:
                # Java code template for random data insertion
                return f"""
import java.util.UUID;
import java.util.Random;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

// First, create the table if it doesn't exist
String createTableQuery = String.format(
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "id uuid PRIMARY KEY, " +
    "name text, " +
    "email text, " +
    "created_at timestamp" +
    ")");

session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created or verified successfully");

// Insert random rows
Random random = new Random();
DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

// Lists for generating random data
List<String> firstNames = Arrays.asList("James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", 
    "Linda", "William", "Elizabeth", "David", "Susan", "Richard", "Jessica", "Joseph", "Sarah", "Thomas", 
    "Karen", "Charles", "Nancy");

List<String> lastNames = Arrays.asList("Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", 
    "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", 
    "Garcia", "Martinez", "Robinson");

List<String> domains = Arrays.asList("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", 
    "example.com", "company.com", "mail.org");

System.out.println("Inserting {num_rows} random rows into {keyspace}.{table_name}...");
for (int i = 0; i < {num_rows}; i++) {{
    // Generate random data
    String firstName = firstNames.get(random.nextInt(firstNames.size()));
    String lastName = lastNames.get(random.nextInt(lastNames.size()));
    String name = firstName + " " + lastName;
    
    String emailName = (firstName + "." + lastName).toLowerCase();
    String domain = domains.get(random.nextInt(domains.size()));
    String email = emailName + "@" + domain;
    
    // Random date within the last year
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime randomDate = now.minusDays(random.nextInt(365));
    String formattedDate = randomDate.format(formatter);
    
    // Insert the row
    String insertQuery = 
        "INSERT INTO {keyspace}.{table_name} (id, name, email, created_at) " +
        "VALUES (uuid(), ?, ?, ?)";
    
    session.execute(insertQuery, name, email, formattedDate);
    System.out.println("Inserted row " + (i+1) + "/{num_rows}");
}}

System.out.println("Successfully inserted {num_rows} random rows into {keyspace}.{table_name}");
"""
            else:
                # Java code template for sequential data
                return f"""
// First, create the table if it doesn't exist
String createTableQuery = String.format(
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "id uuid PRIMARY KEY, " +
    "name text, " +
    "email text, " +
    "created_at timestamp" +
    ")");

session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created or verified successfully");

// Insert rows with sequential data
System.out.println("Inserting {num_rows} rows into {keyspace}.{table_name}...");
for (int i = 0; i < {num_rows}; i++) {{
    String insertQuery = String.format(
        "INSERT INTO {keyspace}.{table_name} (id, name, email, created_at) " +
        "VALUES (uuid(), 'User %d', 'user%d@example.com', toTimestamp(now()))",
        i, i);
    
    session.execute(insertQuery);
    System.out.println("Inserted row " + (i+1) + "/{num_rows}");
}}

System.out.println("Successfully inserted {num_rows} rows into {keyspace}.{table_name}");
"""
            
    elif "create table" in query:
        # Extract table name
        table_name = "new_table"
        table_match = re.search(r'create\s+table\s+(\w+)', query)
        if table_match:
            table_name = table_match.group(1)
        
        # Generate CQL for creating a table
        if driver_type == "python":
            return f"""
# Create a new table
create_table_query = \"\"\"
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    id uuid PRIMARY KEY,
    name text,
    email text,
    created_at timestamp
)
\"\"\"
session.execute(create_table_query)
print(f"Table {keyspace}.{table_name} created successfully")
"""
        else:  # Java
            return f'''
// Create a new table
String createTableQuery = 
    "CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (" +
    "   id uuid PRIMARY KEY," +
    "   name text," +
    "   email text," +
    "   created_at timestamp" +
    ")";
    
session.execute(createTableQuery);
System.out.println("Table {keyspace}.{table_name} created successfully");
'''
    
    elif "select" in query or "query" in query or "get" in query:
        # Extract table name
        table_name = "users"  # Default table name
        
        # Generate CQL for a select query
        if driver_type == "python":
            return f'''
# Perform a SELECT query
select_query = "SELECT * FROM {keyspace}.{table_name} LIMIT 100"
rows = session.execute(select_query)

# Process results
for row in rows:
    print(row)
    
# Count total rows returned
print(f"Total rows returned: {{sum(1 for _ in rows)}}")
'''
        else:  # Java
            return f'''
// Perform a SELECT query
String selectQuery = "SELECT * FROM {keyspace}.{table_name} LIMIT 100";
ResultSet rs = session.execute(selectQuery);

// Process results
int count = 0;
for (Row row : rs) {{
    System.out.println(row.toString());
    count++;
}}

System.out.println("Total rows returned: " + count);
'''
    
    else:
        # Default case for unrecognized requests
        return f"// Could not parse natural language request: '{query}'\n// Please provide a valid CQL query or a clearer instruction."

@app.get("/")
async def root():
    return {
        "message": "Astra DB Query Interface API",
        "version": "1.0.0",
        "endpoints": {
            "/": "This documentation",
            "/api/execute-query": "Execute CQL queries (POST)",
            "/api/generate-driver-code": "Generate driver code (POST)",
            "/api/run-tests": "Run Astra DB feature tests (LWT and large partitions) (POST)"
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

def connect_to_astra(config: ConnectionConfig):
    temp_file_path = None
    try:
        print(f"Using keyspace: {config.keyspace}")
        if config.database_id:
            print(f"Database ID: {config.database_id}")
        print(f"Region: {config.region}")
        
        # Save secure bundle to temp file
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file_obj:
            temp_file_obj.write(base64.b64decode(config.secure_bundle))
            temp_file_path = temp_file_obj.name
            print(f"Attempting to connect with cloud config: {{'secure_connect_bundle': {temp_file_path}}}")

        # Use the token directly without parsing
        auth_provider = PlainTextAuthProvider(username="token", password=config.token)
        
        # Connect to Astra using CloudConfig
        cloud_config = {
            'secure_connect_bundle': temp_file_path
        }
        
        # Create cluster and connect using standard synchronous API
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        
        print("Testing connection...")
        session = cluster.connect()
        
        # Set keyspace
        print(f"Connected successfully, testing keyspace {config.keyspace}...")
        session.set_keyspace(config.keyspace)
        print("Keyspace set successfully")
        
        return session, cluster, temp_file_path
        
    except Exception as e:
        print(f"Unexpected error during connection: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as cleanup_err:
                print(f"Error during cleanup: {cleanup_err}")
        raise HTTPException(status_code=500, detail=f"Failed to connect: {str(e)}")

async def execute_query(query: str, config: ConnectionConfig):
    session = None
    cluster = None
    temp_file_path = None
    
    try:
        # Connect to Astra
        session, cluster, temp_file_path = connect_to_astra(config)
        
        # Execute query
        print(f"Executing CQL query: {query}")
        
        # Execute query synchronously
        rows = session.execute(query)
        
        # Convert result to dictionary format
        columns = rows.column_names
        results = []
        
        print(f"Query executed successfully. Column names: {columns}")
        
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert non-serializable values to strings
                if isinstance(value, (uuid.UUID, datetime.datetime, datetime.date)):
                    value = str(value)
                row_dict[col] = value
            results.append(row_dict)
        
        print(f"Processed {len(results)} rows of data")
        
        response_data = {
            "columns": columns,
            "rows": results
        }
        
        print(f"Returning response: {response_data}")
        return response_data
        
    except Exception as e:
        print(f"Query execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
        
    finally:
        # Clean up resources
        if session:
            try:
                session.shutdown()
            except Exception as e:
                print(f"Error during session shutdown: {e}")
        if cluster:
            try:
                cluster.shutdown()
            except Exception as e:
                print(f"Error during cluster shutdown: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as cleanup_err:
                print(f"Error during cleanup: {cleanup_err}")

@app.post("/api/execute-query")
async def handle_query(request: QueryRequest):
    mode = request.mode.get('mode', 'execute')
    
    if mode not in VALID_MODES:
        raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be one of {VALID_MODES}")
    
    try:
        query = request.query
        config = request.config
        
        print(f"Received query request: {query}")
        
        if mode == "driver":
            # Generate driver code for the query
            driver_response = generate_driver_code(request)
            print(f"Generated driver code response")
            return driver_response
        elif mode == "natural_language":
            # Parse natural language query
            driver_type = request.mode.get('driver_type', 'python')
            parsed_query = parse_natural_language_query(query, config.keyspace, driver_type)
            
            # For natural language queries, we need to return the entire code block
            # rather than passing it through the generic driver code generator
            print(f"Generated natural language response for: {query}")
            
            return {
                "driver_code": parsed_query,
                "original_query": query,
                "instructions": f"Generated {driver_type} driver code from your natural language request."
            }
        else:
            # Execute query and return results
            print(f"Executing query: {query}")
            result = await execute_query(query, config)
            print(f"Query execution completed")
            
            return {
                "columns": result["columns"] if "columns" in result else None,
                "rows": result["rows"] if "rows" in result else None
            }
    except Exception as e:
        print(f"Unexpected error during connection: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        else:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/run-tests")
async def run_astra_db_tests(request: QueryRequest):
    """
    Run tests for Astra DB features (LWT and large partitions)
    
    This endpoint requires the same connection configuration as execute-query
    but instead of running a user query, it runs a series of tests to demonstrate
    and validate Astra DB features.
    """
    session = None
    cluster = None
    temp_file_path = None
    
    try:
        # Use the same connection setup as execute_query
        session, cluster, temp_file_path = connect_to_astra(request.config)
        
        # Extract test parameters from query
        test_type = "all"  # Default to run all tests
        num_rows = 100  # Default number of rows for large partition test
        
        query_lower = request.query.lower()
        
        # Parse query for specific test type
        if "lwt" in query_lower:
            test_type = "lwt"
        elif "partition" in query_lower:
            test_type = "partition"
            
            # Extract number of rows if specified
            num_rows_match = re.search(r'(\d+)\s+rows', query_lower)
            if num_rows_match:
                num_rows = int(num_rows_match.group(1))
        
        # Run the requested tests
        keyspace = request.config.keyspace
        
        if test_type == "lwt":
            results = astra_db_tests.test_lwt_operations(session, keyspace)
            return {
                "test_type": "Lightweight Transactions (LWT)",
                "results": results
            }
        elif test_type == "partition":
            results = astra_db_tests.test_large_partition(
                session, 
                keyspace, 
                num_rows=num_rows
            )
            return {
                "test_type": "Large Partition",
                "rows_tested": num_rows,
                "results": results
            }
        else:
            # Run all tests
            results = astra_db_tests.run_all_tests(session, keyspace)
            return {
                "test_type": "All Tests",
                "results": results
            }
            
    except Exception as e:
        print(f"Test execution error: {e}")
        raise HTTPException(status_code=500, detail=f"Test execution failed: {str(e)}")
        
    finally:
        # Clean up resources
        if session:
            try:
                session.shutdown()
            except Exception as e:
                print(f"Error during session shutdown: {e}")
        if cluster:
            try:
                cluster.shutdown()
            except Exception as e:
                print(f"Error during cluster shutdown: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except Exception as cleanup_err:
                print(f"Error during cleanup: {cleanup_err}")

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "Request failed",
            "detail": exc.detail,
            "status_code": exc.status_code
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    ) 