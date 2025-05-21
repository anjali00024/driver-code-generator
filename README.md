# Astra DB Query Interface

A React-based web interface for executing Astra DB queries and generating driver code.

## Features

- Connect to Astra DB
- Execute CQL queries
- View query results in a tabular format
- Generate driver code for your queries
- Support for optional keyspace selection
- Error handling and loading states

## Prerequisites

- Node.js (v14 or higher)
- Python 3.7 or higher
- npm or yarn
- Access to an Astra DB database

## Important Notes

- Astra DB does not support Materialized Views
- This application focuses on CQL queries and driver code generation for features supported by Astra DB

## Setup and Running

### Backend

1. Navigate to the backend directory:
   ```bash
   cd backend
   ```

2. Create and activate a new virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Start the backend server:
   ```bash
   uvicorn main:app --reload --port 8000
   ```

   The backend API will be available at http://localhost:8000

### Frontend

1. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm start
   ```

4. Open [http://localhost:3000](http://localhost:3000) in your browser

## Usage

1. Enter your Astra DB connection details:
   - Secure Connect Bundle (upload your Astra DB credentials zip file)
   - Client ID
   - Client Secret
   - Keyspace
2. Click "Connect"
3. Once connected, you can:
   - Enter CQL queries in the query editor
   - Execute queries and view results
   - View generated driver code for your queries
   - Disconnect when done

## Quick Start

You can also use the provided script to start both servers simultaneously:

```bash
chmod +x start_servers.sh
./start_servers.sh
``` 