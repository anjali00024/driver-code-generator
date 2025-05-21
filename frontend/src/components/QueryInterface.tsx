import React, { useState, ChangeEvent } from 'react';
import {
  Box,
  Button,
  Textarea,
  VStack,
  useToast,
  Code,
  Text,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  TableContainer,
  Heading,
  CircularProgress,
  RadioGroup,
  Stack,
  Radio,
} from '@chakra-ui/react';

interface ConnectionConfig {
  secureConnectBundle: string;
  keyspace: string;
  token: string;
  region: string;
  connectionMethod: 'driver';
  driverType: 'python' | 'java';
  consistencyLevel: string;
  retryPolicy: string;
  loadBalancingPolicy: string;
  dbUUID: string;
}

interface QueryInterfaceProps {
  config: ConnectionConfig;
}

interface QueryResult {
  columns?: string[];
  rows?: Record<string, any>[];
  driver_code?: string;
  original_query?: string;
  instructions?: string;
  error?: string;
  execution_time?: number;
}

function QueryInterface({ config }: QueryInterfaceProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<QueryResult | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [queryMode, setQueryMode] = useState<'execute' | 'driver' | 'natural_language'>('driver');
  const toast = useToast();

  const handleSubmit = async () => {
    if (!query.trim()) {
      toast({ title: 'Query cannot be empty', status: 'warning', duration: 3000, isClosable: true });
      return;
    }
    setIsLoading(true);
    setResults(null);

    const backendPayload = {
      query: query,
      config: {
        database_id: config.dbUUID || '',
        token: config.token,
        keyspace: config.keyspace,
        region: config.region,
        secure_bundle: config.secureConnectBundle
      },
      mode: {
        mode: queryMode,
        driver_type: config.driverType,
        consistency_level: config.consistencyLevel,
        retry_policy: config.retryPolicy,
        load_balancing_policy: config.loadBalancingPolicy
      }
    };

    try {
      console.log('Sending request to backend:', backendPayload);
      const response = await fetch('/api/execute-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(backendPayload),
      });

      const data = await response.json();
      console.log('Received response from backend:', data);

      if (!response.ok) {
        throw new Error(data.detail || data.error || 'Failed to execute query');
      }
      
      // Check response structure and log it for debugging
      if (data.columns) {
        console.log('Columns received:', data.columns);
      } else {
        console.log('No columns in response');
      }
      
      if (data.rows) {
        console.log('Rows received:', data.rows);
        console.log('Row count:', data.rows.length);
      } else {
        console.log('No rows in response');
      }
      
      setResults(data);
      toast({ 
        title: getSuccessMessage(),
        status: 'success', 
        duration: 3000, 
        isClosable: true 
      });
    } catch (error: any) {
      console.error('Error executing query:', error);
      const errorMessage = error.message || 'An unexpected error occurred.';
      setResults({ error: errorMessage });
      toast({ title: 'Error', description: errorMessage, status: 'error', duration: 5000, isClosable: true });
    }
    setIsLoading(false);
  };

  const getSuccessMessage = () => {
    switch (queryMode) {
      case 'execute': 
        return 'Query executed successfully!';
      case 'driver': 
        return 'Driver code generated successfully!';
      case 'natural_language': 
        return 'Natural language query processed successfully!';
      default: 
        return 'Operation completed successfully!';
    }
  };

  const getInputPlaceholder = () => {
    switch (queryMode) {
      case 'execute': 
        return 'Enter your CQL query here (e.g., SELECT * FROM table_name)';
      case 'driver': 
        return 'Enter your CQL query to generate driver code';
      case 'natural_language': 
        return 'Describe what you want to do in plain English (e.g., "Insert 10 rows", "Create a large partition with 1000 rows", or "Use lightweight transactions")';
      default: 
        return 'Enter your query here';
    }
  };

  const getButtonText = () => {
    switch (queryMode) {
      case 'execute': 
        return 'Execute Query';
      case 'driver': 
        return 'Generate Driver Code';
      case 'natural_language': 
        return 'Generate Code from Description';
      default: 
        return 'Submit';
    }
  };

  const renderResults = () => {
    if (results) {
      console.log('About to render results:', {
        hasColumns: Boolean(results.columns),
        columnData: results.columns,
        hasRows: Boolean(results.rows),
        rowCount: results.rows?.length,
        firstRow: results.rows && results.rows.length > 0 ? results.rows[0] : null,
        hasDriverCode: Boolean(results.driver_code),
        hasError: Boolean(results.error)
      });
    }
  };

  renderResults();

  return (
    <Box p={5} shadow="md" borderWidth="1px" borderRadius="md" w={"100%"} maxW={"800px"}>
      <Heading size="md" mb={4}>Driver Code Generator</Heading>
      <VStack spacing={4} align="stretch">
        <Textarea
          placeholder={getInputPlaceholder()}
          value={query}
          onChange={(e: ChangeEvent<HTMLTextAreaElement>) => setQuery(e.target.value)}
          rows={5}
        />
        
        <RadioGroup onChange={(value: string) => setQueryMode(value as 'execute' | 'driver' | 'natural_language')} value={queryMode}>
          <Stack direction="row" spacing={5} mb={3}>
            <Radio value="execute">Execute Query</Radio>
            <Radio value="driver">Generate Driver Code</Radio>
            <Radio value="natural_language">Natural Language</Radio>
          </Stack>
        </RadioGroup>
        
        <Button colorScheme="teal" onClick={handleSubmit} isLoading={isLoading}>
          {getButtonText()}
        </Button>
      </VStack>

      {isLoading && <CircularProgress isIndeterminate color="teal.300" my={4} />}

      {results && (
        <Box mt={6}>
          <Heading size="sm" mb={3}>Results</Heading>
          
          {/* Debug button to show raw response */}
          <Button 
            size="xs" 
            colorScheme="gray" 
            mb={2}
            onClick={() => {
              console.log('Raw results:', results);
              toast({
                title: 'Raw results logged to console',
                description: 'Check browser console for details',
                status: 'info',
                duration: 3000
              });
            }}
          >
            Debug: Show Raw Response
          </Button>
          
          {results.error && (
            <Code colorScheme="red" p={3} borderRadius="md">
              Error: {results.error}
            </Code>
          )}
          
          {results.execution_time !== undefined && (
            <Text fontSize="sm" color="gray.500" mb={2}>
              Execution time: {results.execution_time.toFixed(4)} ms
            </Text>
          )}
          
          {/* Display original natural language query if available */}
          {results.original_query && (
            <Box mt={2} mb={4}>
              <Text fontSize="sm" fontWeight="bold">Original request:</Text>
              <Code p={2} borderRadius="md" display="block" whiteSpace="pre-wrap">
                {results.original_query}
              </Code>
            </Box>
          )}
          
          {/* Display driver code if available */}
          {results.driver_code && (
            <Box mt={4}>
              <Heading size="sm" mb={2}>Driver Code</Heading>
              <Code p={3} borderRadius="md" display="block" whiteSpace="pre" overflowX="auto">
                {results.driver_code}
              </Code>
              {results.instructions && (
                <Text fontSize="sm" mt={2}>{results.instructions}</Text>
              )}
            </Box>
          )}
          
          {/* Display query results in a table if available */}
          {results.columns && results.rows && results.rows.length > 0 && (
            <TableContainer mt={4}>
              <Table variant="striped" size="sm">
                <Thead>
                  <Tr>
                    {results.columns.map((colName) => (
                      <Th key={colName}>{colName}</Th>
                    ))}
                  </Tr>
                </Thead>
                <Tbody>
                  {results.rows.map((row, rowIndex) => (
                    <Tr key={rowIndex}>
                      {results.columns?.map((colName) => {
                        const cellValue = row[colName];
                        return (
                          <Td key={colName}>
                            {typeof cellValue === 'object' 
                              ? JSON.stringify(cellValue) 
                              : cellValue !== null && cellValue !== undefined 
                                ? String(cellValue) 
                                : ''}
                          </Td>
                        );
                      })}
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            </TableContainer>
          )}
          {results.rows && results.rows.length === 0 && (
            <Text mt={2} fontStyle="italic">Query executed successfully, but no rows were returned.</Text>
          )}
        </Box>
      )}
    </Box>
  );
}

export default QueryInterface; 