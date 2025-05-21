import React, { useState } from 'react';
import './App.css';
import ConnectionForm from './components/ConnectionForm';
import QueryInterface from './components/QueryInterface';
import { ChakraProvider, Box, Heading } from '@chakra-ui/react';

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

const App: React.FC = () => {
  const [connectionConfig, setConnectionConfig] = useState<ConnectionConfig | null>(null);

  return (
    <ChakraProvider>
      <Box p={5}>
        <Heading mb={4}>Driver Code Generator</Heading>
        {!connectionConfig ? (
          <ConnectionForm onConnect={setConnectionConfig} />
        ) : (
          <QueryInterface config={connectionConfig} />
        )}
      </Box>
    </ChakraProvider>
  );
};

export default App; 