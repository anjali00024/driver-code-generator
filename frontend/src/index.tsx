import React from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import App from './AppComponent';
import { ChakraProvider } from '@chakra-ui/react';

const container = document.getElementById('root');
if (container) {
  const root = createRoot(container);
  root.render(
    <React.StrictMode>
      <ChakraProvider>
        <App />
      </ChakraProvider>
    </React.StrictMode>
  );
} else {
  console.error('Failed to find the root element');
} 