import React, { useState } from 'react';
import {
  Box,
  Button,
  FormControl,
  FormLabel,
  Input,
  VStack,
  useToast,
  Heading,
  Text,
  RadioGroup,
  Stack,
  Radio,
  Select
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

interface ConnectionFormProps {
  onConnect: (config: ConnectionConfig) => void;
}

const ConnectionForm: React.FC<ConnectionFormProps> = ({ onConnect }) => {
  const [keyspace, setKeyspace] = useState('');
  const [token, setToken] = useState('');
  const [secureBundle, setSecureBundle] = useState<File | null>(null);
  const [connectionMethod, setConnectionMethod] = useState<'driver'>('driver');
  const [region, setRegion] = useState('');
  const [driverType, setDriverType] = useState<'python' | 'java'>('python');
  const [consistencyLevel, setConsistencyLevel] = useState('LOCAL_QUORUM');
  const [retryPolicy, setRetryPolicy] = useState('DEFAULT_RETRY_POLICY');
  const [loadBalancingPolicy, setLoadBalancingPolicy] = useState('TOKEN_AWARE');
  const [dbUUID, setDbUUID] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const toast = useToast();

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!secureBundle || !keyspace || !token || !region) {
      toast({
        title: 'Error',
        description: 'All fields (Secure Connect Bundle, Keyspace, Token, Region) are required.',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
      return;
    }
    if (!driverType) {
      toast({
        title: 'Error',
        description: 'Driver Type is required.',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
      return;
    }

    setIsLoading(true);
    let secureBundleBase64 = '';
    try {
      secureBundleBase64 = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.readAsDataURL(secureBundle);
        reader.onload = () => resolve((reader.result as string).split(',')[1]);
        reader.onerror = (error) => reject(error);
      });
    } catch (error) {
      toast({
        title: 'Error reading file',
        description: 'Could not read the secure connect bundle.',
        status: 'error',
        duration: 5000,
        isClosable: true,
      });
      setIsLoading(false);
      return;
    }
    
    const configToSet: ConnectionConfig = {
      secureConnectBundle: secureBundleBase64,
      keyspace,
      token,
      region,
      connectionMethod,
      driverType,
      consistencyLevel,
      retryPolicy,
      loadBalancingPolicy,
      dbUUID
    };

    onConnect(configToSet);
    setIsLoading(false);
     toast({
        title: 'Configuration Set',
        description: 'Connection details have been configured.',
        status: 'success',
        duration: 3000,
        isClosable: true,
      });
  };

  return (
    <Box p={5} shadow="md" borderWidth="1px" borderRadius="md" w={"100%"} maxW={"500px"}>
      <Heading size="md" mb={4}>Connect to Astra DB</Heading>
      <form onSubmit={handleSubmit}>
        <VStack spacing={4}>
          <FormControl isRequired>
            <FormLabel htmlFor="secureBundle">Secure Connect Bundle (.zip)</FormLabel>
            <Input
              type="file"
              id="secureBundle"
              accept=".zip"
              onChange={(e) => setSecureBundle(e.target.files ? e.target.files[0] : null)}
              p={1}
            />
          </FormControl>
          <FormControl isRequired>
            <FormLabel htmlFor="token">Astra DB Token</FormLabel>
            <Input
              id="token"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              placeholder="Enter your Astra DB Token"
            />
            <Text fontSize="xs" color="gray.500" mt={1}>
              Use your Astra DB token directly (without the AstraCS:clientId:clientSecret format).
              You can generate this token from the Astra DB dashboard.
            </Text>
          </FormControl>
          <FormControl isRequired>
            <FormLabel htmlFor="keyspace">Keyspace</FormLabel>
            <Input
              id="keyspace"
              value={keyspace}
              onChange={(e) => setKeyspace(e.target.value)}
              placeholder="e.g., my_keyspace"
            />
          </FormControl>
          
          <FormControl isRequired>
            <FormLabel htmlFor="region">Region</FormLabel>
            <Input
              id="region"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              placeholder="e.g., us-east-1"
            />
          </FormControl>
          
          <FormControl>
            <FormLabel htmlFor="dbUUID">Database UUID (Optional)</FormLabel>
            <Input
              id="dbUUID"
              value={dbUUID}
              onChange={(e) => setDbUUID(e.target.value)}
              placeholder="e.g., 3ba6cbeb-e33b-4c59-b1d4-45a8d8d2d8f4"
            />
          </FormControl>
          
          <FormControl isRequired>
            <FormLabel htmlFor="driverType">Driver Type</FormLabel>
            <RadioGroup onChange={setDriverType as any} value={driverType}>
              <Stack direction="row" spacing={5}>
                <Radio value="python">Python</Radio>
                <Radio value="java">Java</Radio>
              </Stack>
            </RadioGroup>
          </FormControl>
          
          <FormControl isRequired>
            <FormLabel htmlFor="consistencyLevel">Consistency Level</FormLabel>
            <Select 
              id="consistencyLevel"
              value={consistencyLevel}
              onChange={(e) => setConsistencyLevel(e.target.value)}
            >
              <option value="LOCAL_ONE">LOCAL_ONE</option>
              <option value="LOCAL_QUORUM">LOCAL_QUORUM</option>
              <option value="ALL">ALL</option>
              <option value="QUORUM">QUORUM</option>
              <option value="ONE">ONE</option>
            </Select>
            <Text fontSize="xs" color="gray.500" mt={1}>
              Controls how many replicas must respond for success
            </Text>
          </FormControl>
          
          <FormControl isRequired>
            <FormLabel htmlFor="retryPolicy">Retry Policy</FormLabel>
            <Select 
              id="retryPolicy"
              value={retryPolicy}
              onChange={(e) => setRetryPolicy(e.target.value)}
            >
              <option value="DEFAULT_RETRY_POLICY">Default</option>
              <option value="DOWNGRADING_CONSISTENCY_RETRY_POLICY">Downgrading Consistency</option>
              <option value="FALLTHROUGH_RETRY_POLICY">Fallthrough (No Retries)</option>
            </Select>
            <Text fontSize="xs" color="gray.500" mt={1}>
              Determines how the driver handles retriable errors
            </Text>
          </FormControl>
          
          <FormControl isRequired>
            <FormLabel htmlFor="loadBalancingPolicy">Load Balancing Policy</FormLabel>
            <Select 
              id="loadBalancingPolicy"
              value={loadBalancingPolicy}
              onChange={(e) => setLoadBalancingPolicy(e.target.value)}
            >
              <option value="TOKEN_AWARE">Token Aware</option>
              <option value="ROUND_ROBIN">Round Robin</option>
              <option value="DC_AWARE_ROUND_ROBIN">DC Aware Round Robin</option>
            </Select>
            <Text fontSize="xs" color="gray.500" mt={1}>
              Controls how requests are distributed among nodes
            </Text>
          </FormControl>
          
          <Button type="submit" colorScheme="blue" w="100%" isLoading={isLoading}>
            Connect
          </Button>
        </VStack>
      </form>
      <Text mt={4} fontSize="sm" color="gray.500">
        Ensure your Astra DB credentials are correct. 
        All fields are required for connection to Astra DB.
      </Text>
    </Box>
  );
};

export default ConnectionForm; 