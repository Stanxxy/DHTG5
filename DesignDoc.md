# The design document for DHT
## Requirements
### Functional Requirements
* User can choose which version of DHT
* User can use unified interface to CRUD
* User can retrieve existing node list
* User can retrieve the metadata of stored files 
* User can add nodes with an ip address(identifier)
* User can remove nodes with an ip address
* Provide a command line menu
### Non functional Requirements
* Move data objects when new node added
* Move data objects when existing node removed
* Maintain complete metadata of node on each node
### Extended Requirements (TODO)
* Load balancing ( automatically and manuly )

## Abstraction
Here we assume the document that needs to be saved is string.
### Interfaces
* BasicDHT
  - String insert(String key, String value)
  - String select(String key)
  - String update(String key, String value)
  - String delete(String key)
  - List\<DataObjPair\> listALlData()
* AdvancedDHT extands BasicDHT
  - List\<String\> batchInsert(List\<DataObjPair\> dataObjList)
  - List\<DataObjPair\> batchSelect(Set\<String\> keySet)
  - List\<String\> batchUpdate(List\<DataObjPair\> dataObjList)
  - List\<String\> batchDelete(List\<DataObjPair\> dataObjList)
* NodeManager
  - List\<Node\> listAllNodes()
  - String addNode(Node node)
  - String addNode(String ip)
  - String removeNode(Node node)
  - String removeNode(String ip)
  - boolean loadBalancing() -- to manually do load balancing.
  - boolean autoLB() -- automize load balancing. based on loadBalancing method.
### Implementation
#### Utils
* Printer
  - Attributes
  - Methods
    - void NodePrinter(List\<Node\> nodes)
    - void DataPrinter(List\<Node\> dataObjs)
#### Common
* DHTFactory
  - Attributes
    - DHTList BasicDHT
  - Methods
    - Object Factory(String DHTType) -- our dynamic factory.
    - ...
* DataObjPair
  - Attributes
    - String Key
    - String Value
  - Methods
    - ...
* Node 
  - Attributes
    - String ip
    - HashTable\<String, \DataObjPair> storedData
    - HashTable<String, Node> globalNodeTable
  - Methods
    - ...
#### Cassandra
* CNode extends Node implements NodeManager -- every node is a master.
  - Attrubutes
    - Successor Node -- Cassandra only stores the successor. 
    - ...
  - Methods
    - ...
* CDHT implements BasicDHT
  - Attributes
    - Long hashSpace
  - Methods
    - Long Hash()
    - ...
#### Ceph
* 
[Put your own design here]
## Architecture 
[Better put a pic here.]