/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	if(ring.size() != curMemList.size()){
		change = true;
	}else if(ring.size() != 0){
		for(int i = 0; i < ring.size(); i++){
			if(curMemList[i].getHashCode() != ring[i].getHashCode()){
				change = true;
				break;
			}
		}
	}
	
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

	ring = curMemList;

	if (change){
		stabilizationProtocol(); // run stability protocol for curMemList
	}
}


/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.push_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {

	g_transID++;

	if (quorumMap.find(g_transID) == quorumMap.end()) {
		quorumMap.emplace(g_transID, Quorum(g_transID, CREATE, &memberNode->addr, key, value));
	}
	
	sendClientMessage(CREATE, g_transID, key, value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	
	g_transID++;

	if (quorumMap.find(g_transID) == quorumMap.end()) {
		quorumMap.emplace(g_transID, Quorum(g_transID, READ, &memberNode->addr, key,  ""));
	}

	sendClientMessage(READ, g_transID, key, "");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	g_transID++;
	if (quorumMap.find(g_transID) == quorumMap.end()) {
		quorumMap.emplace(g_transID, Quorum(g_transID, UPDATE, &memberNode->addr, key, value));
	}

	sendClientMessage(UPDATE, g_transID, key, value);
}


/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	g_transID++;

	if (quorumMap.find(g_transID) == quorumMap.end()) {
		quorumMap.emplace(g_transID, Quorum(g_transID, DELETE, &memberNode->addr, key, ""));
	}
	sendClientMessage(DELETE, g_transID, key, "");
}


void MP2Node::sendClientMessage(MessageType type, int txnId, string key, string value){

	// construct the message based on type
	Message* msg;

	vector<Node> replicas = findNodes(key);

	// we require replica type set in message for create and update
	bool requiresReplicaType = type == CREATE || type == UPDATE;

	switch (type) {
		case CREATE:
			msg = new Message(txnId, memberNode->addr, CREATE, key, value);
			break;
		case READ:
			msg = new Message(txnId, memberNode->addr, READ, key);
			break;
		case UPDATE:
			msg = new Message(txnId, memberNode->addr, UPDATE, key, value);
			break;
		case DELETE:
			msg = new Message(txnId, memberNode->addr, DELETE, key);
			break;
		default:
			break;
	}
	// find the replicas of this key
	// send a message to the replicas
	for (int i=0; i<replicas.size(); i++){
		
		if (requiresReplicaType) msg->replica = ReplicaType(i);
		emulNet->ENsend(&memberNode->addr, replicas.at(i).getAddress(), msg->toString());
	
	}

	free(msg);
}

void MP2Node::replyToClient(Message msg, Address requesterAddress, bool success){
	if ((msg.type == CREATE || msg.type == DELETE) && msg.transID == -1) return;
	Message* reply;
	if (msg.type == CREATE || msg.type == UPDATE || msg.type == DELETE) {
		reply = new Message(msg.transID, memberNode->addr, REPLY, success);
		emulNet->ENsend(&memberNode->addr, &requesterAddress, reply->toString());
	}
	free(reply);
}

void MP2Node::readReplyToClient(Message msg, Address requesterAddress, string value){
	Message* reply;
	if (msg.type == READ) {
		reply = new Message(msg.transID, memberNode->addr, value);
		emulNet->ENsend(&memberNode->addr, &requesterAddress, reply->toString());
	}
	free(reply);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transID, Address requesterAddr) {
	
	// Insert key, value, replicaType into the hash table
	
	if (transID != -1) {
		bool success = ht->create(key, value);
		if (success) {
			log->logCreateSuccess(&requesterAddr, false, transID, key, value);
		} else {
			log->logCreateFail(&requesterAddr, false, transID, key, value);
		}

		return success;
	} else {
		string content = this->ht->read(key);
		bool exist = (content != "");
		if(!exist){
			return this->ht->create(key, value);	
		}
	}

	return false;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transID, Address requesterAddr) {
	// Read key from local hash table and return value
	string value = ht->read(key);
	if (value != "") {
		log->logReadSuccess(&requesterAddr, false, g_transID, key, value);
	} else {
		log->logReadFail(&requesterAddr, false, g_transID, key);
	}
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transID, Address requesterAddr) {
	// Update key in local hash table and return true or false
	bool success = ht->update(key, value);
	if (success) {
		log->logUpdateSuccess(&requesterAddr, false, transID, key, value);
	} else {
		log->logUpdateFail(&requesterAddr, false, transID, key, value);
	}
	return success;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transID, Address requesterAddr) {
	// Delete the key from the local hash table
	bool success = ht->deleteKey(key);
	if (success) {
		log->logDeleteSuccess(&requesterAddr, false, g_transID, key);
	} else {
		log->logDeleteFail(&requesterAddr, false, g_transID, key);
	}
	return success;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:	
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them

	Message* msg;

	map<int, Quorum>::iterator iter;
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

		msg = new Message(message);

		switch (msg->type) {
			case CREATE:
				replyToClient(*msg, msg->fromAddr, createKeyValue(msg->key, msg->value, msg->replica, msg->transID, msg->fromAddr));
				break;
			case READ:
				readReplyToClient(*msg, msg->fromAddr, readKey(msg->key, msg->transID, msg->fromAddr));
				break;
			case UPDATE:
				replyToClient(*msg, msg->fromAddr, updateKeyValue(msg->key, msg->value, msg->replica, msg->transID, msg->fromAddr));
				break;
			case DELETE:
				replyToClient(*msg, msg->fromAddr, deletekey(msg->key, msg->transID, msg->fromAddr));
				break;
			case REPLY:
				iter = quorumMap.find(msg->transID);
				iter->second.vote(msg->success);
				break;
			case READREPLY:
				iter = quorumMap.find(msg->transID);
				iter->second.setValue(msg->value);
				iter->second.vote(msg->value != "");
				break;
			default:
				break;
		}

		free(msg);
	}

	/*
	* This function should also ensure all READ and UPDATE operation
	* get QUORUM replies
	*/

	checkQuorum();
}

void MP2Node::checkQuorum() {

	// loop through the quorum map and check if any of the quorum has reached quorum
		for (map<int, Quorum>::iterator it = quorumMap.begin(); it != quorumMap.end();) {
			if (it->second.isQuorumSucceeded()) {
				switch(it->second.getType()) {
					case READ:
						log->logReadSuccess(it->second.getRequester(), true, it->first, it->second.getKey(), it->second.getValue());
						break;
					case UPDATE:
						log->logUpdateSuccess(it->second.getRequester(), true, it->first, it->second.getKey(), it->second.getValue());
						break;
					case DELETE:
						log->logDeleteSuccess(it->second.getRequester(), true, it->first, it->second.getKey());
						break;
					case CREATE:
						log->logCreateSuccess(it->second.getRequester(), true, it->first, it->second.getKey(), it->second.getValue());
						break;
					default:
						break;
				}
				it = quorumMap.erase(it++);
			}
			else if (it->second.isQuorumFailed()) {
				switch(it->second.getType()) {
					case READ:
						log->logReadFail(it->second.getRequester(), true, it->first, it->second.getKey());
						break;
					case UPDATE:
						log->logUpdateFail(it->second.getRequester(), true, it->first, it->second.getKey(), it->second.getValue());
						break;
					case DELETE:
						log->logDeleteFail(it->second.getRequester(), true, it->first, it->second.getKey());
						break;
					case CREATE:
						log->logCreateFail(it->second.getRequester(), true, it->first, it->second.getKey(), it->second.getValue());
						break;
					default:
						break;
				}

				it = quorumMap.erase(it++);
			}
			else {
				++it;
			}
		}
}


/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	map<string, string>::iterator it;

	for(it = this->ht->hashTable.begin(); it != this->ht->hashTable.end(); it++) {
		string key = it->first;
		string value = it->second;
		vector<Node> replicas = findNodes(key);

		Message createMsg(-1, this->memberNode->addr, CREATE, key, value);

		for (int i = 0; i < replicas.size(); i++) {
			emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), createMsg.toString());
		}

		for (map<int, Quorum>::iterator it = quorumMap.begin(); it != quorumMap.end(); it++) {

			if (it->second.getKey() == key) {
				Message transactionMessage(it->second.getTxnId(), memberNode->addr, it->second.getType(), key, value);

				for (int i = 0; i < replicas.size(); i++) {
					emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), transactionMessage.toString());
				}
			}
		}
	}
}

Quorum::Quorum() {
    this->txnId = 0;
    this->type = MessageType::CREATE;
    this->key = "";
    this->value = "";
    this->success = 0;
    this->failure = 0;
}

Quorum::Quorum(int txnId, MessageType type, Address * requester, string key, string value) {
    this->txnId = txnId;
    this->type = type;
	this->requester = requester;
    this->key = key;
    this->value = value;
    this->success = 0;
    this->failure = 0;
}

/**
 * Assignment operator overloading
 */
Quorum& Quorum::operator =(const Quorum &anotherQ) {
    this->txnId = anotherQ.txnId;
    this->type = anotherQ.type;
    this->key = anotherQ.key;
    this->value = anotherQ.value;
    this->success = anotherQ.success;
    this->failure = anotherQ.failure;
    return *this;
}

int Quorum::getTxnId() {
    return this->txnId;
}

void Quorum::vote(bool _success) {
    if (_success) {
        this->success += 1;
    } else {
        this->failure += 1;
    }
}

int Quorum::getTotalVotes() {
    return this->success + this->failure;
}

bool Quorum::isQuorumFailed() {
    return (this->getTotalVotes() >= 2 && this->failure > this->success) || this->getTotalVotes() == 1;
}

bool Quorum::isQuorumSucceeded() {
	return this->getTotalVotes() >= 2 && this->success > this->failure;
}

int Quorum::getSuccess() {
    return this->success;
}

int Quorum::getFailure() {
    return this->failure;
}

string Quorum::getKey() {
    return this->key;
}

string Quorum::getValue() {
    return this->value;
}

MessageType Quorum::getType() {
    return this->type;
}

Address * Quorum::getRequester() {
	return this->requester;
}

void Quorum::setValue(string value) {
	this->value = value;
}

string Quorum::toString() {
    return "TxnId: " + to_string(this->txnId) + " Key: " + this->key + " Value: " + this->value + " Success: " + to_string(this->success) + " Failure: " + to_string(this->failure);
}