/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

class Quorum {
private:
    int success;
    int failure;
    int txnId;
	Address * requester;
    MessageType type;
    string key;
    string value;
public:
    Quorum();
    Quorum(int txnId, MessageType type, Address * requester, string key, string value);
    Quorum& operator =(const Quorum &anotherQ);
    
    int getTotalVotes();
    bool isQuorumFailed();
    bool isQuorumSucceeded();
    void vote(bool _success);
    int getTxnId();
    string getKey();
    string getValue();
	void setValue(string value);
    MessageType getType();
	Address * getRequester();
    int getSuccess();
    int getFailure();
    string toString();
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	void sendClientMessage(MessageType type, int txnId, string key, string value);
	void runStabilizationProtocol(vector<Node> ring);

public:
	// map of transaction id to quorum
	map<int, Quorum> quorumMap;

	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// reply to client
	void replyToClient(Message message, Address requesterAddress, bool success);
	void readReplyToClient(Message msg, Address requesterAddress, string value);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica, int transID, Address requesterAddr);
	string readKey(string key, int transID, Address requesterAddr);
	bool updateKeyValue(string key, string value, ReplicaType replica, int transID, Address requesterAddr);
	bool deletekey(string key, int transID, Address requesterAddr);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	void checkQuorum();

	~MP2Node();
};

#endif /* MP2NODE_H_ */
