package org.otpstudy.distribution

class NoSuchNodeException(val nodeId: NodeId) : Exception("no connection to node $nodeId")
