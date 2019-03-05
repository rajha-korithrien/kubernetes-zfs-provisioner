package provisioner

import (
	"net/rpc"
	"sync"

	"github.com/minio/dsync"
)

// ReconnectRPCClient is a wrapper type for rpc.Client which provides reconnect on first failure.
type ReconnectRPCClient struct {
	mutex    sync.Mutex
	rpc      *rpc.Client
	addr     string
	endpoint string
}

// newClient constructs a ReconnectRPCClient object with addr and endpoint initialized.
// It _doesn't_ connect to the remote endpoint. See Call method to see when the
// connect happens.
func newClient(addr, endpoint string) dsync.NetLocker {
	return &ReconnectRPCClient{
		addr:     addr,
		endpoint: endpoint,
	}
}

// Close closes the underlying socket file descriptor.
func (rpcClient *ReconnectRPCClient) Close() error {
	rpcClient.mutex.Lock()
	defer rpcClient.mutex.Unlock()
	// If rpc client has not connected yet there is nothing to close.
	if rpcClient.rpc == nil {
		return nil
	}
	// Reset rpcClient.rpc to allow for subsequent calls to use a new
	// (socket) connection.
	clnt := rpcClient.rpc
	rpcClient.rpc = nil
	return clnt.Close()
}

// Call makes a RPC call to the remote endpoint using the default codec, namely encoding/gob.
func (rpcClient *ReconnectRPCClient) Call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	rpcClient.mutex.Lock()
	defer rpcClient.mutex.Unlock()
	dialCall := func() error {
		// If the rpc.Client is nil, we attempt to (re)connect with the remote endpoint.
		if rpcClient.rpc == nil {
			clnt, derr := rpc.DialHTTPPath("tcp", rpcClient.addr, rpcClient.endpoint)
			if derr != nil {
				return derr
			}
			rpcClient.rpc = clnt
		}
		// If the RPC fails due to a network-related error, then we reset
		// rpc.Client for a subsequent reconnect.
		return rpcClient.rpc.Call(serviceMethod, args, reply)
	}
	if err = dialCall(); err == rpc.ErrShutdown {
		rpcClient.rpc.Close()
		rpcClient.rpc = nil
		err = dialCall()
	}
	return err
}

func (rpcClient *ReconnectRPCClient) RLock(args dsync.LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.RLock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Lock(args dsync.LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.Lock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) RUnlock(args dsync.LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.RUnlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) Unlock(args dsync.LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.Unlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) ForceUnlock(args dsync.LockArgs) (status bool, err error) {
	err = rpcClient.Call("Dsync.ForceUnlock", &args, &status)
	return status, err
}

func (rpcClient *ReconnectRPCClient) ServerAddr() string {
	return rpcClient.addr
}

func (rpcClient *ReconnectRPCClient) ServiceEndpoint() string {
	return rpcClient.endpoint
}
