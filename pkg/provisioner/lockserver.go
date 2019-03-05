package provisioner

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/minio/dsync"
	"net"
	"net/http"
	"net/rpc"
	"path"
	"strconv"
	"sync"
)

const WriteLock = -1

type LockServer struct {
	mutex   sync.Mutex
	lockMap map[string]int64 // Map of locks, with negative value indicating (exclusive) write lock
	// and positive values indicating number of read locks
}

func (l *LockServer) Lock(args *dsync.LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if _, *reply = l.lockMap[args.Resource]; !*reply {
		l.lockMap[args.Resource] = WriteLock // No locks held on the given name, so claim write lock
	}
	*reply = !*reply // Negate *reply to return true when lock is granted or false otherwise
	return nil
}

func (l *LockServer) Unlock(args *dsync.LockArgs, reply *bool) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	var locksHeld int64
	if locksHeld, *reply = l.lockMap[args.Resource]; !*reply { // No lock is held on the given name
		return fmt.Errorf("unlock attempted on an unlocked entity: %s", args.Resource)
	}
	if *reply = locksHeld == WriteLock; !*reply { // Unless it is a write lock
		return fmt.Errorf("unlock attempted on a read locked entity: %s (%d read locks active)", args.Resource, locksHeld)
	}
	delete(l.lockMap, args.Resource) // Remove the write lock
	return nil
}

func StartLockServer(port int, rpcEndpoint string) {
	lockServer := &LockServer{
		mutex:   sync.Mutex{},
		lockMap: make(map[string]int64),
	}

	portString := strconv.Itoa(port)

	rpcServer := rpc.NewServer()
	err := rpcServer.RegisterName("LockServer", lockServer)
	if err != nil {
		log.Fatalf("unable to register name: %v", err)
	}
	rpcServer.HandleHTTP(rpcEndpoint, path.Join(rpcEndpoint, "_zfsProvisioner"))

	listener, err := net.Listen("tcp", ":"+portString)
	if err == nil {
		log.Infof("LockServer listening at port: %v under path: %v", port, rpcEndpoint)
		http.Serve(listener, nil)
		// It never returns so error handling only happens if something goes wrong
	}

	log.Errorf("Unable to start LockServer on port %v under path %v", port, rpcEndpoint)
	log.Fatal("error:", err)
}
