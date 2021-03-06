/*
 * Minio Cloud Storage, (C) 2014-2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"net/url"
	"path"
	"sync"
	"time"
)

// localAdminClient - represents admin operation to be executed locally.
type localAdminClient struct {
}

// remoteAdminClient - represents admin operation to be executed
// remotely, via RPC.
type remoteAdminClient struct {
	*AuthRPCClient
}

// adminCmdRunner - abstracts local and remote execution of admin
// commands like service stop and service restart.
type adminCmdRunner interface {
	Stop() error
	Restart() error
	ListLocks(bucket, prefix string, relTime time.Duration) ([]VolumeLockInfo, error)
}

// Stop - Sends a message over channel to the go-routine responsible
// for stopping the process.
func (lc localAdminClient) Stop() error {
	globalServiceSignalCh <- serviceStop
	return nil
}

// Restart - Sends a message over channel to the go-routine
// responsible for restarting the process.
func (lc localAdminClient) Restart() error {
	globalServiceSignalCh <- serviceRestart
	return nil
}

// ListLocks - Fetches lock information from local lock instrumentation.
func (lc localAdminClient) ListLocks(bucket, prefix string, relTime time.Duration) ([]VolumeLockInfo, error) {
	return listLocksInfo(bucket, prefix, relTime), nil
}

// Stop - Sends stop command to remote server via RPC.
func (rc remoteAdminClient) Stop() error {
	args := AuthRPCArgs{}
	reply := AuthRPCReply{}
	return rc.Call("Admin.Shutdown", &args, &reply)
}

// Restart - Sends restart command to remote server via RPC.
func (rc remoteAdminClient) Restart() error {
	args := AuthRPCArgs{}
	reply := AuthRPCReply{}
	return rc.Call("Admin.Restart", &args, &reply)
}

// ListLocks - Sends list locks command to remote server via RPC.
func (rc remoteAdminClient) ListLocks(bucket, prefix string, relTime time.Duration) ([]VolumeLockInfo, error) {
	listArgs := ListLocksQuery{
		bucket:  bucket,
		prefix:  prefix,
		relTime: relTime,
	}
	var reply ListLocksReply
	if err := rc.Call("Admin.ListLocks", &listArgs, &reply); err != nil {
		return nil, err
	}
	return reply.volLocks, nil
}

// adminPeer - represents an entity that implements Stop and Restart methods.
type adminPeer struct {
	addr      string
	cmdRunner adminCmdRunner
}

// type alias for a collection of adminPeer.
type adminPeers []adminPeer

// makeAdminPeers - helper function to construct a collection of adminPeer.
func makeAdminPeers(eps []*url.URL) adminPeers {
	var servicePeers []adminPeer

	// map to store peers that are already added to ret
	seenAddr := make(map[string]bool)

	// add local (self) as peer in the array
	servicePeers = append(servicePeers, adminPeer{
		globalMinioAddr,
		localAdminClient{},
	})
	seenAddr[globalMinioAddr] = true

	serverCred := serverConfig.GetCredential()
	// iterate over endpoints to find new remote peers and add
	// them to ret.
	for _, ep := range eps {
		if ep.Host == "" {
			continue
		}

		// Check if the remote host has been added already
		if !seenAddr[ep.Host] {
			cfg := authConfig{
				accessKey:       serverCred.AccessKey,
				secretKey:       serverCred.SecretKey,
				serverAddr:      ep.Host,
				secureConn:      isSSL(),
				serviceEndpoint: path.Join(reservedBucket, adminPath),
				serviceName:     "Admin",
			}

			servicePeers = append(servicePeers, adminPeer{
				addr:      ep.Host,
				cmdRunner: &remoteAdminClient{newAuthRPCClient(cfg)},
			})
			seenAddr[ep.Host] = true
		}
	}

	return servicePeers
}

// Initialize global adminPeer collection.
func initGlobalAdminPeers(eps []*url.URL) {
	globalAdminPeers = makeAdminPeers(eps)
}

// invokeServiceCmd - Invoke Stop/Restart command.
func invokeServiceCmd(cp adminPeer, cmd serviceSignal) (err error) {
	switch cmd {
	case serviceStop:
		err = cp.cmdRunner.Stop()
	case serviceRestart:
		err = cp.cmdRunner.Restart()
	}
	return err
}

// sendServiceCmd - Invoke Stop/Restart command on remote peers
// adminPeer followed by on the local peer.
func sendServiceCmd(cps adminPeers, cmd serviceSignal) {
	// Send service command like stop or restart to all remote nodes and finally run on local node.
	errs := make([]error, len(cps))
	var wg sync.WaitGroup
	remotePeers := cps[1:]
	for i := range remotePeers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// we use idx+1 because remotePeers slice is 1 position shifted w.r.t cps
			errs[idx+1] = invokeServiceCmd(remotePeers[idx], cmd)
		}(i)
	}
	wg.Wait()
	errs[0] = invokeServiceCmd(cps[0], cmd)
}

func listPeerLocksInfo(peers adminPeers, bucket, prefix string, relTime time.Duration) ([]VolumeLockInfo, error) {
	// Used to aggregate volume lock information from all nodes.
	allLocks := make([][]VolumeLockInfo, len(peers))
	errs := make([]error, len(peers))
	var wg sync.WaitGroup
	localPeer := peers[0]
	remotePeers := peers[1:]
	for i, remotePeer := range remotePeers {
		wg.Add(1)
		go func(idx int, remotePeer adminPeer) {
			defer wg.Done()
			// `remotePeers` is right-shifted by one position relative to `peers`
			allLocks[idx], errs[idx] = remotePeer.cmdRunner.ListLocks(bucket, prefix, relTime)
		}(i+1, remotePeer)
	}
	wg.Wait()
	allLocks[0], errs[0] = localPeer.cmdRunner.ListLocks(bucket, prefix, relTime)

	// Summarizing errors received for ListLocks RPC across all
	// nodes.  N B the possible unavailability of quorum in errors
	// applies only to distributed setup.
	errCount, err := reduceErrs(errs, []error{})
	if err != nil {
		if errCount >= (len(peers)/2 + 1) {
			return nil, err
		}
		return nil, InsufficientReadQuorum{}
	}

	// Group lock information across nodes by (bucket, object)
	// pair. For readability only.
	paramLockMap := make(map[nsParam][]VolumeLockInfo)
	for _, nodeLocks := range allLocks {
		for _, lockInfo := range nodeLocks {
			param := nsParam{
				volume: lockInfo.Bucket,
				path:   lockInfo.Object,
			}
			paramLockMap[param] = append(paramLockMap[param], lockInfo)
		}
	}
	groupedLockInfos := []VolumeLockInfo{}
	for _, volLocks := range paramLockMap {
		groupedLockInfos = append(groupedLockInfos, volLocks...)
	}
	return groupedLockInfos, nil
}
