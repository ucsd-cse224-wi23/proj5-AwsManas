package surfstore

import (
	context "context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	serverId      int64
	metaStore     *MetaStore
	commitIndex   int64
	lastApplied   int64
	votedFor      int64
	config        RaftConfig
	/*--------------- For Leader --------------*/
	nextindex  map[string]int64
	matchIndex map[string]int64
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	crashed := s.isCrashed
	fmt.Println("In GetFileInfoMap : serverID : ", s.serverId, "Leader , Crashed : ", leader, crashed)
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RUnlock()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	for {
		suc, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		fmt.Println("getFileInfoMap : got suc as ", suc.Flag)
		if err != nil {
			fmt.Println(err.Error())
		} else if suc.Flag {
			break
		}
	}
	t, err := s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	fmt.Println("Executing GetFileInfoMap  got :", t.FileInfoMap)
	return t, err
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	crashed := s.isCrashed
	fmt.Println("In GetBlockStoreMap : serverID : ", s.serverId, "Leader , Crashed : ", leader, crashed)
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RUnlock()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	for {
		suc, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		fmt.Println("getBlockStoreMap : got suc as ", suc.Flag)
		if suc.Flag {
			break
		}
	}

	t, err := s.metaStore.GetBlockStoreMap(ctx, hashes)
	fmt.Println("Executing GetBlockStoreMap  got :", t.BlockStoreMap)
	return t, err
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	crashed := s.isCrashed
	fmt.Println("In GetBlockStoreAddrs : serverID : ", s.serverId, "Leader , Crashed : ", leader, crashed)
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RUnlock()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !leader {
		return nil, ERR_NOT_LEADER
	}

	for {
		suc, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		fmt.Println("getBlockStoreAddrs : got suc as ", suc.Flag)
		if suc.Flag {
			break
		}
	}
	t, err := s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	fmt.Println("Executing GetBlockStoreAddrs  got :", t.BlockStoreAddrs)
	return t, err
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	crashed := s.isCrashed
	fmt.Println("In UpdateFile : serverID : ", s.serverId, "Leader , Crashed : ", leader, crashed)
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RUnlock()
	if crashed {
		return nil, ERR_SERVER_CRASHED
	}
	if !leader {
		return nil,
			ERR_NOT_LEADER
	}
	var tmp2 UpdateOperation
	tmp2.FileMetaData = filemeta
	tmp2.Term = s.term
	s.log = append(s.log, &tmp2)
	fmt.Println("In UpdateFile , updated log is  : serverID : ", s.serverId, s.log)
	fmt.Println("In UpdateFile , updated log lenght  for serverID : is ", s.serverId, len(s.log))
	cnt := 1
	servedServers := make([]int, len(s.config.RaftAddrs))
	logIndexMinus := make([]int, len(s.config.RaftAddrs))
	tmp := make(chan *AppendEntryOutput)
	servedServers[int(s.serverId)] = 1

	//for cnt <= len(s.config.RaftAddrs)/2 {

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	resp := 0
	for i := range s.config.RaftAddrs {
		if servedServers[i] == 2 {
			logIndexMinus[i] += 1
		}
		if i == int(s.serverId) || servedServers[i] == 1 {
			continue
		} else {
			if s.commitIndex == -1 {
				fmt.Println("logIndexMinus : ", i, logIndexMinus[i])
				data := &AppendEntryInput{
					Entries:      s.log,
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  0,
					LeaderCommit: s.commitIndex,
				}
				resp++
				go append_client(data, s, s.config.RaftAddrs[i], tmp)
			} else {
				fmt.Println("logIndexMinus : ", i, logIndexMinus[i])
				data := &AppendEntryInput{
					Entries:      s.log,
					Term:         s.term,
					PrevLogIndex: s.commitIndex,
					PrevLogTerm:  s.log[s.commitIndex].Term,
					LeaderCommit: s.commitIndex,
				}
				resp++
				go append_client(data, s, s.config.RaftAddrs[i], tmp)
			}

		}
	}

	for i := 0; i < resp; i++ {
		ret := <-tmp
		if ret.Success {
			fmt.Println("UpdateFile - Update sucessfull for : ", ret.ServerId, "in ", s.serverId)
			servedServers[ret.ServerId] = 1
			cnt += 1
		} else {
			if ret.ServerId == -1 {
				fmt.Println("Cant contact some server , continue ")
				continue
			}
			if ret.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				return nil, ERR_NOT_LEADER
			} else {
				servedServers[ret.ServerId] = 2
			}
		}
	}
	//}
	if cnt >= len(s.config.RaftAddrs)/2 {
		s.commitIndex += 1
		s.lastApplied += 1
		fmt.Println("Got majority , applying operation is local ")
		res, err := s.metaStore.UpdateFile(ctx, filemeta)
		return res, err
	} else {
		return nil, ERR_SERVER_CRASHED
	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.Lock()

	fmt.Println("IAM", s.serverId, "Reacieved a appendEntry call with values : Term , LeaderCommit , PrevLogidx , len of entries :  ", input.Term, input.LeaderCommit, input.PrevLogIndex, len(input.Entries))
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		s.isLeaderMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}

	//1
	if input.Term < s.term {
		fmt.Println("IAM ", s.serverId, " Returning false in appendEntry due to case 1")
		var out AppendEntryOutput
		out.ServerId = s.serverId
		out.Success = false
		out.Term = s.term
		out.MatchedIndex = 0
		s.isCrashedMutex.RUnlock()
		s.isLeaderMutex.Unlock()
		return &out, nil
	}

	//2
	// if int64(len(s.log)) > input.PrevLogIndex && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
	// if len(s.log) != 0 && int64(len(s.log))-1 < input.PrevLogIndex {
	// 	fmt.Println("IAM ", s.serverId, " Returning false in appendEntry due to case 2")
	// 	var out AppendEntryOutput
	// 	out.ServerId = s.serverId
	// 	out.Success = false
	// 	out.Term = s.term
	// 	out.MatchedIndex = 0
	// 	return &out, nil
	// }

	//2
	// if input.PrevLogIndex != -1 {
	// 	if len(s.log) <= int(input.PrevLogIndex) {
	// 		fmt.Println("IAM ", s.serverId, " Returning false in appendEntry due to my log is smaller than prevLogIndex")
	// 		var out AppendEntryOutput
	// 		out.ServerId = s.serverId
	// 		out.Success = false
	// 		out.Term = s.term
	// 		out.MatchedIndex = 0
	// 		return &out, nil
	// 	}
	// }

	s.isLeader = false
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Unlock()
	s.term = input.Term

	//3
	// if len(s.log) > 0 && input.PrevLogIndex == -1 {
	// 	s.log = make([]*UpdateOperation, 0)
	// } else if len(s.log) > 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
	// 	s.log = s.log[0:input.PrevLogIndex]
	// }

	//4

	// for idx := max(int(s.lastApplied), 0); int(idx) < len(s.log); idx++ {
	// 	if tmp < len(entries) && s.log[idx] == entries[tmp] {
	// 		tmp = tmp + 1
	// 	}
	// }

	s.log = input.Entries

	// 5
	runUpdate := false
	prevCommit := s.commitIndex
	if input.LeaderCommit > s.commitIndex {
		min := input.LeaderCommit
		if min > int64(len(s.log)-1) {
			min = int64(len(s.log) - 1)
		}
		s.commitIndex = min
		runUpdate = true
	}

	for ss := int(prevCommit) + 1; ss <= int(s.commitIndex) && runUpdate; ss++ {
		fmt.Println("Applying updatefile for server ", s.serverId)
		s.metaStore.UpdateFile(ctx, s.log[ss].FileMetaData)
		s.lastApplied = int64(ss)
	}

	fmt.Println("IAM ", s.serverId, " Before returning sucess here's my values : len(log), commit_idx , term ", len(s.log), s.commitIndex, s.term)
	var out AppendEntryOutput
	out.Success = true
	out.Term = s.term
	out.ServerId = s.serverId
	out.MatchedIndex = int64(len(s.log)) - 1
	return &out, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		s.isLeaderMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeader = true
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Unlock()
	s.term += 1
	return s.SendHeartbeat(ctx, &emptypb.Empty{})
}
func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	s.isLeaderMutex.RLock()
	leader := s.isLeader
	crashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.RUnlock()

	if crashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !leader {
		return nil, ERR_NOT_LEADER
	}
	fmt.Println("Sending heartbeat from ", s.serverId)

	tmp := make(chan *AppendEntryOutput)
	for i := range s.config.RaftAddrs {
		if i == int(s.serverId) {
			continue
		} else {
			if s.commitIndex == -1 {
				data := &AppendEntryInput{
					Entries:      s.log,
					Term:         s.term,
					PrevLogIndex: -1,
					PrevLogTerm:  0,
					LeaderCommit: s.commitIndex,
				}
				fmt.Println("Sending This to follower : ", *data)
				go append_client(data, s, s.config.RaftAddrs[i], tmp)
			} else {
				data := &AppendEntryInput{
					Entries:      s.log,
					Term:         s.term,
					PrevLogIndex: s.commitIndex,
					PrevLogTerm:  s.log[s.commitIndex].Term,
					LeaderCommit: s.commitIndex,
				}
				fmt.Println("Sending This to follower : ", *data)
				go append_client(data, s, s.config.RaftAddrs[i], tmp)
			}

		}
	}

	suc := false
	cnt := 1
	for i := 0; i < len(s.config.RaftAddrs)-1; i++ {
		ret := <-tmp
		if ret.Success {
			fmt.Println("Got sucess heartbeat response from  : ", ret.ServerId)
			cnt += 1
		} else {
			fmt.Println("Got a negetive herbeat response from : ", ret.ServerId)

			if ret.ServerId != -1 && ret.Term > s.term { // i was not a leader
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				fmt.Println("term of this server greater than mine, i am not leader : ", ret.ServerId)
				return nil, ERR_NOT_LEADER
			}
		}
	}

	fmt.Println("Finished sending heartbeat - got ", cnt, "responses")
	if cnt > len(s.config.RaftAddrs)/2 {
		suc = true
	}

	return &Success{Flag: suc}, nil
}

func append_client(data_to_send *AppendEntryInput, s *RaftSurfstore, address_to_send string, tmp chan *AppendEntryOutput) {

	appendEntryOutputError := AppendEntryOutput{
		Success:  false,
		ServerId: -1,
	}
	conn, err := grpc.Dial(address_to_send, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Got error while dialing grpc call : ", err.Error())
		tmp <- &appendEntryOutputError
		return
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	appendEntryOutput2, err := c.AppendEntries(ctx, data_to_send)

	if err != nil {
		tmp <- &appendEntryOutputError
		conn.Close()
		return
	}

	tmp <- appendEntryOutput2
	conn.Close()
	return
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
