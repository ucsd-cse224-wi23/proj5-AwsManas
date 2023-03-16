package surfstore

import (
	context "context"
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

	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	panic("todo")
	return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	panic("todo")
	return nil, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	panic("todo")
	return nil, nil
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
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		s.isLeaderMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}

	//1
	if input.Term < s.term {
		var out AppendEntryOutput
		out.ServerId = s.serverId
		out.Success = false
		out.Term = s.term
		return &out, nil
	}

	//2
	// if int64(len(s.log)) > input.PrevLogIndex && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
	if int64(len(s.log)) < input.PrevLogIndex {
		var out AppendEntryOutput
		out.ServerId = s.serverId
		out.Success = false
		out.Term = s.term
		return &out, nil
	}

	s.isLeader = false
	s.isCrashedMutex.RUnlock()
	s.isLeaderMutex.Unlock()

	//3
	if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		s.log = s.log[0:input.PrevLogIndex]
	}

	//4
	entries := input.Entries
	tmp := 0
	for idx := s.lastApplied + 1; int(idx) < len(s.log); idx++ {
		if s.log[idx] == entries[tmp] {
			tmp = tmp + 1
		}
	}

	for tmp < len(entries) {
		s.log = append(s.log, entries[tmp])
		tmp = tmp + 1
	}

	// 5
	prevCommit := s.commitIndex
	if input.LeaderCommit > s.commitIndex {
		min := input.LeaderCommit
		if min > int64(len(s.log)-1) {
			min = int64(len(s.log) - 1)
		}
		s.commitIndex = min
	}

	for ss := prevCommit + 1; ss <= s.commitIndex; ss++ {
		s.metaStore.UpdateFile(ctx, s.log[ss].FileMetaData)
		s.lastApplied = ss
	}

	var out AppendEntryOutput
	out.Success = true
	out.Term = s.term
	out.ServerId = s.serverId
	out.MatchedIndex = int64(len(s.log))
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

	s.term = s.term + 1
	return s.SendHeartbeat(ctx, &emptypb.Empty{})
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

	tmp := make(chan AppendEntryOutput)
	for i := range s.config.RaftAddrs {
		if i == int(s.serverId) {
			continue
		} else {
			data := &AppendEntryInput{
				Entries:      make([]*UpdateOperation, 0),
				Term:         s.term,
				PrevLogIndex: int64(len(s.log) - 1),
				PrevLogTerm:  s.log[len(s.log)-1].Term,
				LeaderCommit: s.commitIndex,
			}

			go append_client(data, s, s.config.RaftAddrs[i], tmp)
		}
	}

	suc := false
	cnt := 1
	for i := 0; i < len(s.config.RaftAddrs)-1; i++ {
		ret := <-tmp
		if ret.Success {
			cnt += 1
		}
	}

	if cnt > len(s.config.RaftAddrs)/2 {
		suc = true
	}

	return &Success{Flag: suc}, nil
}

func append_client(data_to_send *AppendEntryInput, s *RaftSurfstore, address_to_send string, tmp chan AppendEntryOutput) {
	conn, err := grpc.Dial(address_to_send, grpc.WithInsecure())
	//var appendEntryOutput AppendEntryOutput
	appendEntryOutput := AppendEntryOutput{
		Success: false,
	}
	if err != nil {
		tmp <- appendEntryOutput
		return
	}

	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	appendEntryOutput2, err := c.AppendEntries(ctx, data_to_send)

	if err != nil {
		tmp <- appendEntryOutput
		conn.Close()
		return
	}

	tmp <- *appendEntryOutput2
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
