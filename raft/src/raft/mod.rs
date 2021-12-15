use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::{select, FutureExt, StreamExt};
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}
// naokilh's raft lab test

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

/// raft node state
#[derive(Debug, PartialEq, Clone)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// RpcEvent
#[derive(Debug)]
pub enum Event {
    RequestVoteReply {
        reply: RequestVoteReply,
    },
    AppendEntriesReply {
        reply: AppendEntriesReply,
        is_heart: bool,
        new_next_index: usize,
    },
    ResetTimeout,
    Timeout,
    HeartBeat,
    Shutdown,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    current_term: u64,

    vote_for: Option<u64>,
    //Candidate
    vote_from: HashSet<u64>,
    //Leader
    next_index: Vec<usize>,
    match_index: Vec<usize>,
    log: Vec<Entry>,

    commit_index: u64,

    last_applied: u64,

    apply_tx: UnboundedSender<ApplyMsg>,
    event_loop_tx: Option<UnboundedSender<Event>>,

    executor: ThreadPool,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            role: Role::Follower,
            current_term: 0,
            vote_for: None,
            vote_from: HashSet::new(),
            log: vec![],
            next_index: vec![],
            match_index: vec![],
            commit_index: 0,
            last_applied: 0,
            apply_tx: apply_ch,
            event_loop_tx: None,
            executor: ThreadPool::new().unwrap(),
        };
        rf.log.push(Entry {
            term: 0,
            data: vec![],
        });
        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }
    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }
    fn trans_role(&mut self, role: Role) {
        self.role = role;
        self.vote_for = None;
        self.event_loop_tx
            .as_ref()
            .unwrap()
            .unbounded_send(Event::ResetTimeout)
            .unwrap();

        match self.role {
            Role::Candidate => {
                self.current_term += 1;
                self.vote_from = HashSet::new();
                self.hold_election();
            }
            Role::Leader => {
                self.next_index = vec![self.log.len(); self.peers.len()];
                self.match_index = vec![0; self.peers.len()];
                self.send_all_heart_beat();
            }
            Role::Follower => { /*undo*/ }
        }
    }
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        match self.role {
            Role::Leader => {
                let mut data = vec![];
                labcodec::encode(command, &mut data).map_err(Error::Encode)?;
                let entry = Entry {
                    term: self.current_term,
                    data,
                };
                self.log.push(entry);
                self.sync_log();
                Ok((self.last_log_index(), self.last_log_term()))
            }
            _ => Err(Error::NotLeader),
        }
    }
}
///Elections and RequestVote
impl Raft {
    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    // Your code here if you want the rpc becomes async.
    // Example:
    // ```
    // let peer = &self.peers[server];
    // let peer_clone = peer.clone();
    // let (tx, rx) = channel();
    // peer.spawn(async move {
    //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
    //     tx.send(res);
    // });
    // rx
    // ```
    /// send all request vote
    fn send_request_vote(&self, args: RequestVoteArgs) {
        for (id, peer) in self.peers.iter().enumerate() {
            if id == self.me {
                continue;
            }
            let tx = self
                .event_loop_tx
                .as_ref()
                .expect("no event sender")
                .clone();
            let peer_clone = peer.clone();
            let args = args.clone();

            self.executor
                .spawn(async move {
                    if let Ok(reply) = peer_clone.request_vote(&args).await.map_err(Error::Rpc) {
                        tx.unbounded_send(Event::RequestVoteReply { reply })
                            .unwrap();
                    }
                })
                .unwrap();
        }
    }
    fn handle_request_reply(&mut self, reply: RequestVoteReply) {
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.trans_role(Role::Follower);
        }
        match self.role {
            Role::Candidate => {
                if reply.vote_granted && reply.term == self.current_term {
                    self.vote_from.insert(reply.id);
                    if self.vote_from.len() * 2 > self.peers.len() {
                        self.trans_role(Role::Leader);
                    }
                }
            }
            _ => { /*"other role can't handle request vote"*/ }
        }
    }
    fn hold_election(&mut self) {
        self.vote_for = Some(self.me as u64);
        self.vote_from.insert(self.me as u64);
        self.send_request_vote(RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        });
    }
}
///AppendEntries methods
impl Raft {
    ///send append entries to single id
    fn send_append_entry(&self, id: usize, entry: AppendEntriesArgs, is_heart: bool) {
        let tx = self
            .event_loop_tx
            .as_ref()
            .expect("no event sender")
            .clone();
        let peer_clone = self.peers[id].clone();
        let new_next_index = self.log.len();
        self.executor
            .spawn(async move {
                if let Ok(reply) = peer_clone.append_entries(&entry).await.map_err(Error::Rpc) {
                    tx.unbounded_send(Event::AppendEntriesReply {
                        reply,
                        is_heart,
                        new_next_index,
                    })
                    .unwrap();
                }
            })
            .unwrap();
    }
    ///NEXT TO DO
    fn handle_append_reply(
        &mut self,
        reply: AppendEntriesReply,
        is_heart: bool,
        new_next_index: usize,
    ) {
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.trans_role(Role::Follower);
        }
        // heartbeat only need to do last one
        if is_heart {
            return;
        }
        let id = reply.from as usize;
        match self.role {
            Role::Leader => {
                if reply.success {
                    self.next_index[id] = new_next_index;
                    self.match_index[id] = new_next_index - 1;
                    self.try_commit();
                } else {
                    //if prev_log do not match,Resend
                    self.next_index[id] -= 1;
                    self.send_append_entry_at(self.next_index[id], id);
                }
            }
            _ => {}
        }
    }
    fn send_append_entry_at(&self, at: usize, to: usize) {
        let entries = self.log[at..].iter().cloned().collect();
        let prev_log_index = at - 1;
        self.send_append_entry(
            to,
            AppendEntriesArgs {
                entries,
                prev_log_index: prev_log_index as u64,
                term: self.current_term,
                leader_id: self.me as u64,
                prev_log_term: self.log[prev_log_index].term,
                leader_commit_index: self.commit_index,
            },
            false,
        );
    }
    fn try_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut new_commit_index = self.commit_index;
        for wait_for_commit_index in self.commit_index as usize..self.log.len() {
            let mut match_count = 1;
            self.match_index.iter().enumerate().for_each(|(id, idx)| {
                if id != self.me && *idx >= wait_for_commit_index {
                    match_count += 1;
                }
            });
            //why need to check term here?
            //because of the leader may have been changed to follower in the meantime and the log may be changed in the meantime too.
            if self.log[wait_for_commit_index].term != self.current_term {
                continue;
            }
            if match_count * 2 > self.peers.len() {
                new_commit_index = wait_for_commit_index as u64;
            } else {
                break;
            }
        }
        self.commit_and_send_apply(new_commit_index);
    }
    fn commit_and_send_apply(&mut self, new_commit_index: u64) {
        //TO DO
        if new_commit_index <= self.commit_index {
            return;
        }

        for i in (self.commit_index as usize + 1)..=new_commit_index as usize {
            let msg = ApplyMsg {
                command_valid: true,
                command: self.log[i].data.clone(),
                command_index: i as u64,
            };

            self.apply_tx.unbounded_send(msg).unwrap();
        }
        self.commit_index = new_commit_index;
    }
}
///Other methods
impl Raft {
    fn handle_task(&mut self, event: Event) {
        match event {
            Event::AppendEntriesReply {
                reply,
                is_heart,
                new_next_index,
            } if self.role == Role::Leader => {
                self.handle_append_reply(reply, is_heart, new_next_index);
            }
            Event::RequestVoteReply { reply } if self.role == Role::Candidate => {
                self.handle_request_reply(reply);
            }
            Event::Timeout if self.role != Role::Leader => self.trans_role(Role::Candidate),
            Event::HeartBeat if self.role == Role::Leader => self.send_all_heart_beat(),
            _ => {}
        }
    }
    /// send heart-beat to all peers
    fn send_all_heart_beat(&mut self) {
        for (id, _) in self.peers.iter().enumerate() {
            if id == self.me {
                continue;
            }
            self.send_append_entry(
                id,
                AppendEntriesArgs {
                    term: self.current_term,
                    leader_id: self.me as u64,
                    prev_log_index: self.log.len() as u64 - 1,
                    prev_log_term: self.log.last().unwrap().term,
                    entries: vec![],
                    leader_commit_index: self.commit_index as u64,
                },
                true,
            );
        }
    }
    fn sync_log(&self) {
        for (id, _) in self.peers.iter().enumerate() {
            if id == self.me {
                continue;
            }
            self.send_append_entry_at(self.next_index[id], id);
        }
    }
    fn last_log_index(&self) -> u64 {
        self.log.len() as u64 - 1
    }
    fn last_log_term(&self) -> u64 {
        self.log.last().unwrap().term
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(Default::default());
        self.persist();
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }

    pub fn just_test(&self) {}
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    event_loop_tx: UnboundedSender<Event>,
    shut: Arc<AtomicBool>,
    executor: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        // Your code here.
        let (event_loop_tx, event_loop_rx) = mpsc::unbounded();
        raft.event_loop_tx = Some(event_loop_tx.clone());

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            event_loop_tx,
            shut: Arc::new(AtomicBool::new(false)),
            executor: ThreadPool::new().unwrap(),
        };
        node.event_loop(event_loop_rx);
        node
    }
    fn event_loop(&self, mut event_loop_rx: UnboundedReceiver<Event>) {
        let raft = Arc::clone(&self.raft);
        let event_loop_tx = self.event_loop_tx.clone();
        let shut = self.shut.clone();
        self.executor
            .spawn(async move {
                let build_rand_timer = || {
                    futures_timer::Delay::new(Duration::from_millis(
                        rand::thread_rng().gen_range(150, 300),
                    ))
                    .fuse()
                };
                let build_hb_timer = || futures_timer::Delay::new(Duration::from_millis(50)).fuse();

                let mut timeout_timer = build_rand_timer();
                let mut hb_timer = build_hb_timer();

                while !shut.load(std::sync::atomic::Ordering::SeqCst) {
                    select! {
                        event = event_loop_rx.select_next_some() => {
                            match event {
                                Event::ResetTimeout => timeout_timer = build_rand_timer(),
                                Event::Shutdown => shut.store(true,std::sync::atomic::Ordering::SeqCst),
                                event => raft.lock().unwrap().handle_task(event),
                            }
                        }
                        _ = timeout_timer => {
                            event_loop_tx.unbounded_send(Event::Timeout).unwrap();
                            timeout_timer = build_rand_timer();
                        },
                        _ = hb_timer => {
                            event_loop_tx.unbounded_send(Event::HeartBeat).unwrap();
                            hb_timer = build_hb_timer();
                        }
                    }
                }
            })
            .expect("failed to spawn event loop");
    }
    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }
    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.event_loop_tx.unbounded_send(Event::Shutdown).unwrap();
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut raft = self.raft.lock().unwrap();

        if args.term > raft.current_term {
            raft.trans_role(Role::Follower);
            raft.current_term = args.term;
        }
        let term = raft.current_term;
        // let up_to_date = (args.last_log_index, args.last_log_term)
        //     >= (raft.last_log_index(), raft.last_log_term());
        let up_to_date = (args.last_log_term, args.last_log_index)
            >= (raft.last_log_term(), raft.last_log_index());
        let vote_granted = match raft.role {
            Role::Follower if raft.vote_for == None && up_to_date => true,
            _ => false,
        };
        Ok(RequestVoteReply {
            id: raft.me as u64,
            term,
            vote_granted,
        })
    }
    ///AppendEntriesArgs RPC handler
    ///handle append_entries_args
    ///append entries sorts work

    //并不是一接受到AppendEntries并且成功就commit_index更新并且apply
    //而是leader在接收到半数成功后，才会update leader_commit_index
    //最后通过heartbeat or next append_entry使得follower update
    //commit index and apply to persist data

    // Append_Entry Flow ↓
    // 1.leader send append_entries to followers
    //  leader ->A_E-> followers

    // 2.follwer del with A_E and reply result to leader
    //  follower ->reply-> leader

    // 3.leader del with reply and update peer's match_index(success) or resend new_enties to peers(fail)
    //  leader ->update-> self.match_index
    //           \->resend-> follower

    // 4.when leader get majority success from follower,update self.commit_index
    //  leader ->update-> self.commit_index

    // 5.when next Append or Heart,leader will send new leader_commit_index,follower compare it and decide to commit_and_apply or not to
    //  leader ->A or H-> follower, follower ->cmp-> update or not update
    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.lock().unwrap();
        if args.term >= raft.current_term {
            raft.current_term = args.term;
            raft.trans_role(Role::Follower);
        }
        //initialize
        let term = raft.current_term;
        let role = raft.role.clone();
        let pli = args.prev_log_index as usize;
        let plt = args.prev_log_term;

        let success = match (role, raft.log.get(pli), term == args.term) {
            (Role::Follower, Some(Entry { term, .. }), true) if *term == plt => {
                while raft.log.len() > pli + 1 {
                    raft.log.pop();
                }
                let mut entries = args.entries;
                raft.log.append(&mut entries);
                if args.leader_commit_index > raft.commit_index {
                    let new_commit_index = args.leader_commit_index.min(raft.last_log_index());
                    raft.commit_and_send_apply(new_commit_index);
                }
                true
            }
            _ => false,
        };
        Ok(AppendEntriesReply {
            from: raft.me as u64,
            term,
            success,
        })
    }
}
