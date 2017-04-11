/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2017                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#include <set>
#include <map>
#include <string>
#include <numeric>
#include <fstream>
#include <iostream>
#include <iterator>
#include <unordered_set>

#define CAF_SUITE manual_stream_management
#include "caf/test/dsl.hpp"

#include "caf/io/middleman.hpp"
#include "caf/io/basp_broker.hpp"
#include "caf/io/network/test_multiplexer.hpp"

using std::cout;
using std::endl;
using std::string;

using namespace caf;

namespace {

/// Used to initiate a peering request.
using peer_atom = atom_constant<atom("peer")>;

/// Identifies a single topic.
using topic = std::string;

/// Identifies a set of topics.
using topics = std::set<topic>;

/// A single element (as key/value pair) in a stream.
using element = std::pair<topic, int>;

/// An unbound sequence of elements.
using stream_t = stream<element>;

/// A smart pointer to a `stream_handler`.
using stream_handler_ptr = intrusive_ptr<stream_handler>;

/// A downstream for a path consuming `element` messages.
using downstream_t = downstream<element>;

bool in_filter(const topics& ts, const topic& t) {
  auto e = ts.end();
  return std::find(ts.begin(), e, t) != e;
}

/// A peer is connected via two streams: one for inputs and one for outputs.
class peer_state : public ref_counted {
public:
  using buf_type = std::deque<element>;

  struct {
    stream_t sid;
    stream_handler_ptr ptr;
  }
  in;

  struct {
    stream_t sid;
    stream_handler_ptr ptr;
    buf_type buf;
    topics filter;
  }
  out;

  peer_state() {
    // nop
  }
};

/// A sink is a local actor listening to one or more topics.
class sink_state {
public:
  stream_handler_ptr ptr;
  downstream_t& out;
  topics filter;

  sink_state(stream_handler_ptr p, topics ts)
      : ptr(std::move(p)),
        out(static_cast<downstream_t&>(*ptr->get_downstream())),
        filter(std::move(ts)) {
    // nop
  }
};

struct state;

/// A policy used by the core actor to broadcast local messages to all remotes.
class core_broadcast_policy final : public downstream_policy {
public:
  core_broadcast_policy(state& st) : st_(st) {
    // nop
  }

  void push(abstract_downstream& out, size_t*) override;

  size_t available_credit(const abstract_downstream& out) override;

  static std::unique_ptr<downstream_policy> make(state& st) {
    return std::unique_ptr<downstream_policy>{new core_broadcast_policy(st)};
  }

private:
  /// State of the parent actor.
  state& st_;
};

/// Stores streams to and from remote peers as well as streams to and from local
/// actors.
struct state {
  using peer_ptr = intrusive_ptr<peer_state>;

  using stream_handler_ptr = intrusive_ptr<stream_handler>;

  state(event_based_actor* self_p) : self(self_p) {
    // nop
  }

  /// Returns the peer to the currently processed stream message, i.e.,
  /// `self->current_mailbox_element()->stages.back()` if the stages stack
  /// is not empty.
  strong_actor_ptr prev_peer_from_handshake() {
    auto& xs = self->current_mailbox_element()->content();
    strong_actor_ptr res;
    if (xs.match_elements<stream_msg>()) {
      auto& x = xs.get_as<stream_msg>(0);
      if (holds_alternative<stream_msg::open>(x.content)) {
        res = get<stream_msg::open>(x.content).prev_stage;
      }
    }
    return res;
    // auto& stages = self->current_mailbox_element()->stages;
    // return stages.empty() ? nullptr : stages.back();
  }

  template <class T>
  stream_t source(const peer_ptr& ptr, const T& handshake_argument) {
    return self->add_source(
      // Tell remote side what topics we have subscribers to.
      std::forward_as_tuple(handshake_argument),
      // initialize state
      [](unit_t&) {
        // nop
      },
      // get next element
      [ptr](unit_t&, downstream_t& out, size_t num) mutable {
        auto& xs = ptr->out.buf;
        auto n = std::min(num, xs.size());
        if (n > 0) {
          for (size_t i = 0; i < n; ++i)
            out.push(xs[i]);
          xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
        }
      },
      // never done 
      [](const unit_t&) {
        return false;
      },
      // use our custom policy
      core_broadcast_policy::make(*this)
    );
  }

  template <class T>
  stream_t new_stream(const strong_actor_ptr& dest, const peer_ptr& ptr,
                      const T& handshake_argument) {
    return self->new_stream(
      dest,
      // Tell remote side what topics we have subscribers to.
      std::forward_as_tuple(handshake_argument),
      // initialize state
      [](unit_t&) {
        // nop
      },
      // get next element
      [ptr](unit_t&, downstream_t& out, size_t num) mutable {
        auto& xs = ptr->out.buf;
        auto n = std::min(num, xs.size());
        if (n > 0) {
          for (size_t i = 0; i < n; ++i)
            out.push(xs[i]);
          xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
        }
      },
      // never done 
      [](const unit_t&) {
        return false;
      },
      [=](expected<void>) {
        // 
      },
      // use our custom policy
      core_broadcast_policy::make(*this)
    );
  }

  stream_handler_ptr sink(const stream_t& in, const peer_ptr&) {
    return self->add_sink(
      // input stream
      in,
      // initialize state
      [](unit_t&) {
        // nop
      },
      // processing step 
      [=](unit_t&, element x) mutable {
        // TODO: this breaks batching, because we signalize each element
        //       individually to the downstream; solution: implement a custom
        //       stream_stage that implements this directly and pushes
        //       connected streams in `push()`.
        for (auto& kvp : sinks){
          auto& sst = kvp.second;
          if (in_filter(sst.filter, x.first)) {
            sst.out.push(x);
            sst.ptr->push();
          }
        }
      },
      // cleanup and produce result message
      [](unit_t&) {
        // nop
      }
    ).ptr();
  }

  /// Streams to and from peers.
  std::map<strong_actor_ptr, peer_ptr> streams;

  /// List of pending peering requests, i.e., state established after
  /// receiving {`peer`, topics} (step #1) but before receiving the actual
  /// stream handshake (step #3).
  std::map<strong_actor_ptr, peer_ptr> pending;

  /// Streams to local actors consuming data.
  std::map<strong_actor_ptr, sink_state> sinks;

  /// Streams to local actors producing data.
  std::map<strong_actor_ptr, stream_handler_ptr> sources;

  /// Requested topics on this core.
  topics filter;

  /// Points to the parent actor.
  event_based_actor* self;
};

void core_broadcast_policy::push(abstract_downstream& x, size_t* hint) {
  using dtype = downstream_t;
  CAF_ASSERT(dynamic_cast<dtype*>(&x) != nullptr);
  auto& out = static_cast<dtype&>(x);
  auto num = hint ? *hint : available_credit(x);
  auto first = out.buf().begin();
  auto last = std::next(first, static_cast<ptrdiff_t>(num));
  for (auto& kvp : st_.streams) {
    auto& ptr = kvp.second->out.ptr;
    CAF_ASSERT(ptr != nullptr);
    auto& path = static_cast<dtype&>(*ptr->get_downstream());
    auto& buf = path.buf();
    buf.insert(buf.end(), first, last);
    path.push();
  }
  out.buf().erase(first, last);
}

size_t core_broadcast_policy::available_credit(const abstract_downstream&) {
  if (st_.streams.empty())
    return 0;
  size_t res = std::numeric_limits<size_t>::max();
  for (auto& kvp : st_.streams) {
    auto& ptr = kvp.second->out.ptr;
    CAF_ASSERT(ptr != nullptr);
    res = std::min(res, ptr->get_downstream()->total_credit());
  }
  return res;
}

behavior core(stateful_actor<state>* self, topics ts) {
  self->state.filter = std::move(ts);
  return {
    // -- Peering requests from local actors, i.e., "step 0". ------------------
    [=](peer_atom, strong_actor_ptr remote_core) -> result<void> {
      if (remote_core == nullptr)
        return sec::invalid_argument;
      // Simply return if we already are peering with B.
      auto& st = self->state;
      auto i = st.streams.find(remote_core);
      if (i != st.streams.end())
        return unit;
      // Create necessary state and send message to remote core.
      self->send(actor{self} * actor_cast<actor>(remote_core),
                 peer_atom::value, self->state.filter);
      return unit;
    },
    // -- 3-way handshake for establishing peering streams between A and B. ----
    // -- A (this node) performs steps #1 and #3. B performs #2 and #4. --------
    // Step #1: A demands B shall establish a stream back to A. A has
    //          subscribers to the topics `ts`.
    [=](peer_atom, topics& peer_ts) -> stream_t {
      auto& st = self->state;
      // Reject anonymous peering requests.
      auto p = self->current_sender();
      if (!p) {
        CAF_LOG_INFO("Removed anonymous peering request.");
        return invalid_stream;
      }
      // Ignore unexpected handshakes as well as handshakes that collide
      // with an already pending handshake.
      if (st.streams.count(p) > 0 || st.pending.count(p) > 0) {
        CAF_LOG_INFO("Received peering request for already known peer.");
        return invalid_stream;
      }
      auto ptr = make_counted<peer_state>();
      auto res = st.source(ptr, self->state.filter);
      ptr->out.filter = std::move(peer_ts);
      ptr->out.sid = res.id();
      ptr->out.ptr = res.ptr();
      st.pending.emplace(p, std::move(ptr));
      return res;
    },
    // step #2: B establishes a stream to A, sending its own local subscriptions
    [=](const stream_t& in, topics& filter) {
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (p == nullptr) {
        CAF_LOG_INFO("Ingored anonymous peering request.");
        return;
      }
      // Initialize required state for in- and output stream.
      auto ptr = make_counted<peer_state>();
      st.sink(in, ptr);
      ptr->in.sid = in.id();
      ptr->in.ptr = self->streams().find(in.id())->second;
      auto res = st.new_stream(p, ptr, ok_atom::value);
      ptr->out.filter = std::move(filter);
      ptr->out.sid = res.id();
      ptr->out.ptr = res.ptr();
    },
    // step #3: A establishes a stream to B
    // (now B has a stream to A and vice versa)
    [=](const stream_t& in, ok_atom) {
      auto& st = self->state;
      // Reject anonymous peering requests and unrequested handshakes.
      auto p = st.prev_peer_from_handshake();
      if (!p) {
        CAF_LOG_INFO("Ignored anonymous peering request.");
        return;
      }
      // Reject step #3 handshake if this actor didn't receive a step #1
      // handshake previously.
      auto i = st.pending.find(p);
      if (i == st.pending.end()) {
        CAF_LOG_WARNING("Received a step #3 handshake, but no #1 previously.");
        return;
      }
      // Finalize state by creating a sink and updating our peer information.
      auto& ptr = i->second;
      ptr->in.sid = in.id();
      ptr->in.ptr = st.sink(in, ptr);
      st.streams.emplace(p, std::move(ptr));
      st.pending.erase(i);
    },
    // -- Communication to local actors: incoming streams and subscriptions. ---
    [=](join_atom, topics&) -> expected<stream_t> {
      auto& st = self->state;
      auto& cs = self->current_sender();
      if (cs == nullptr)
        return sec::cannot_add_downstream;
      if (st.sinks.count(cs) != 0)
        return sec::downstream_already_exists;
      using buf_type = std::vector<element>;
      auto res = self->add_source(
        [](unit_t&) {
          // nop
        },
        // Get next element.
        [](unit_t&, downstream_t&, size_t) {
          // nop
        },
        // Did we reach the end?.
        [](const unit_t&) {
          // The stream exists as long as this actor is alive.
          return false;
        }
      );
      //st.sinks.emplace(cs, res.ptr());
      return res;
    },
    [=](const stream_t&) {

    }
  };
}

void driver(event_based_actor* self, const actor& sink) {
  using buf_type = std::vector<element>;
  self->new_stream(
    // Destination.
    sink,
    // Initialize send buffer with 10 elements.
    [](buf_type& xs) {
      xs = buf_type{{"a", 0}, {"b", 0}, {"a", 1}, {"a", 2}, {"b", 1},
                    {"b", 2}, {"a", 3}, {"b", 3}, {"a", 4}, {"a", 5}};
    },
    // Get next element.
    [](buf_type& xs, downstream_t& out, size_t num) {
      auto n = std::min(num, xs.size());
      for (size_t i = 0; i < n; ++i)
        out.push(xs[i]);
      xs.erase(xs.begin(), xs.begin() + static_cast<ptrdiff_t>(n));
    },
    // Did we reach the end?.
    [](const buf_type& xs) {
      return xs.empty();
    },
    // Handle result of the stream.
    [](expected<void>) {
      // nop
    }
  );
}

void consumer(event_based_actor* self, topics ts, const actor& src) {
  self->send(self * src, join_atom::value, std::move(ts));
  self->become(
    [=](const stream_t& in) {
      self->add_sink(
        // Input stream.
        in,
        // Initialize state.
        [](unit_t&) {
          // nop
        },
        // Process single element.
        [](unit_t&, element) {
          // nop
        },
        // Cleanup.
        [](unit_t&) {
          // nop
        }
      );
    }
  );
}

struct config : actor_system_config {
public:
  config() {
    add_message_type<element>("element");
  }
};

using fixture = test_coordinator_fixture<config>;

} // namespace <anonymous>

CAF_TEST_FIXTURE_SCOPE(manual_stream_management, fixture)

CAF_TEST(two_peers) {
  // Spawn core actors.
  auto core1 = sys.spawn(core, topics{"a", "b", "c"});
  auto core2 = sys.spawn(core, topics{"c", "d", "e"});
  sched.run();
  // Connect a consumer (leaf) to core2.
  auto leaf = sys.spawn(consumer, topics{"b"}, core2);
  sched.run_once();
  expect((atom_value, topics),
         from(leaf).to(core2).with(join_atom::value, topics{"b"}));
  expect((stream_msg::open), from(_).to(leaf).with(_, core2, _, _, false));
  expect((stream_msg::ack_open), from(leaf).to(core2).with(_, 5, _, false));
  // Initiate handshake between core1 and core2.
  self->send(core1, peer_atom::value, actor_cast<strong_actor_ptr>(core2));
  expect((peer_atom, strong_actor_ptr), from(self).to(core1).with(_, core2));
  // Step #1: core1  --->    ('peer', topics)    ---> core2
  expect((peer_atom, topics),
         from(core1).to(core2).with(_, topics{"a", "b", "c"}));
  // Step #2: core1  <---   (stream_msg::open)   <--- core2
  expect((stream_msg::open),
         from(_).to(core1).with(
           std::make_tuple(_, topics{"c", "d", "e"}), core2, _, _,
           false));
  // Step #3: core1  --->   (stream_msg::open)   ---> core2
  //          core1  ---> (stream_msg::ack_open) ---> core2
  expect((stream_msg::open), from(_).to(core2).with(_, core1, _, _, false));
  expect((stream_msg::ack_open), from(core1).to(core2).with(_, 5, _, false));
  // Shutdown.
  CAF_MESSAGE("Shutdown core actors.");
  anon_send_exit(core1, exit_reason::user_shutdown);
  anon_send_exit(core2, exit_reason::user_shutdown);
  sched.run();


  return;
  // core1 <----(stream_msg::ack_open)------ core2
  expect((stream_msg::ack_open), from(core2).to(core1).with(_, 5, _, false));
  // core1 ----(stream_msg::batch)---> core2
  expect((stream_msg::batch),
         from(core1).to(core2).with(5, std::vector<int>{1, 2, 3, 4, 5}, 0));
  // core1 <--(stream_msg::ack_batch)---- core2
  expect((stream_msg::ack_batch), from(core2).to(core1).with(5, 0));
  // core1 ----(stream_msg::batch)---> core2
  expect((stream_msg::batch),
         from(core1).to(core2).with(4, std::vector<int>{6, 7, 8, 9}, 1));
  // core1 <--(stream_msg::ack_batch)---- core2
  expect((stream_msg::ack_batch), from(core2).to(core1).with(4, 1));
  // core1 ----(stream_msg::close)---> core2
  expect((stream_msg::close), from(core1).to(core2).with());
  // core2 ----(result: 25)---> core1
  expect((int), from(core2).to(core1).with(45));
}

CAF_TEST_FIXTURE_SCOPE_END()
