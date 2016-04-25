// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, RwLock};
use std::boxed::{Box, FnBox};
use std::net::SocketAddr;
use std::fmt::{self, Formatter, Display};

use super::Result;
use util::{self, HandyRwLock};
use util::worker::{Runnable, Worker};
use pd::PdClient;

pub type Callback = Box<FnBox(Result<SocketAddr>) + Send>;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}

/// Snapshot generating task.
struct Task {
    store_id: u64,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "resolve store {} address", self.store_id)
    }
}

pub struct Runner<T: PdClient> {
    cluster_id: u64,
    pd_client: Arc<RwLock<T>>,
}

impl<T: PdClient> Runner<T> {
    fn resolve(&self, store_id: u64) -> Result<SocketAddr> {
        // TODO: cache store address for some time so that we can use it
        // even pd is down.
        let store = try!(self.pd_client.rl().get_store(self.cluster_id, store_id));

        let addr = store.get_address();
        let sock = try!(util::to_socket_addr(addr));
        Ok(sock)
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        let store_id = task.store_id;
        let resp = self.resolve(store_id);
        task.cb.call_box((resp,))
    }
}

pub struct PdStoreAddrResolver {
    worker: Worker<Task>,
}

impl PdStoreAddrResolver {
    pub fn new<T>(cluster_id: u64, pd_client: Arc<RwLock<T>>) -> Result<PdStoreAddrResolver>
        where T: PdClient + 'static
    {
        let mut r = PdStoreAddrResolver {
            worker: Worker::new("store address resolve worker".to_owned()),
        };

        let runner = Runner {
            cluster_id: cluster_id,
            pd_client: pd_client,
        };
        box_try!(r.worker.start(runner));
        Ok(r)
    }
}

impl StoreAddrResolver for PdStoreAddrResolver {
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()> {
        let task = Task {
            store_id: store_id,
            cb: cb,
        };
        box_try!(self.worker.schedule(task));
        Ok(())
    }
}

impl Drop for PdStoreAddrResolver {
    fn drop(&mut self) {
        if let Err(e) = self.worker.stop() {
            error!("failed to stop store address resolve thread: {:?}!!!", e);
        }
    }
}

pub struct MockStoreAddrResolver;

impl StoreAddrResolver for MockStoreAddrResolver {
    fn resolve(&self, _: u64, _: Callback) -> Result<()> {
        unimplemented!();
    }
}