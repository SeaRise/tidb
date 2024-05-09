// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestChangePumpAndDrainer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	tk.MustMatchErrMsg("change pump to node_state ='paused' for node_id 'pump1'", "URL scheme must be http, https, unix, or unixs.*")
	tk.MustMatchErrMsg("change drainer to node_state ='paused' for node_id 'drainer1'", "URL scheme must be http, https, unix, or unixs.*")
}

func TestIssue52985(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (cc1 int,cc2 text);")
	tk.MustExec("insert into t1 values (1, 'aaaa'),(2, 'bbbb'),(3, 'cccc');")

	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (cc1 int,cc2 text,primary key(cc1));")
	tk.MustExec("insert into t2 values (2, '2');")

	tk.MustExec("set tidb_executor_concurrency = 1;")
	tk.MustExec("set tidb_window_concurrency = 100;")

	ret := tk.MustQuery("SELECT DISTINCT cc2, cc2, cc1 FROM t2 UNION ALL SELECT count(1) over (partition by cc1), cc2, cc1 FROM t1;")
	ret.Check(testkit.Rows("2 2 2", "1 bbbb 2", "1 cccc 3", "1 aaaa 1"))
}
