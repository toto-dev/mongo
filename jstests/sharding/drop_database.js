// Tests the behavior of a $lookup when the mongos contains stale routing information for the
// local and/or foreign collections.  This includes when mongos thinks the collection is sharded
// when it's not, and likewise when mongos thinks the collection is unsharded but is actually
// sharded.
(function() {
"use strict";

const testName = "test_drop_db";
const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

const db = st.s0.getDB(testName);
const coll = db[testName + "_local"];

// Ensure that shard0 is the primary shard.
assert.commandWorked(db.adminCommand({enableSharding: db.getName()}));
st.ensurePrimaryShard(db.getName(), st.shard0.shardName);

jsTest.log("XOXO: insert before ------------------ ");
assert.commandWorked(coll.insert({_id: 0, a: 1}));

jsTest.log("XOXO: drop ------------------ ");
assert.commandWorked(db.dropDatabase());

jsTest.log("XOXO: insert after ------------------ ");
assert.commandWorked(coll.insert({_id: 0, a: 1}));

jsTest.log("XOXO: stopping ------------------ ");
st.stop();
})();
