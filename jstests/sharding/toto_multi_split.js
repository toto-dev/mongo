/*
 *  Tests that orphans eventually get deleted after setting the featureCompatibilityVersion to 4.4.
 */
load("jstests/replsets/rslib.js");

(function() {
"use strict";

const dbName = "test";
const collName = "foo";
const ns = dbName + "." + collName;
const iterations = 10000;
const slaveDelay = 1;

const st = new ShardingTest({shards: 1, mongos: 1, config: 7});

jsTest.log("XOXO: Reconfig config replset  ------------------ ");
/*
var config = st.configRS.getReplSetConfigFromNode();
printjson(config);
// Primary
assert(config.members[0].priority > 0)
// Secondaries
assert(config.members[1].priority == 0)
config.members[1].slaveDelay = slaveDelay;
assert(config.members[2].priority == 0)
config.members[2].slaveDelay = slaveDelay;

config.version += 1;
reconfig(st.configRS, config, true );

assert.soon(function() {
    var secConn = st.configRS.getSecondary();
    var config = secConn.getDB('local').system.replset.findOne();
    return config.members[1].slaveDelay == slaveDelay;
    ;
});
*/

// Create a sharded collection with four chunks: [-inf, 50), [50, 100), [100, 150) [150, inf)
assert.commandWorked(st.s.adminCommand({enableSharding: dbName}));
assert.commandWorked(st.s.adminCommand({movePrimary: dbName, to: st.shard0.shardName}));
assert.commandWorked(st.s.adminCommand({shardCollection: ns, key: {x: 1}}));

for (var x = 0; x < (50 * iterations); x += 50) {
    jsTest.log("XOXO loop[" + ((x + 50) / 50) + "] Splitting on x: " + x);
    assert.commandWorked(st.s.adminCommand({split: ns, middle: {x: x}}));
}

jsTest.log("XOXO stopping ------------------");
st.stop();
})();
