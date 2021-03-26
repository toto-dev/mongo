/**
 * Basic test from the drop collection command on a sharded cluster that verifies collections are
 * cleaned up properly.
 */
(function() {
"use strict";

load("jstests/sharding/libs/find_chunks_util.js");

var st = new ShardingTest({shards: 2});

const configDB = st.s.getDB('config');
const dbName = 'testDropCollDB';
var dbCounter = 0;

function getCollectionUUID(ns) {
    return configDB.collections.findOne({_id: ns}).uuid;
}

function getNewDb() {
    return st.s.getDB(dbName + dbCounter++);
}

function assertCollectionDropped(ns, uuid = null) {
    // No more documents
    assert.eq(
        0, st.s.getCollection(ns).countDocuments({}), "Found documents for dropped collection.");

    // No more tags
    assert.eq(0,
              configDB.tags.countDocuments({ns: ns}),
              "Found unexpected tag for a collection after drop.");

    // No more chunks
    const errMsg = "Found collection entry in 'config.collection' after drop.";
    // Before 5.0 chunks were indexed by ns, now by uuid
    assert.eq(0, configDB.chunks.countDocuments({ns: ns}), errMsg);
    if (uuid != null) {
        assert.eq(0, configDB.chunks.countDocuments({uuid: uuid}), errMsg);
    }

    // No more coll entry
    assert.eq(null, st.s.getCollection(ns).exists());

    // Check for the collection with majority RC to verify that the write to remove the collection
    // document from the catalog has propagated to the majority snapshot. Note that here we
    // explicitly use a command instead of going through the driver's 'find' helper, in order to be
    // able to specify a 'majority' read concern.
    //
    // assert.eq(0, configDB.chunks.countDocuments({_id: ns{));
    //
    // TODO (SERVER-51881): Remove this check after 5.0 is released
    var collEntry =
        assert
            .commandWorked(configDB.runCommand(
                {find: 'collections', filter: {_id: ns}, readConcern: {'level': 'majority'}}))
            .cursor.firstBatch;
    if (collEntry.length > 0) {
        assert.eq(1, collEntry.length);
        assert.eq(true, collEntry[0].dropped);
    }
}

jsTest.log("Drop unsharded collection.");
{
    const db = st.s.getDB('asd');
    const coll = db['unshardedColl0'];

    // Create the collection
    assert.commandWorked(coll.insert({x: 1}));
    assert.eq(1, coll.countDocuments({x: 1}));
    // Drop the collection
    let awaitShell = startParallelShell(() => {
        assert.commandWorked(db.getSiblingDB("asd").runCommand({drop: "unshardedColl0"}));
    }, st.s.port);

    jsTest.log("XOXO waiting for the drop to hang");
    sleep(7000);
    jsTest.log("XOXO Stepping down rs0");
    assert.commandWorked(st.rs0.getPrimary().adminCommand({replSetStepDown: 60, force: 1}));
    jsTest.log("XOXO Stepping down rs1");
    assert.commandWorked(st.rs1.getPrimary().adminCommand({replSetStepDown: 60, force: 1}));
    assertCollectionDropped(coll.getFullName());
}

st.stop();
})();
