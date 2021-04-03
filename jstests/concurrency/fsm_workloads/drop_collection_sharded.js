/**
 * drop_collection_sharded.js
 *
 * Repeatedly creates and drops a collection.
 *
 * @tags: [
 *   requires_sharding,
 * ]
 */
'use strict';

var $config = (function() {
    var data = {
        // Use the workload name as a prefix for the collection name,
        // since the workload name is assumed to be unique.
        collName: 'sharded_coll_for_test',
    };

    var states = (function() {
        function init(db, collName) {
        }

        function create(db, collName) {
            const nss = db.getName() + '.' + this.collName;
            jsTest.log("XOXO shardCollection START");
            assert.commandWorked(db.adminCommand({shardCollection: nss, key: {_id: 1}}));
            jsTest.log("XOXO shardCollection END");
        }

        function drop(db, collName) {
            jsTest.log("XOXO dropCollection START");
            assert.commandWorked(db.runCommand({drop: this.collName}));
            jsTest.log("XOXO dropCollection END");
        }

        return {init: init, create: create, drop: drop};
    })();

    var transitions = {
        init: {create: 1},
        create: {create: 0.5, drop: 0.5},
        drop: {create: 0.5, drop: 0.5}
    };

    return {
        threadCount: 1,
        iterations: 60,
        startState: 'init',
        data: data,
        states: states,
        transitions: transitions
    };
})();
