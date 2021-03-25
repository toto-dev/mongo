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
        collPrefix: 'sharded_coll_for_test_',
        collCount: 5,
    };

    var states = (function() {
        function init(db, collName) {
            this.collName = this.collPrefix + (this.tid % this.collCount);
        }

        function create(db, collName) {
            const nss = db.getName() + '.' + this.collName;
            jsTest.log("XOXO shardCollection START: " + nss);
            assertAlways.commandWorked(db.adminCommand({shardCollection: nss, key: {_id: 1}}));
            jsTest.log("XOXO shardCollection END: " + nss);
        }

        function drop(db, collName) {
            jsTest.log("XOXO dropCollection START: " + db + "." + this.collName);
            assertAlways.commandWorked(db.runCommand({drop: this.collName}));
            jsTest.log("XOXO dropCollection END: " + db + "." + this.collName);
        }

        return {init: init, create: create, drop: drop};
    })();

    var transitions = {
        init: {create: 1},
        create: {create: 0.5, drop: 0.5},
        drop: {create: 0.5, drop: 0.5}
    };

    return {
        threadCount: 15,
        iterations: 100,
        startState: 'init',
        data: data,
        states: states,
        transitions: transitions
    };
})();
