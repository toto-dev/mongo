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
            assertAlways.commandWorked(db.adminCommand({shardCollection: nss, key: {_id: 1}}));
        }

        function drop(db, collName) {
            assertAlways.commandWorked(db.runCommand({drop: this.collName}));
        }

        return {init: init, create: create, drop: drop};
    })();

    var transitions = {
        init: {create: 0.5, drop: 0.5},
        create: {create: 0.5, drop: 0.5},
        drop: {create: 0.5, drop: 0.5}
    };

    return {
        threadCount: 1,
        iterations: 100,
        startState: 'init',
        data: data,
        states: states,
        transitions: transitions
    };
})();
