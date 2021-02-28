/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/s/drop_collection_coordinator_document_gen.h"
#include "mongo/db/s/sharding_ddl_coordinator.h"
#include "mongo/s/shard_id.h"

namespace mongo {

class DropCollectionCoordinator final : public ShardingDDLCoordinator {
public:
    using StateDoc = DropCollectionCoordinatorDocument;
    using State = DropCollectionCoordinatorStateEnum;

    DropCollectionCoordinator(const BSONObj& initialState);
    ~DropCollectionCoordinator() override;

    void checkIfOptionsConflict(const BSONObj& doc) const;

    void interrupt(Status status) override;

    boost::optional<BSONObj> reportForCurrentOp(
        MongoProcessInterface::CurrentOpConnectionsMode connMode,
        MongoProcessInterface::CurrentOpSessionsMode sessionMode) noexcept override;

    /**
     * Returns a Future that will be resolved when all work associated with this Instance has
     * completed running.
     */
    SharedSemiFuture<void> getCompletionFuture() const {
        return _completionPromise.getFuture();
    }

private:
    ExecutorFuture<void> _runImpl(std::shared_ptr<executor::ScopedTaskExecutor> executor,
                                  const CancelationToken& token) noexcept override;

    template <typename Func>
    auto _transitionToState(const State& state, Func&& transitionFunc) {
        return [=] {
            if (_doc.getState() >= state) {
                return;
            }
            transitionFunc();
            _moveToState(state);
        };
    };

    void _insertStateDocument(StateDoc&& doc);
    void _updateStateDocument(StateDoc&& newStateDoc);
    void _removeStateDocument();
    void _moveToState(State newState);

    DropCollectionCoordinatorDocument _doc;

    Mutex _mutex = MONGO_MAKE_LATCH("DropCollectionCoordinator::_mutex");
    SharedPromise<void> _completionPromise;
};

}  // namespace mongo
