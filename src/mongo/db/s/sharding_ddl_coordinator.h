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

#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/primary_only_service.h"
#include "mongo/db/s/dist_lock_manager.h"
#include "mongo/db/s/forwardable_operation_metadata.h"
#include "mongo/db/s/sharding_ddl_coordinator_gen.h"
#include "mongo/db/s/sharding_ddl_coordinator_service.h"
#include "mongo/executor/task_executor.h"
#include "mongo/util/future.h"

namespace mongo {

class ShardingDDLCoordinator_NORESILIENT {
public:
    ShardingDDLCoordinator_NORESILIENT(OperationContext* opCtx, const NamespaceString& nss);
    SemiFuture<void> run(OperationContext* opCtx);

protected:
    NamespaceString _nss;
    ForwardableOperationMetadata _forwardableOpMetadata;

private:
    virtual SemiFuture<void> runImpl(std::shared_ptr<executor::TaskExecutor>) = 0;
};

class ShardingDDLCoordinator
    : public repl::PrimaryOnlyService::TypedInstance<ShardingDDLCoordinator> {
public:
    explicit ShardingDDLCoordinator(const BSONObj& ddlOpDoc) {
        const auto idElem = ddlOpDoc["_id"];
        uassert(6092801,
                str::stream()
                    << "Missing _id element constructing a new instance of ShardingDDLCoordinator",
                !idElem.eoo());
        auto op = ShardingDDLCoordinatorId::parse(IDLParserErrorContext("ShardingDDLCoordinatorId"),
                                                  idElem.Obj().getOwned());
        _nss = op.getNss();
    };

    virtual ~ShardingDDLCoordinator() = default;

    void failConstruction(Status errorStatus) {
        _constructionCompletionPromise.setError(errorStatus);
    };

    void completeConstruction(ForwardableOperationMetadata&& frwOpMetadata,
                              std::stack<DistLockManager::ScopedDistLock>&& scopedLocks) {
        _forwardableOpMetadata = std::move(frwOpMetadata);
        _scopedLocks = std::move(scopedLocks);
        _constructionCompletionPromise.emplaceValue();
    };

    SemiFuture<void> run(std::shared_ptr<executor::ScopedTaskExecutor> executor,
                         const CancelationToken& token) noexcept override {
        return _constructionCompletionPromise.getFuture()
            .thenRunOn(**executor)
            .then([this, executor, token, anchor = shared_from_this()] {
                return _runImpl(executor, token);
            })
            .onCompletion([this, anchor = shared_from_this()](const Status& status) {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();

                while (!_scopedLocks.empty()) {
                    _scopedLocks.top().assignNewOpCtx(opCtx);
                    _scopedLocks.pop();
                }
                return status;
            })
            .semi();
    };

    virtual Status checkIfOptionsConflict(const BSONObj& stateDoc) const = 0;

protected:
    NamespaceString _nss;
    // It is safe to access the following variables only when the > _constructionCompletionPromise
    // completes
    ForwardableOperationMetadata _forwardableOpMetadata;
    std::stack<DistLockManager::ScopedDistLock> _scopedLocks;

private:
    virtual ExecutorFuture<void> _runImpl(std::shared_ptr<executor::ScopedTaskExecutor> executor,
                                          const CancelationToken& token) = 0;

    SharedPromise<void> _constructionCompletionPromise;
};

}  // namespace mongo
