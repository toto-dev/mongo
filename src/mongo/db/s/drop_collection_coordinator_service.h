/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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
#include "mongo/db/repl/primary_only_service.h"
#include "mongo/db/s/drop_collection_coordinator_document_gen.h"

namespace mongo {

class DropCollectionCoordinatorService final : public repl::PrimaryOnlyService {
public:
    static constexpr StringData kServiceName = "DropCollectionCoordinator"_sd;
    static const NamespaceString kStateDocumentNamespace;

    explicit DropCollectionCoordinatorService(ServiceContext* serviceContext)
        : PrimaryOnlyService(serviceContext) {}

    ~DropCollectionCoordinatorService() = default;

    StringData getServiceName() const override {
        return kServiceName;
    }

    NamespaceString getStateDocumentsNS() const override {
        return kStateDocumentNamespace;
    }

    ThreadPool::Limits getThreadPoolLimits() const override {
        return ThreadPool::Limits();
    }

    std::shared_ptr<PrimaryOnlyService::Instance> constructInstance(
        BSONObj initialState) const override {
        return std::make_shared<Instance>(std::move(initialState));
    }

    class Instance final : public PrimaryOnlyService::TypedInstance<Instance> {
    public:
        using StateDoc = DropCollectionCoordinatorDocument;
        using State = DropCollectionCoordinatorStateEnum;

        explicit Instance(const BSONObj& doc);

        ~Instance();

        SemiFuture<void> run(std::shared_ptr<executor::ScopedTaskExecutor> executor,
                             const CancelationToken& token) noexcept override;

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
        void _stopMigrations(OperationContext* opCtx);
        void _removeCollMetadataFromConfig(OperationContext* opCtx);
        void _sendDropCollToParticipants(OperationContext* opCtx);

        void _insertStateDocument(StateDoc&& doc);
        void _updateStateDocument(StateDoc&& newStateDoc);
        void _removeStateDocument();
        void _transitionToState(State newState);

        Mutex _mutex = MONGO_MAKE_LATCH("DropCollectionCoordinatorService::Instance::_mutex");

        StateDoc _doc;
        const NamespaceString _nss;

        SharedPromise<void> _completionPromise;
    };
};


}  // namespace mongo
