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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/drop_collection_coordinator_service.h"

#include "mongo/db/api_parameters.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"


namespace mongo {

const NamespaceString DropCollectionCoordinatorService::kStateDocumentNamespace =
    NamespaceString(NamespaceString::kConfigDb, "ddl.dropCollection.coordinator");

DropCollectionCoordinatorService::Instance::Instance(const BSONObj& stateDoc)
    : repl::PrimaryOnlyService::TypedInstance<DropCollectionCoordinatorService::Instance>(),
      _doc(DropCollectionCoordinatorDocument::parse(
          IDLParserErrorContext("DropCollectionCoordinatorDocument"), stateDoc)),
      _nss(_doc.getId()) {}

DropCollectionCoordinatorService::Instance::~Instance() {
    stdx::lock_guard<Latch> lg(_mutex);
    invariant(_completionPromise.getFuture().isReady());
}

SemiFuture<void> DropCollectionCoordinatorService::Instance::run(
    std::shared_ptr<executor::ScopedTaskExecutor> executor,
    const CancelationToken& token) noexcept {
    return ExecutorFuture<void>(**executor)
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kInitialized) {
                return;
            }
            _transitionToState(State::kInitialized);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kCollectionFrozen)
                return;
            {
                auto opCtx = cc().makeOperationContext();
                _stopMigrations(opCtx.get());
            }
            _transitionToState(State::kCollectionFrozen);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kFetchedCollInfo)
                return;
            {
                auto opCtx = cc().makeOperationContext();
                const auto routingInfo =
                    uassertStatusOK(Grid::get(opCtx.get())
                                        ->catalogCache()
                                        ->getCollectionRoutingInfoWithRefresh(opCtx.get(), _nss));
                _doc.setSharded(routingInfo.isSharded());
            }
            _transitionToState(State::kFetchedCollInfo);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kMetadataCleanedOnConfig)
                return;
            {
                // TODO patch up the operation context with:
                // impersonation and comment
                auto opCtx = cc().makeOperationContext();
                // TODO handle failures here
                _removeCollMetadataFromConfig(opCtx.get());
            }
            _transitionToState(State::kMetadataCleanedOnConfig);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kCollectionDroppedOnParticipants)
                return;
            {
                // TODO patch up the operation context with:
                // impersonation and comment
                auto opCtx = cc().makeOperationContext();
                // TODO handle failures here
                _sendDropCollToParticipants(opCtx.get());
            }
            _transitionToState(State::kCollectionDroppedOnParticipants);
        })
        .onCompletion([this, anchor = shared_from_this()](const Status& status) {
            stdx::lock_guard<Latch> lk(_mutex);
            if (_completionPromise.getFuture().isReady()) {
                // interrupt() was called before we got here.
                return;
            }

            if (!status.isOK()) {
                LOGV2_ERROR(5280901,
                            "Error running drop collection",
                            "namespace"_attr = _nss,
                            "error"_attr = redact(status));
                _transitionToState(State::kError);
                _completionPromise.setError(status);
                return;
            }

            LOGV2_DEBUG(5390501, 1, "Collection dropped", "namespace"_attr = _nss);
            _removeStateDocument();
            _completionPromise.emplaceValue();
        })
        .semi();
}

void DropCollectionCoordinatorService::Instance::interrupt(Status status) {
    LOGV2_DEBUG(5390502,
                1,
                "Drop collection coordinator received an interrupt",
                "namespace"_attr = _nss,
                "reason"_attr = redact(status));

    // Resolve any unresolved promises to avoid hanging.
    stdx::lock_guard<Latch> lg(_mutex);
    if (!_completionPromise.getFuture().isReady()) {
        _completionPromise.setError(status);
    }
}

boost::optional<BSONObj> DropCollectionCoordinatorService::Instance::reportForCurrentOp(
    MongoProcessInterface::CurrentOpConnectionsMode connMode,
    MongoProcessInterface::CurrentOpSessionsMode sessionMode) noexcept {
    // TODO report correct info here
    return boost::none;
}

void DropCollectionCoordinatorService::Instance::_stopMigrations(OperationContext* opCtx) {
    // TODO SERVER-53861 this will not stop current ongoing migrations
    uassertStatusOK(Grid::get(opCtx)->catalogClient()->updateConfigDocument(
        opCtx,
        CollectionType::ConfigNS,
        BSON(CollectionType::kNssFieldName << _nss.ns()),
        BSON("$set" << BSON(CollectionType::kAllowMigrationsFieldName << false)),
        false /* upsert */,
        ShardingCatalogClient::kMajorityWriteConcern));
}

void DropCollectionCoordinatorService::Instance::_removeCollMetadataFromConfig(
    OperationContext* opCtx) {
    IgnoreAPIParametersBlock ignoreApiParametersBlock(opCtx);
    const auto catalogClient = Grid::get(opCtx)->catalogClient();

    ON_BLOCK_EXIT([this, opCtx] {
        Grid::get(opCtx)->catalogCache()->invalidateCollectionEntry_LINEARIZABLE(_nss);
    });

    // Remove chunk data
    uassertStatusOK(
        catalogClient->removeConfigDocuments(opCtx,
                                             ChunkType::ConfigNS,
                                             BSON(ChunkType::ns(_nss.ns())),
                                             ShardingCatalogClient::kMajorityWriteConcern));
    // Remove tag data
    uassertStatusOK(
        catalogClient->removeConfigDocuments(opCtx,
                                             TagsType::ConfigNS,
                                             BSON(TagsType::ns(_nss.ns())),
                                             ShardingCatalogClient::kMajorityWriteConcern));
    // Remove coll metadata
    uassertStatusOK(
        catalogClient->removeConfigDocuments(opCtx,
                                             CollectionType::ConfigNS,
                                             BSON(CollectionType::kNssFieldName << _nss.ns()),
                                             ShardingCatalogClient::kMajorityWriteConcern));
}

void DropCollectionCoordinatorService::Instance::_sendDropCollToParticipants(
    OperationContext* opCtx) {
    auto* const shardRegistry = Grid::get(opCtx)->shardRegistry();

    std::vector<ShardId> participants;
    if (_doc.getSharded()) {
        participants = shardRegistry->getAllShardIds(opCtx);
    } else {
        participants = {ShardingState::get(opCtx)->shardId()};
    }

    const ShardsvrDropCollectionParticipant dropCollectionParticipant(_nss);

    for (const auto& shardId : participants) {
        const auto& shard = uassertStatusOK(shardRegistry->getShard(opCtx, shardId));

        const auto swDropResult = shard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            _nss.db().toString(),
            CommandHelpers::appendMajorityWriteConcern(dropCollectionParticipant.toBSON({})),
            Shard::RetryPolicy::kIdempotent);

        uassertStatusOKWithContext(
            Shard::CommandResponse::getEffectiveStatus(std::move(swDropResult)),
            str::stream() << "Error dropping collection " << _nss.toString()
                          << " on participant shard " << shardId);
    }
}

void DropCollectionCoordinatorService::Instance::_insertStateDocument(StateDoc&& doc) {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(DropCollectionCoordinatorService::kStateDocumentNamespace);
    store.add(opCtx.get(), doc, WriteConcerns::kMajorityWriteConcern);
    _doc = doc;
}

void DropCollectionCoordinatorService::Instance::_updateStateDocument(StateDoc&& newDoc) {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(DropCollectionCoordinatorService::kStateDocumentNamespace);
    store.update(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _nss.toString()),
                 newDoc.toBSON(),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = newDoc;
}

void DropCollectionCoordinatorService::Instance::_transitionToState(State newState) {
    StateDoc newDoc(_doc);
    newDoc.setState(newState);

    LOGV2_DEBUG(5390501,
                2,
                "Drop collection coordinator state transition",
                "namespace"_attr = _nss,
                "newState"_attr = DropCollectionCoordinatorState_serializer(newDoc.getState()),
                "oldState"_attr = DropCollectionCoordinatorState_serializer(_doc.getState()));

    if (_doc.getState() < State::kInitialized) {
        _insertStateDocument(std::move(newDoc));
        return;
    }
    _updateStateDocument(std::move(newDoc));
}

void DropCollectionCoordinatorService::Instance::_removeStateDocument() {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(DropCollectionCoordinatorService::kStateDocumentNamespace);
    store.remove(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _nss.toString()),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = {};
}


}  // namespace mongo
