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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kSharding

#include "mongo/db/s/drop_collection_coordinator.h"

// TODO cleanup includes
#include "mongo/db/api_parameters.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/persistent_task_store.h"
#include "mongo/db/s/database_sharding_state.h"
#include "mongo/db/s/drop_collection_coordinator.h"
#include "mongo/db/s/sharding_ddl_util.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"
#include "mongo/util/assert_util.h"

namespace mongo {

DropCollectionCoordinator::DropCollectionCoordinator(const BSONObj& initialState)
    : ShardingDDLCoordinator(initialState),
      _doc(DropCollectionCoordinatorDocument::parse(
          IDLParserErrorContext("DropCollectionCoordinatorDocument"), initialState)) {}

DropCollectionCoordinator::~DropCollectionCoordinator() {
    stdx::lock_guard<Latch> lg(_mutex);
    invariant(_completionPromise.getFuture().isReady());
}

void DropCollectionCoordinator::interrupt(Status status) {
    LOGV2_DEBUG(5390502,
                1,
                "Drop collection coordinator received an interrupt",
                "namespace"_attr = nss(),
                "reason"_attr = redact(status));

    // Resolve any unresolved promises to avoid hanging.
    stdx::lock_guard<Latch> lg(_mutex);
    if (!_completionPromise.getFuture().isReady()) {
        _completionPromise.setError(status);
    }
}

boost::optional<BSONObj> DropCollectionCoordinator::reportForCurrentOp(
    MongoProcessInterface::CurrentOpConnectionsMode connMode,
    MongoProcessInterface::CurrentOpSessionsMode sessionMode) noexcept {
    // TODO report correct info here
    return boost::none;
}

Status DropCollectionCoordinator::checkIfOptionsConflict(const BSONObj& doc) const {
    return Status::OK();
};

void DropCollectionCoordinator::_insertStateDocument(StateDoc&& doc) {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(
        ShardingDDLCoordinatorService::kDDLCoordinatorDocumentsNamespace);
    store.add(opCtx.get(), doc, WriteConcerns::kMajorityWriteConcern);
    _doc = doc;
}

void DropCollectionCoordinator::_updateStateDocument(StateDoc&& newDoc) {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(
        ShardingDDLCoordinatorService::kDDLCoordinatorDocumentsNamespace);
    store.update(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _doc.getId().toBSON()),
                 newDoc.toBSON(),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = newDoc;
}

void DropCollectionCoordinator::_transitionToState(State newState) {
    StateDoc newDoc(_doc);
    newDoc.setState(newState);

    LOGV2_DEBUG(5390501,
                2,
                "Drop collection coordinator state transition",
                "namespace"_attr = nss(),
                "newState"_attr = DropCollectionCoordinatorState_serializer(newDoc.getState()),
                "oldState"_attr = DropCollectionCoordinatorState_serializer(_doc.getState()));

    if (_doc.getState() < State::kInitialized) {
        _insertStateDocument(std::move(newDoc));
        return;
    }
    _updateStateDocument(std::move(newDoc));
}

void DropCollectionCoordinator::_removeStateDocument() {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(
        ShardingDDLCoordinatorService::kDDLCoordinatorDocumentsNamespace);
    store.remove(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _doc.getId().toBSON()),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = {};
}

void DropCollectionCoordinator::_stopMigrations(OperationContext* opCtx) const {
    // TODO SERVER-53861 this will not stop current ongoing migrations
    uassertStatusOK(Grid::get(opCtx)->catalogClient()->updateConfigDocument(
        opCtx,
        CollectionType::ConfigNS,
        BSON(CollectionType::kNssFieldName << nss().ns()),
        BSON("$set" << BSON(CollectionType::kAllowMigrationsFieldName << false)),
        false /* upsert */,
        ShardingCatalogClient::kMajorityWriteConcern));
}

void DropCollectionCoordinator::_sendDropCollToParticipants(
    OperationContext* opCtx, const std::vector<ShardId>& participants) const {
    const ShardsvrDropCollectionParticipant dropCollectionParticipant(nss());
    auto* const shardRegistry = Grid::get(opCtx)->shardRegistry();

    for (const auto& shardId : participants) {
        const auto& shard = uassertStatusOK(shardRegistry->getShard(opCtx, shardId));

        const auto swDropResult = shard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            nss().db().toString(),
            CommandHelpers::appendMajorityWriteConcern(dropCollectionParticipant.toBSON({})),
            Shard::RetryPolicy::kIdempotent);

        uassertStatusOKWithContext(
            Shard::CommandResponse::getEffectiveStatus(std::move(swDropResult)),
            str::stream() << "Error dropping collection " << nss().toString()
                          << " on participant shard " << shardId);
    }
}

ExecutorFuture<void> DropCollectionCoordinator::_runImpl(
    std::shared_ptr<executor::ScopedTaskExecutor> executor,
    const CancelationToken& token) noexcept {
    return ExecutorFuture<void>(**executor)
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kInitialized)
                return;
            _transitionToState(State::kInitialized);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kCollectionFrozen)
                return;
            {
                auto opCtx = cc().makeOperationContext();
                _stopMigrations(opCtx.get());
                try {
                    auto coll =
                        Grid::get(opCtx.get())->catalogClient()->getCollection(opCtx.get(), nss());
                    _doc.setCollInfo(std::move(coll));
                } catch (ExceptionFor<ErrorCodes::NamespaceNotFound>&) {
                    // The collection to drop is unsharded
                    _doc.setCollInfo(boost::none);
                }
            }
            _transitionToState(State::kCollectionFrozen);
        })
        .then([this, anchor = shared_from_this()] {
            if (_doc.getState() >= State::kDropped)
                return;
            {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                // TODO restore forwardable operation metadata on stepup
                _forwardableOpMetadata.setOn(opCtx);

                auto* const shardRegistry = Grid::get(opCtx)->shardRegistry();
                std::vector<ShardId> participants;
                const auto collIsSharded = bool(_doc.getCollInfo());


                LOGV2_DEBUG(5390504,
                            2,
                            "Dropping collection",
                            "namespace"_attr = nss(),
                            "sharded"_attr = collIsSharded);
                if (collIsSharded) {
                    invariant(_doc.getCollInfo());
                    const auto& coll = _doc.getCollInfo().get();
                    // TODO handle failures here
                    sharding_ddl_util::removeCollMetadataFromConfig(opCtx, coll);
                    participants = shardRegistry->getAllShardIds(opCtx);
                } else {
                    // The collection is not sharded or didn't exist, just remove tags
                    sharding_ddl_util::removeTagsMetadataFromConfig(opCtx, nss());
                    participants = {ShardingState::get(opCtx)->shardId()};
                }

                _sendDropCollToParticipants(opCtx, std::move(participants));
            }
            _transitionToState(State::kDropped);
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
                            "namespace"_attr = nss(),
                            "error"_attr = redact(status));
                // TODO _transitionToState(State::kError);
                _completionPromise.setError(status);
                return;
            }

            LOGV2_DEBUG(5390503, 1, "Collection dropped", "namespace"_attr = nss());
            // TODO _removeStateDocument();
            _completionPromise.emplaceValue();
        });
}

}  // namespace mongo
