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

#include "mongo/db/persistent_task_store.h"
#include "mongo/db/s/sharding_ddl_util.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/s/sharding_util.h"
#include "mongo/logv2/log.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/sharded_ddl_commands_gen.h"

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

    BSONObjBuilder cmdBob;
    if (const auto& optComment = getForwardableOpMetadata().getComment()) {
        cmdBob.append("comment", optComment.get());
    }
    BSONObjBuilder bob;
    bob.append("type", "op");
    bob.append("desc", "DropCollectionCoordinator");
    bob.append("op", "command");
    bob.append("ns", nss().toString());
    bob.append("command", cmdBob.obj());
    bob.append("state", _doc.getState());
    bob.append("active", true);
    return bob.obj();
}

void DropCollectionCoordinator::checkIfOptionsConflict(const BSONObj& doc) const {};

void DropCollectionCoordinator::_insertStateDocument(StateDoc&& doc) {
    auto coorMetadata = doc.getShardingDDLCoordinatorMetadata();
    // TODO we can do this on the step up for all the documents
    coorMetadata.setRecoveredFromDisk(true);
    doc.setShardingDDLCoordinatorMetadata(coorMetadata);

    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(NamespaceString::kShardingDDLCoordinatorsNamespace);
    store.add(opCtx.get(), doc, WriteConcerns::kMajorityWriteConcern);
    _doc = doc;
}

void DropCollectionCoordinator::_updateStateDocument(StateDoc&& newDoc) {
    auto opCtx = cc().makeOperationContext();
    PersistentTaskStore<StateDoc> store(NamespaceString::kShardingDDLCoordinatorsNamespace);
    store.update(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _doc.getId().toBSON()),
                 newDoc.toBSON(),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = newDoc;
}

void DropCollectionCoordinator::_moveToState(State newState) {
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
    PersistentTaskStore<StateDoc> store(NamespaceString::kShardingDDLCoordinatorsNamespace);
    LOGV2_DEBUG(5390502,
                2,
                "Removing state document for drop collection coordinator",
                "namespace"_attr = nss());
    store.remove(opCtx.get(),
                 BSON(StateDoc::kIdFieldName << _doc.getId().toBSON()),
                 WriteConcerns::kMajorityWriteConcern);

    _doc = {};
}

ExecutorFuture<void> DropCollectionCoordinator::_runImpl(
    std::shared_ptr<executor::ScopedTaskExecutor> executor,
    const CancelationToken& token) noexcept {
    return ExecutorFuture<void>(**executor)
        .then(_transitionToState(State::kInitialized, [this, anchor = shared_from_this()] {}))
        .then(_transitionToState(
            State::kCollectionFrozen,
            [this, anchor = shared_from_this()] {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                getForwardableOpMetadata().setOn(opCtx);

                try {
                    sharding_ddl_util::stopMigrations(opCtx, nss());
                    auto coll = Grid::get(opCtx)->catalogClient()->getCollection(opCtx, nss());
                    _doc.setCollInfo(std::move(coll));
                } catch (ExceptionFor<ErrorCodes::NamespaceNotSharded>&) {
                    // The collection is not sharded or doesn't exist.
                    _doc.setCollInfo(boost::none);
                }
            }))
        .then(_transitionToState(
            State::kDropped,
            [this, executor = executor, anchor = shared_from_this()] {
                auto opCtxHolder = cc().makeOperationContext();
                auto* opCtx = opCtxHolder.get();
                getForwardableOpMetadata().setOn(opCtx);

                const auto collIsSharded = bool(_doc.getCollInfo());

                LOGV2_DEBUG(5390504,
                            2,
                            "Dropping collection",
                            "namespace"_attr = nss(),
                            "sharded"_attr = collIsSharded);

                if (collIsSharded) {
                    invariant(_doc.getCollInfo());
                    const auto& coll = _doc.getCollInfo().get();
                    sharding_ddl_util::removeCollMetadataFromConfig(opCtx, coll);
                } else {
                    // The collection is not sharded or didn't exist, just remove tags
                    sharding_ddl_util::removeTagsMetadataFromConfig(opCtx, nss());
                }

                const auto primaryShardId = ShardingState::get(opCtx)->shardId();
                const ShardsvrDropCollectionParticipant dropCollectionParticipant(nss());
                sharding_util::sendCommandToShards(opCtx,
                                                   nss().db(),
                                                   CommandHelpers::appendMajorityWriteConcern(
                                                       dropCollectionParticipant.toBSON({})),
                                                   {primaryShardId},
                                                   **executor);

                if (collIsSharded) {
                    auto participants = Grid::get(opCtx)->shardRegistry()->getAllShardIds(opCtx);
                    // Remove prumary shard from participants
                    participants.erase(
                        std::remove(participants.begin(), participants.end(), primaryShardId),
                        participants.end());
                    sharding_util::sendCommandToShards(opCtx,
                                                       nss().db(),
                                                       CommandHelpers::appendMajorityWriteConcern(
                                                           dropCollectionParticipant.toBSON({})),
                                                       participants,
                                                       **executor);
                }
            }))
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
                // TODO _moveToState(State::kError);
                // TODO delete the state doc?
                _completionPromise.setError(status);
                return;
            }

            LOGV2_DEBUG(5390503, 1, "Collection dropped", "namespace"_attr = nss());
            _removeStateDocument();
            _completionPromise.emplaceValue();
        });
}

}  // namespace mongo
