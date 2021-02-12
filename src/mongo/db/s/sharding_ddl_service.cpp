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

#include "mongo/db/s/sharding_ddl_service.h"

#include "mongo/base/checked_cast.h"
#include "mongo/db/s/database_sharding_state.h"
#include "mongo/db/s/drop_collection_coordinator.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/sharding_ddl_coordinator.h"
#include "mongo/db/s/sharding_ddl_operation_gen.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/logv2/log.h"

namespace mongo {

namespace {

/*
 * Extracts the sharding ddl operation id from a general operation document
 */
ShardingDdlOperationId extractShardingDDlOperationId(const BSONObj& ddlOpDocument) {
    const auto idElem = ddlOpDocument["_id"];
    uassert(
        6092801, str::stream() << "Missing _id element in DDL operation document", !idElem.eoo());
    return ShardingDdlOperationId::parse(IDLParserErrorContext("ShardingDdlOperationId"),
                                         idElem.Obj().getOwned());
}

}  // namespace

const NamespaceString ShardingDDLOperationService::kDDLOperationDocumentsNamespace =
    NamespaceString(NamespaceString::kConfigDb, "sharding.ddl.operations");

ShardingDDLOperationService* ShardingDDLOperationService::getService(OperationContext* opCtx) {
    auto registry = repl::PrimaryOnlyServiceRegistry::get(opCtx->getServiceContext());
    auto service = registry->lookupServiceByName(kServiceName);
    return checked_cast<ShardingDDLOperationService*>(std::move(service));
}

std::shared_ptr<ShardingDDLOperationService::Instance>
ShardingDDLOperationService::constructInstance(BSONObj initialState) const {
    const auto op = extractShardingDDlOperationId(initialState);

    switch (op.getOperationType()) {
        case DDLOperationTypeEnum::kDropCollectionCoordinator:
            return std::make_shared<DropCollectionCoordinator>(std::move(initialState));
            break;
        default:
            uasserted(ErrorCodes::BadValue,
                      str::stream() << "Encountered unknown Sharding DDL operation type: "
                                    << DDLOperationType_serializer(op.getOperationType()));
    }
}

std::shared_ptr<ShardingDDLOperationService::Instance>
ShardingDDLOperationService::getOrCreateInstance(OperationContext* opCtx, BSONObj initialState) {
    const auto opId = extractShardingDDlOperationId(initialState);
    const auto opName = DDLOperationType_serializer(opId.getOperationType());
    const auto& nss = opId.getNss();
    const auto isConfigDB = nss.isConfigDB();

    // Checks that this is the primary shard for the namespace's db
    auto checkPrimaryShard = [&] {
        const auto dbPrimaryShardId = [&]() {
            Lock::DBLock dbWriteLock(opCtx, nss.db(), MODE_IS);
            auto dss = DatabaseShardingState::get(opCtx, nss.db());
            auto dssLock = DatabaseShardingState::DSSLock::lockShared(opCtx, dss);
            // The following call will also ensure that the database version matches
            return dss->getDatabaseInfo(opCtx, dssLock).getPrimary();
        }();

        const auto thisShardId = ShardingState::get(opCtx)->shardId();

        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "This is not the primary shard for db " << nss.db()
                              << " expected: " << dbPrimaryShardId << " shardId: " << thisShardId,
                dbPrimaryShardId == thisShardId);
    };

    if (!isConfigDB) {
        // Check that the operation context has a database version for this namespace
        const auto clientDbVersion = OperationShardingState::get(opCtx).getDbVersion(nss.db());
        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "Request sent without attaching database version",
                clientDbVersion);

        // Check if this is actually the primary shard for the given namespace.
        // This is a preliminary best effort checks since we are not holding the database lock.
        checkPrimaryShard();
    }

    auto [opAlreadyExists, op] = [&] {
        // Prevent concurrent insertion of other instances
        stdx::lock_guard<Latch> lk(_mutex);

        if (auto op = PrimaryOnlyService::lookupInstance(opCtx, BSON("_id" << opId.toBSON()))) {
            return std::make_pair(true, checked_pointer_cast<ShardingDDLCoordinator>(op.get()));
        }
        auto op = PrimaryOnlyService::getOrCreateInstance(opCtx, initialState);
        return std::make_pair(false, checked_pointer_cast<ShardingDDLCoordinator>(op));
    }();

    // If the existing instance doesn't have conflicting options just return that one
    if (opAlreadyExists) {
        uassertStatusOK(op->checkIfOptionsConflict(initialState));
        return op;
    }

    // Complete construction of the newly created instance by taking the locks
    try {
        std::stack<DistLockManager::ScopedDistLock> scopedLocks;

        auto distLockManager = DistLockManager::get(opCtx);
        auto dbDistLock = uassertStatusOK(
            distLockManager->lock(opCtx, nss.db(), opName, DistLockManager::kDefaultLockTimeout));
        scopedLocks.emplace(dbDistLock.moveToAnotherThread());

        // Check again under the dbLock if this is still the primary shard for the database
        if (!isConfigDB) {
            checkPrimaryShard();
        }

        if (!nss.ns().empty()) {
            // TODO if this acquisition fails the scoped DB lock won't be able to unlock since it
            // doesn't have an opCtx
            // If the namespace is a collection grab the lock also for it.
            auto collDistLock = uassertStatusOK(distLockManager->lock(
                opCtx, nss.ns(), opName, DistLockManager::kDefaultLockTimeout));
            scopedLocks.emplace(collDistLock.moveToAnotherThread());
        }

        ForwardableOperationMetadata frwOpMetadata{opCtx};

        op->completeConstruction(std::move(frwOpMetadata), std::move(scopedLocks));
    } catch (const DBException& ex) {
        // TODO add error log here to inform about the failed creation of the new coordinator
        op->failConstruction(
            ex.toStatus("Failed to complete construction of sharding DDL operation"));
        throw;
    }

    return op;
}

}  // namespace mongo
