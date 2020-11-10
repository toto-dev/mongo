/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "mongo/platform/basic.h"

#include "mongo/db/s/ddl_operation.h"
#include "mongo/stdx/thread.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

TEST(OperationRegistryTest, testRegisterOpeartion) {
    OperationRegistry opReg;

    auto mpOp = opReg.registerOperation(MovePrimary(2));
    ASSERT(mpOp->mustExecute());
}

TEST(OperationRegistryTest, testSameOperationSameParam) {
    OperationRegistry opReg;

    auto mpOp1 = opReg.registerOperation(MovePrimary(2));
    auto mpOp2 = opReg.registerOperation(MovePrimary(2));
    ASSERT_TRUE(mpOp1->mustExecute());
    ASSERT_FALSE(mpOp2->mustExecute());
}

TEST(OperationRegistryTest, testSameOperationDifferentParam) {
    OperationRegistry opReg;

    auto mpOp1 = opReg.registerOperation(MovePrimary(2));
    ASSERT_THROWS_CODE(opReg.registerOperation(MovePrimary(3)),
                       AssertionException,
                       ErrorCodes::ConflictingOperationInProgress);
}

TEST(OperationRegistryTest, testOperationCompletion) {
    OperationRegistry opReg;

    auto mpOp = opReg.registerOperation(MovePrimary(2));
    stdx::thread signalThread([&] { mpOp->signalCompletion(); });
    mpOp->getCompletionFuture().get();

    signalThread.join();
}

TEST(OperationRegistryTest, testSecondOperationCompletion) {
    OperationRegistry opReg;

    auto mpOp1 = opReg.registerOperation(MovePrimary(2));
    auto mpOp2 = opReg.registerOperation(MovePrimary(2));
    stdx::thread signalThread([&] { mpOp1->signalCompletion(); });
    mpOp2->getCompletionFuture().get();

    signalThread.join();
}

}  // namespace
}  // namespace mongo
