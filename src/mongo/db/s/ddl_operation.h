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

#pragma once

#include "mongo/platform/basic.h"

#include <memory>
#include <utility>

#include "mongo/platform/atomic_word.h"
#include "mongo/util/future.h"

namespace mongo {

class Operation {
public:
    Operation() = default;
    Operation(const Operation& other){};

    virtual ~Operation() = default;

    bool mustExecute() {
        auto prev = false;
        return _shouldExecute.compareAndSwap(&prev, true);
    }

    SharedSemiFuture<void> getCompletionFuture() const {
        return _promise.getFuture();
    }

    void signalCompletion() {
        _promise.emplaceValue();
    }

    virtual bool operator==(const Operation& rhs) const = 0;
    virtual std::string toString() const = 0;

protected:
    AtomicWord<bool> _shouldExecute;
    SharedPromise<void> _promise;
};

class OperationRegistry {
public:
    template <typename OpType>
    std::shared_ptr<OpType> registerOperation(const OpType& newOp) {
        if (_activeOperation) {
            // Another operation is already active
            auto downcastedActiveOp = std::dynamic_pointer_cast<OpType>(_activeOperation);
            uassert(ErrorCodes::ConflictingOperationInProgress,
                    str::stream() << "Conflicting operation in progress: "
                                  << downcastedActiveOp->toString(),
                    downcastedActiveOp && *downcastedActiveOp == newOp);
            return downcastedActiveOp;
        }

        auto newOpPtr = std::make_shared<OpType>(newOp);
        _activeOperation = newOpPtr;
        return newOpPtr;
    }

    SharedSemiFuture<void> getCompletionFuture() const {
        return _activeOperation->getCompletionFuture();
    }

private:
    std::shared_ptr<Operation> _activeOperation;
};


class MovePrimary : public Operation {

public:
    MovePrimary(int args) : _args(args) {}

    bool operator==(const Operation& rhs) const override {
        try {
            const auto& downcastedRhs = dynamic_cast<const MovePrimary&>(rhs);
            return downcastedRhs._args == _args;
        } catch (const std::bad_cast&) {
            return false;
        }
    }

    std::string toString() const override {
        return str::stream() << "MovePrimary operation with args: " << _args;
    }

private:
    int _args;
};

}  // namespace mongo
