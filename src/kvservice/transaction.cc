#include "transaction.h"

namespace scc {
    Transaction::Transaction(unsigned int tid) : txId(tid) {
    }

    Transaction::Transaction(unsigned int tid, TxContex *txc) {
        this->txId = tid;
        this->txContex = new TxContex(*txc);
    }

    void Transaction::setTxContex(TxContex *txc) {
        txContex = txc;
    }

    TxContex *Transaction::getTxContex() {
        return txContex;
    }

    unsigned long Transaction::getTxId() const {
        return txId;
    }

    void Transaction::setTxId(unsigned long txid) {
        txId = txid;
    }

    PrepareRequest::PrepareRequest(unsigned int prid) : id(prid) {}
}