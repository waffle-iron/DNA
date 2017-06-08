package node

import (
	"DNA/common"
	"DNA/common/log"
	"DNA/core/ledger"
	"DNA/core/transaction"
	"DNA/errors"
	msg "DNA/net/message"
	. "DNA/net/protocol"
	"fmt"
	"sync"
)

//var lk sync.Mutex
var point_txnPool string

type TXNPool struct {
	sync.Mutex
	txnCnt uint64
	count int
	list   map[common.Uint256]*transaction.Transaction
}

func (txnPool *TXNPool) lockme(){
	//point_txnPooln := fmt.Sprintf("%p=",txnPool)
	//if point_txnPooln!= point_txnPool{
	//	log.Trace("point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
	//	point_txnPool=point_txnPooln
	//}
	txnPool.Lock()
	txnPool.count++
	if txnPool.count>1{
		log.Trace("txnPool.count =",txnPool.count)
	}
}

func (txnPool *TXNPool) unLockme(){
	//point_txnPooln := fmt.Sprintf("%p=",txnPool)
	//if point_txnPooln!= point_txnPool{
	//	log.Trace("point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
	//	point_txnPool=point_txnPooln
	//}
	txnPool.Unlock()
	txnPool.count--
}

func (txnPool *TXNPool) GetTransaction(hash common.Uint256) *transaction.Transaction {
	point_txnPooln := fmt.Sprintf("%p=",txnPool)
	if point_txnPooln!= point_txnPool{
		log.Trace("Get Trans point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
		point_txnPool=point_txnPooln
	}
	txnPool.lockme()
	defer txnPool.unLockme()
	txn := txnPool.list[hash]
	// Fixme need lock
	return txn
}

func (txnPool *TXNPool) AppendTxnPool(txn *transaction.Transaction) bool {
	point_txnPooln := fmt.Sprintf("%p=",txnPool)
	if point_txnPooln!= point_txnPool{
		log.Trace("Append point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
		point_txnPool=point_txnPooln
	}
	hash := txn.Hash()
	// TODO: Call VerifyTransactionWithTxPool to verify tx
	txnPool.lockme()
	txnPool.list[hash] = txn
	txnPool.txnCnt++
	txnPool.unLockme()
	return true
}

// Attention: clean the trasaction Pool after the consensus confirmed all of the transcation
func (txnPool *TXNPool) GetTxnPool(cleanPool bool) map[common.Uint256]*transaction.Transaction {
	point_txnPooln := fmt.Sprintf("%p=",txnPool)
	if point_txnPooln!= point_txnPool{
		log.Trace("GetTxn point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
		point_txnPool=point_txnPooln
	}
	txnPool.lockme()
	defer txnPool.unLockme()

	list := txnPool.list
	if cleanPool == true {
		txnPool.init()
	}
	return DeepCopy(list)
}

func DeepCopy(mapIn map[common.Uint256]*transaction.Transaction) map[common.Uint256]*transaction.Transaction {
	reply := make(map[common.Uint256]*transaction.Transaction)
	for k, v := range mapIn {
		reply[k] = v
	}
	return reply
}

// Attention: clean the trasaction Pool with committed transactions.
func (txnPool *TXNPool) CleanTxnPool(txs []*transaction.Transaction) error {
	point_txnPooln := fmt.Sprintf("%p=",txnPool)
	if point_txnPooln!= point_txnPool{
		log.Trace("Clean point_txnPooln changed=",point_txnPooln,"old=",point_txnPool)
		point_txnPool=point_txnPooln
	}
	txsNum := len(txs)
	txInPoolNum := len(txnPool.list)
	cleaned := 0
	// skip the first bookkeeping transaction
	for _, tx := range txs[1:] {
		delete(txnPool.list, tx.Hash())
		cleaned++
	}
	if txsNum-cleaned != 1 {
		log.Info(fmt.Sprintf("The Transactions num Unmatched. Expect %d, got %d .\n", txsNum, cleaned))
	}
	log.Trace(fmt.Sprintf("[CleanTxnPool], Requested %d clean, %d transactions cleaned from localNode.TransPool and remains %d still in TxPool", txsNum, cleaned, txInPoolNum-cleaned))
	return nil
}

func (txnPool *TXNPool) init() {
	txnPool.list = make(map[common.Uint256]*transaction.Transaction)
	txnPool.txnCnt = 0
}

func (node *node) SynchronizeTxnPool() {
	node.nbrNodes.RLock()
	defer node.nbrNodes.RUnlock()

	for _, n := range node.nbrNodes.List {
		if n.state == ESTABLISH {
			msg.ReqTxnPool(n)
		}
	}
}

func (txnPool *TXNPool) CleanSubmittedTransactions(block *ledger.Block) error {
	txnPool.lockme()
	defer txnPool.unLockme()
	log.Trace()

	err := txnPool.CleanTxnPool(block.Transactions)
	if err != nil {
		return errors.NewDetailErr(err, errors.ErrNoCode, "[TxnPool], CleanSubmittedTransactions failed.")
	}
	return nil
}