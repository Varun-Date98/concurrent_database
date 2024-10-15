import os
import logging
from enum import Enum
from typing import Dict, List, Optional


# Setting up logging
engine: "TransactionManager"
logging.basicConfig(
    level=logging.INFO,
    filename="database.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class LockType(Enum):
    """
    Enum class for lock types
    """
    UNLOCKED = 0
    READ_LOCK = 1
    WRITE_LOCK = 2


class TransactionState(Enum):
    """
    Enum class for transaction states
    """
    ACTIVE = 0
    ABORTED = 1
    WAITING = 2
    COMMITTED = 3


class Lock:
    """
    Lock class to simulate database locks
    """
    def __init__(self, item: str) -> None:
        self.item = item
        self.type = LockType.UNLOCKED                           # Type of lock
        self.read_locked_by: List[Transaction] = []             # Tracks transaction holding read locks
        self.waiting_transactions: List[Transaction] = []       # Tracks waiting transactions
        self.write_locked_by: Optional[Transaction] = None      # Tracks transaction holding write lock

    def __repr__(self) -> str:
        return f"Item: {self.item} | Lock type: {self.type} | Read locked by: {[txn.id for txn in self.read_locked_by]} Write locked by: {self.write_locked_by.id if self.write_locked_by else None} | Waiting transactions: {[txn.id for txn in self.waiting_transactions]}"


class Transaction:
    """
    Transaction class to spawn new database transactions
    """
    timestamp: int = 0

    def __init__(self, id: int) -> None:
        self.id = id                            # Transaction id
        self.waiting_ops = []                   # List of waiting ops
        Transaction.timestamp += 1              # Increment global timestamp
        self.locked_resources = set()           # Set of resources locked
        self.state = TransactionState.ACTIVE    # Transaction state
        self.timestamp = Transaction.timestamp  # Transaction timestamp

    def __eq__(self, other: "Transaction") -> bool:
        return self.id == other.id

    def __lt__(self, other: "Transaction") -> bool:
        return self.timestamp < other.timestamp

    def __repr__(self) -> str:
        return f"ID: {self.id} | TS: {self.timestamp} | Locked Resources: {self.locked_resources} | State: {self.state}"


class LockManager:
    """
    Database lock manager class
    """
    def __init__(self) -> None:
        self.lock_table: Dict[str, Lock] = {}    # mapping db_item -> lock
    
    def __repr__(self) -> str:
        return f"{self.lock_table}"

    def get_read_lock(self, item: str, txn: Transaction) -> bool:
        """Method to get a read lock on resource item by transaction txn

        Args:
            item (str): item to be read locked
            txn (Transaction): requesting transaction

        Returns:
            bool: True if lock is given False otherwise
        """
        logging.info(f"Transaction T{txn.id} attempting to get read lock on resource {item}")
        if item not in self.lock_table or self.lock_table[item].type == LockType.UNLOCKED:
            # No previous lock exists or item is unlocked
            lock = Lock(item)
            lock.type = LockType.READ_LOCK
            lock.read_locked_by.append(txn)
            self.lock_table[item] = lock
            logging.info(f"Created new lock {lock}")
            print(f"{item} is read locked by T{txn.id}")
            return True

        # Get the existing lock
        lock = self.lock_table[item]

        if lock.type == LockType.READ_LOCK:
            # Item has a shared lock, grant the new lock
            lock.read_locked_by.append(txn)
            print(f"{item} is read locked by T{txn.id}")
            return True
        else:
            # Item is read locked, implement wound wait
            locking_txn = lock.write_locked_by

            if locking_txn.timestamp < txn.timestamp:
                # Txn waits
                print(f"T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id} (following wound-wait)")
                logging.info(f"Transaction T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id}")
                lock.waiting_transactions.append(txn)
                return False
            else:
                engine.abort(locking_txn, self)
                print(f"Abort T{locking_txn.id} as T{locking_txn.id} is younger than T{txn.id} (following wound-wait)")
                logging.info(f"Transaction T{locking_txn.id} aborted following wound-wait. Lock ownership transfered to T{txn.id}")
                lock.write_locked_by = None
                lock.type = LockType.READ_LOCK
                lock.read_locked_by.append(txn)
                return True
    
    def get_write_lock(self, item: str, txn: Transaction) -> bool:
        """Method to get a write lock on resource item by transaction txn

        Args:
            item (str): resource to be write locked
            txn (Transaction): requesting transaction

        Returns:
            bool: True if write lock is granted else False
        """
        logging.info(f"Transaction T{txn.id} attempting to get write lock on resource {item}")
        if item not in self.lock_table or self.lock_table[item].type == LockType.UNLOCKED:
            # No previous lock exists or item is unlocked
            lock = Lock(item)
            lock.write_locked_by = txn
            self.lock_table[item] = lock
            lock.type = LockType.WRITE_LOCK
            logging.info(f"Created new lock {lock}")
            print(f"{item} is write locked by T{txn.id}")
            return True
        
        # Get the existing lock
        lock = self.lock_table[item]

        if lock.type == LockType.READ_LOCK:
            # Try to abort transactions holding read lock
            for locking_txn in lock.read_locked_by:
                if locking_txn == txn:
                    continue
                elif locking_txn.timestamp > txn.timestamp:
                    engine.abort(locking_txn, self)
                    print(f"Abort T{locking_txn.id} as T{locking_txn.id} is younger than T{txn.id} (following wound-wait)")
                    logging.info(f"Transaction T{locking_txn.id} aborted following wound-wait. Lock ownership transfered to T{txn.id}")
                else:
                    break
            
            if not lock.read_locked_by:
                lock.write_locked_by = txn
                lock.type = LockType.WRITE_LOCK
                print(f"{item} is write locked by T{txn.id}")
                logging.info(f"{item} write locked by T{txn.id}")
                return True
            elif len(lock.read_locked_by) == 1 and lock.read_locked_by[0] == txn:
                # Upgrade lock to WRITE LOCK
                lock.write_locked_by = txn
                lock.read_locked_by.clear()
                lock.type = LockType.WRITE_LOCK
                print(f"Read lock on {item} by T{txn.id} is upgraded to write lock")
                logging.info(f"Lock type upgraded to WRITE_LOCK for resource {item} and transaction T{txn.id}")
                return True
            
            print(f"T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id} (following wound-wait)")
            logging.info(f"Transaction T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id}")
            lock.waiting_transactions.append(txn)
            return False
        else:
            # Item is write locked
            locking_txn = lock.write_locked_by

            if locking_txn.timestamp < txn.timestamp:
                # Txn waits
                lock.waiting_transactions.append(txn)
                print(f"T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id} (following wound-wait)")
                logging.info(f"Transaction T{txn.id} waits as T{txn.id} is younger than T{locking_txn.id}")
                return False
            else:
                engine.abort(locking_txn, self)
                lock.write_locked_by = txn
                print(f"Abort T{locking_txn.id} as T{locking_txn.id} is younger than T{txn.id} (following wound-wait)")
                logging.info(f"Transaction T{locking_txn.id} aborted following wound-wait. Lock ownership transfered to T{txn.id}")
                return True
    
    def unlock_item(self, item: str, txn: Transaction) -> bool:
        """Unlocks given item by removing the given transaction
        Also sends signal to restart waiting transaction if item is unlocked

        Args:
            item (str): item to be unlocked
            txn (Transaction): transaction holding the lock

        Returns:
            bool: True if lock is removed False otherwise
        """
        if item not in self.lock_table:
            # Can not unlock non-existing lock
            return False
        
        lock = self.lock_table[item]

        if lock.type == LockType.READ_LOCK and txn in lock.read_locked_by:
            lock.read_locked_by.remove(txn)

            if not lock.read_locked_by:
                lock.type = LockType.UNLOCKED
        elif lock.type == LockType.WRITE_LOCK and txn == lock.write_locked_by:
            lock.write_locked_by = None
            lock.type = LockType.UNLOCKED

        logging.info(f"Transaction T{txn.id} released locks on resource {item}. Updated lock: {lock}")
        return lock.type == LockType.UNLOCKED


class TransactionManager:
    """
    Database transaction manager class
    """
    def __init__(self) -> None:
        self.transaction_table: Dict[int, Transaction] = {}     # maps transaction id to transaction objet
    
    def __repr__(self) -> str:
        return f"{self.transaction_table}"
    
    def initiate_transaction(self, id: int) -> bool:
        if id in self.transaction_table:
            return False
        
        txn = Transaction(id)
        self.transaction_table[id] = txn
        print(f"T{id} begins ID={id} TS={txn.timestamp} state={txn.state}")
        return True

    def read_item(self, item: str, txn: Transaction, lock_manager: LockManager) -> bool:
        if txn.state != TransactionState.ACTIVE:
            return False
        
        if not lock_manager.get_read_lock(item, txn):
            # Read lock could not be given add operation to the list of waiting ops
            txn.waiting_ops.append(("r", item))
            txn.state = TransactionState.WAITING
            return False
        
        txn.locked_resources.add(item)
        return True

    def write_item(self, item: str, txn: Transaction, lock_manager: LockManager) -> bool:
        if txn.state != TransactionState.ACTIVE:
            return False
        
        if not lock_manager.get_write_lock(item, txn):
            txn.waiting_ops.append(("w", item))
            txn.state = TransactionState.WAITING
            return False
        
        txn.locked_resources.add(item)
        return True

    def commit(self, txn: Transaction, lock_manager: LockManager) -> bool:
        if txn.state != TransactionState.ACTIVE:
            txn.waiting_ops.append(("c", ""))
            print(f"T{txn.id} can not be committed (not active or already committed/aborted)")
            return False
        
        unlocked_resources = []
        for resource in txn.locked_resources:
            if lock_manager.unlock_item(resource, txn):
                unlocked_resources.append(resource)
        
        txn.state = TransactionState.COMMITTED
        print(f"T{txn.id} is committed")
        
        for resource in txn.locked_resources:
            lock = lock_manager.lock_table[resource]
            
            # print(f"Checking lock for {lock.item}, waiting transactions are: {[t.id for t in lock.waiting_transactions]} -------")
            if lock.waiting_transactions:
                self.restart(lock.waiting_transactions.pop(0), lock_manager)
        
        txn.locked_resources.clear()
        return True

    def abort(self, txn: Transaction, lock_manager: LockManager) -> bool:
        if txn.state not in [TransactionState.ACTIVE, TransactionState.WAITING]:
            print(f"T{txn.id} can not be aborted (not active or waiting or already committed/aborted)")
            return False
        
        unlocked_resources = []
        for resource in txn.locked_resources:
            if lock_manager.unlock_item(resource, txn):
                unlocked_resources.append(resource)
        
        txn.state = TransactionState.ABORTED
        txn.locked_resources.clear()
        txn.waiting_ops.clear()
        for resource in unlocked_resources:
            lock = lock_manager.lock_table[resource]

            if lock.waiting_transactions:
                self.restart(lock.waiting_transactions.pop(0), lock_manager)
        
        return True

    def restart(self, txn: Transaction, lock_manager: LockManager) -> bool:
        logging.info(f"Attempting to restart transaction T{txn.id}. Operations to be run {txn.waiting_ops}")
        if txn.state != TransactionState.WAITING:
            return False
        
        txn.state = TransactionState.ACTIVE
        print(f"Restarting T{txn.id}, waiting operations: {txn.waiting_ops}")

        while txn.state == TransactionState.ACTIVE and txn.waiting_ops:
            op, item = txn.waiting_ops.pop(0)

            if op == "r":
                self.read_item(item, txn, lock_manager)
            elif op == "w":
                self.write_item(item, txn, lock_manager)
            elif op == "c":
                self.commit(txn, lock_manager)
            
            if txn.state == TransactionState.WAITING:
                txn.waiting_ops.insert(0, (op, item))
                return False
        
        return True
        

def execute_operation(operation: str, db_engine: TransactionManager, lock_table: LockManager) -> None:
    op = operation[0]
    id = int(operation[1])

    if op in "rw":
        item = operation[3]

    if op == "b":
        db_engine.initiate_transaction(id)

    if op == "r":
        txn = db_engine.transaction_table[id]
        db_engine.read_item(item, txn, lock_table)

    if op == "w":
        txn = db_engine.transaction_table[id]
        db_engine.write_item(item, txn, lock_table)

    if op == "e":
        txn = db_engine.transaction_table[id]
        db_engine.commit(txn, lock_table)


def main():
    global engine
    engine = TransactionManager()
    input_dir = "./input"

    for file in os.listdir(input_dir):
        schedule = []
        lock_table = LockManager()

        if file.endswith(".txt"):
            with open(input_dir + "/" + file) as f:
                logging.info(f"Reading file {file}")
                schedule.extend(f.read().split("\n"))

        logging.info(f"Operations in schedule: {schedule}")

        for operation in schedule:
            if operation:
                execute_operation(operation, engine, lock_table)
            
            logging.info(f"Lock table -> \n{lock_table}")
            logging.info(f"Transaction table -> \n{engine}")

        # Resetting the lock and transaction table for next file
        lock_table.lock_table.clear()
        engine.transaction_table.clear()
        logging.info("=" * 200)
        print("=" * 148)
        print("=" * 148)


if __name__ == "__main__":
    main()
