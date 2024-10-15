
# **Transaction Management System**

## **Overview**
This project implements a **Database Transaction Management System** with support for locking mechanisms. It ensures **concurrency control** through **read and write locks** using the **wound-wait protocol** for deadlock prevention. The system allows transactions to:
- **Read and write items** with appropriate locks.
- **Commit or abort transactions** based on their state.
- Manage locks via the **Lock Manager**.
- Handle **waiting and restarting transactions** using the **Transaction Manager**.

---

## **Features**
- **Two types of locks**: 
  - **Read Lock** (shared)
  - **Write Lock** (exclusive)
- **Wound-wait deadlock prevention**: 
  - Younger transactions wait if necessary.
  - Older transactions abort younger ones to avoid deadlock.
- **Transaction States**: 
  - **ACTIVE**
  - **WAITING**
  - **ABORTED**
  - **COMMITTED**
- **Lock management**: Keeps track of which transactions hold or wait on locks.
- **Transaction management**: Manages transaction lifecycles and ensures consistency.

---

## **Project Structure**

```
├── database.log         # Log file for all system events
├── input/               # Folder containing input files with transaction schedules
│   └── <schedule>.txt   # Example: schedule1.txt
├── transaction_manager.py  # Main transaction and lock management system
└── README.md            # This file
```

---

## **Dependencies**
This project requires **Python 3.x**. No additional packages are needed beyond the standard library.

---

## **How to Run the Program**

1. **Setup Input Directory**  
   - Create a folder named **`input`** in the project directory.
   - Add one or more **schedule files** inside the `input/` folder with the following format:
     ```
     b1    # Begin transaction T1
     r1(x) # T1 reads item x
     w1(y) # T1 writes item y
     e1    # Commit transaction T1
     ```
     Each line corresponds to a **transaction operation**.

2. **Run the Program**  
   Open a terminal or command prompt and execute the following command:
   ```bash
   python transaction_manager.py
   ```

3. **Check Logs and Outputs**  
   - **Logs**: The program logs all operations to `database.log`.
   - **Console Output**: Displays lock statuses and transaction states during execution.

---

## **Operations Format**
| **Operation** | **Description**                                 | **Example**  |
|---------------|--------------------------------------------------|--------------|
| `b<id>`       | Begin a new transaction with ID `<id>`           | `b1`         |
| `r<id>(x)`    | Transaction `<id>` reads item `x`                | `r1(x)`      |
| `w<id>(y)`    | Transaction `<id>` writes item `y`               | `w2(y)`      |
| `e<id>`       | Commit the transaction with ID `<id>`            | `e1`         |

---

## **How It Works**

1. **Lock Manager**:
   - **Read Lock**: Multiple transactions can read the same item concurrently.
   - **Write Lock**: Only one transaction can hold a write lock on an item at a time.
   - **Upgrade Locks**: Read locks can be upgraded to write locks if held by the same transaction.

2. **Wound-Wait Protocol**:
   - If a transaction requests a lock that conflicts with an existing one, the **wound-wait strategy** is applied:
     - If the requesting transaction is **younger**, it **waits**.
     - If it is **older**, the lock-holding transaction **aborts**.

3. **Transaction Lifecycle**:
   - Transactions can be **committed** or **aborted** based on conflicts or conditions during execution.
   - If a transaction is **aborted**, waiting transactions are restarted.

---

## **Sample Input and Output**

### **Sample Input File (`input/schedule1.txt`):**
```
b1
r1(x)
b2
w2(x)
e1
e2
```

### **Expected Output (Console):**
```
x is read locked by T1
T2 waits as T2 is younger than T1 (following wound-wait)
T1 is committed
Abort T2 as T2 is younger than T1 (following wound-wait)
T2 is committed
```

---

## **Logging**
- All operations and lock updates are logged in the **`database.log`** file.
- Example log entries:
  ```
  2024-10-15 10:12:00 - INFO - Transaction T1 attempting to get read lock on resource x
  2024-10-15 10:12:01 - INFO - Transaction T2 waiting due to conflict with T1
  2024-10-15 10:12:02 - INFO - Transaction T1 committed
  ```

---

## **Possible Improvements**
- **Deadlock detection**: Implement a detection mechanism to break cyclic waits.
- **Timeouts**: Add a timeout for waiting transactions to abort them automatically.
- **Multithreading**: Enhance performance by simulating concurrent transactions with threading.

---

## **Conclusion**
This project provides a simplified simulation of **transaction management** with **concurrency control** through locks. It demonstrates how database systems manage transactions using **protocols like wound-wait** to maintain consistency and avoid deadlocks. 

---

## **Contact**
For any questions or issues, feel free to reach out!
