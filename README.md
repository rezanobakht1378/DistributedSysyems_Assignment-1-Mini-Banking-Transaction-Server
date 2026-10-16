# Distributed Systems: Assingment 1: Mini Banking Transaction Server
# Reza Nobakht
## student id: 404724008

## Design

The service uses JSON over HTTP. Clients send balance-changing operations to
`POST /transactions`, for example
`{"action":"withdraw","account":"A1001","amount":200}`. The server returns a
JSON success or a structured error. Balance inquiry uses
`GET /balance/inquiry?account=A1001`; deposits, withdrawals, and transfers all
use `POST /transactions` with an `action` field. `amount` is a generic unit,
not a dollar amount. The server passes it as a Python `Decimal` and stores
values in PostgreSQL `NUMERIC(20, 2)` to avoid floating-point rounding errors.
All accounts and customers are fictional.

FastAPI handles concurrent requests, while `BANK_MAX_CLIENTS` limits the
number of requests executing in the application at once. SQLAlchemy ORM
sessions manage PostgreSQL connections and map accounts, transactions, and
idempotency records to model classes. A write runs in a database transaction
and locks affected account rows with `SELECT ... FOR UPDATE`; simultaneous
updates therefore serialize and each withdrawal checks the latest committed
balance.
`X-Request-ID` is the idempotency key: retries with the same ID and payload
return the saved result; reusing an ID for a different operation is rejected.

## ACID and errors

* **Atomicity:** account changes, transaction records, and the saved
  idempotency response are committed in one PostgreSQL transaction. A failed
  operation rolls back all of them; this includes both sides of a transfer.
* **Consistency:** balances use PostgreSQL numeric values with a database
  constraint preventing negative values. Amounts must be positive and have at
  most two decimal places. Deposits and withdrawals are the only operations
  that create or remove money; transfers preserve the total.
* **Isolation:** PostgreSQL row locks serialize concurrent transactions that
  update the same account; readers see committed state. The
  concurrent-withdrawal test checks that successful operations cannot
  overspend the account.
* **Durability:** PostgreSQL commits the account state, transaction history,
  and idempotency record before success is returned. Configure PostgreSQL with
  its normal durable WAL settings and retain its data directory across server
  restarts.

Unknown accounts, insufficient funds, invalid or missing fields, unsupported
actions, malformed JSON, and reused request IDs are returned as structured
errors. Middleware logs each request with its ID, client IP, action, account,
result, and latency to `banking_server.log` and standard output.

## Primary/backup bonus

In cluster mode, the primary sends each write to the backup and waits for the
backup's PostgreSQL commit before committing locally and replying success. Both
nodes use the same request ID, so a client retry after a lost response cannot
apply the operation twice. The backup polls the primary's health endpoint and
promotes itself after the configured number of failed checks. The included
client tries configured server URLs in order and reuses one request ID across
retries.

This two-node design is intended for the assignment's crash-stop demonstration.
It does not use an external consensus quorum or fencing service. The primary
therefore fails closed if it cannot confirm its replica, and a primary that
discovers it has been superseded stops accepting writes. During recovery,
check the active node's role and database before manually restarting or
reconfiguring the former primary. This implementation does not provide
automatic failback or promise availability through arbitrary network
partitions.

## Run

Install Python 3.10 or later and the pinned dependencies:

```text
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

For the required two-VM deployment, bind the server to its network interface
(the default is `0.0.0.0`) and use the server VM's real IP addresses. On Linux,
configure the primary on Server A:

```text
export BANK_NODE_ID=server-a
export BANK_ROLE=primary
export BANK_PEER_URL=http://SERVER_B_IP:8000
export DATABASE_URL=postgresql://banking_user:change-me@127.0.0.1:5432/banking_db
export BANK_MAX_CLIENTS=50
python main.py
```

In PowerShell, the equivalent primary setup is:

```powershell
$env:BANK_NODE_ID = "server-a"
$env:BANK_ROLE = "primary"
$env:BANK_PEER_URL = "http://SERVER_B_IP:8000"
$env:DATABASE_URL = "postgresql://banking_user:change-me@POSTGRESQL_HOST:5432/banking_db"
$env:BANK_MAX_CLIENTS = "50"
python main.py
```

Configure the backup on Server B by using these Linux settings:

```text
export BANK_NODE_ID=server-b
export BANK_ROLE=backup
export BANK_PRIMARY_URL=http://SERVER_A_IP:8000
export DATABASE_URL=postgresql://banking_user:change-me@127.0.0.1:5432/banking_db
export BANK_MAX_CLIENTS=50
export BANK_HEARTBEAT_INTERVAL=1
export BANK_FAILURE_THRESHOLD=3
python main.py
```

Use equivalent `$env:BANK_...` assignments in PowerShell when Server B runs
Windows. Replication has no authentication, so restrict the endpoint to a
trusted network using the VM firewall.

Set `BANK_SERVER_URLS` on the client machine to the server VM URLs, primary
first, for example
`http://SERVER_A_IP:8000,http://SERVER_B_IP:8000`. Then run:

```text
python client.py balance A1001
python client.py deposit A1001 25.50
python client.py withdraw A1001 10
python client.py transfer A1001 A1002 5
```

Create the PostgreSQL database and user first, then set `DATABASE_URL` to
their connection string on each server. Keep each instance's PostgreSQL data
directory persistent; run the database on the server VM or point to an
appropriately isolated PostgreSQL instance for that node. For a single server,
omit the cluster variables. `BANK_MAX_CLIENTS` configures concurrent request
handling. Tests create isolated temporary schemas in the dedicated PostgreSQL
database named by `BANK_TEST_DATABASE_URL` and run with
`python -m unittest discover -s tests`.

## Demonstration checklist

Run client and server VMs on a network and show normal balance, deposit, and
withdraw requests; concurrent withdrawals on one account; each error case;
the server log; process restart with the same database; and, for the bonus,
stop Server A while requests are active and show Server B promoting and
serving retries. Record the required screen video from those VM runs.
