# Distributed Systems: Assingment 1: Mini Banking Transaction Server
# Reza Nobakht
## student id: 404724008

## Design

The service uses JSON over HTTP. Clients send balance-changing operations to
`POST /transactions`, for example
`{"action":"withdraw","account":"A1001","amount":200}`. The server returns a
JSON success or a structured error. Balance inquiry uses
`GET /balance/inquiry?account=A1001`; deposits, withdrawals, and transfers use
`POST /transactions` with an `action` field. A transfer includes a
`destination`, for example
`{"action":"transfer","account":"A1001","destination":"A1002","amount":25}`.
`amount` is a generic unit, not a dollar amount. The server passes it as a
Python `Decimal` and stores values in PostgreSQL `NUMERIC(20, 2)` to avoid
floating-point rounding errors. All accounts and customers are fictional.

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
  operation rolls back all of them, including both sides of a transfer.
* **Consistency:** balances use PostgreSQL numeric values with a database
  constraint preventing negative values. Amounts must be positive and have at
  most two decimal places. Deposits and withdrawals create or remove money;
  transfers move the same amount between accounts without changing the total.
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

## Primary/backup and split-brain bonus

In cluster mode, the primary sends each write to the backup and waits for the
backup's PostgreSQL commit before committing locally and replying success. Both
nodes use the same request ID, so a client retry after a lost response cannot
apply the operation twice. The backup promotes itself only after acquiring an
exclusive PostgreSQL advisory lock from a third, independent witness database.
Both banking servers must use the same `BANK_WITNESS_DATABASE_URL` and
`BANK_CLUSTER_ID`; each keeps its own PostgreSQL banking database. The witness
records a monotonically increasing leadership epoch. Every replicated write
carries that epoch, and servers reject requests from an old leader.

If the servers lose contact but can still reach the witness, the current
primary retains the lock and the backup stays read-only. If the primary crashes,
PostgreSQL releases its session lock and the backup can acquire it and promote.
If a former primary returns, it cannot take the lock from the new primary. It
first downloads the active node's full account, transaction, and idempotency
snapshot, then remains a standby. A primary that cannot confirm its witness or
replica fails closed. A restarted primary with an existing witness term waits
for the backup to confirm or recover the latest state before it can take
leadership, avoiding promotion of a stale local database. When the
inter-server connection returns, the active primary synchronizes the standby
before reopening writes. The client tries the configured server URLs and
reuses a request ID across retries.

The witness must run on an independent host, remain reachable from both server
VMs during the server-to-server partition, and use durable PostgreSQL storage.
If the witness is unavailable, neither node may take over without a confirmed
lock. Restrict the witness and internal replication/snapshot routes to the
trusted VM network with firewall rules; these internal routes do not use a
shared application secret.

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
export BANK_WITNESS_DATABASE_URL=postgresql://WITNESS_HOST:5432/banking_witness
export BANK_CLUSTER_ID=mini-banking-lab
export DATABASE_URL=postgresql://banking_user:change-me@127.0.0.1:5432/banking_db
export BANK_MAX_CLIENTS=50
python main.py
```

In PowerShell, the equivalent primary setup is:

```powershell
$env:BANK_NODE_ID = "server-a"
$env:BANK_ROLE = "primary"
$env:BANK_PEER_URL = "http://SERVER_B_IP:8000"
$env:BANK_WITNESS_DATABASE_URL = "postgresql://WITNESS_HOST:5432/banking_witness"
$env:BANK_CLUSTER_ID = "mini-banking-lab"
$env:DATABASE_URL = "postgresql://banking_user:change-me@POSTGRESQL_HOST:5432/banking_db"
$env:BANK_MAX_CLIENTS = "50"
python main.py
```

Configure the backup on Server B by using these Linux settings:

```text
export BANK_NODE_ID=server-b
export BANK_ROLE=backup
export BANK_PRIMARY_URL=http://SERVER_A_IP:8000
export BANK_WITNESS_DATABASE_URL=postgresql://WITNESS_HOST:5432/banking_witness
export BANK_CLUSTER_ID=mini-banking-lab
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

Create a PostgreSQL database and user for each server, then set `DATABASE_URL`
to that server's connection string. Keep both data directories persistent.
Create the separate witness database on a third machine reachable by both
servers, and configure the same witness URL and cluster ID on both. For a
single server, omit the cluster variables. `BANK_MAX_CLIENTS` configures
concurrent request handling. Tests create isolated temporary schemas in the
dedicated PostgreSQL database named by `BANK_TEST_DATABASE_URL` and run with
`python -m unittest discover -s tests`.

## Demonstration checklist

Run client and server VMs on a network and show normal balance, deposit, and
withdraw requests; concurrent withdrawals on one account; each error case;
the server log; process restart with the same database; and, for the bonus,
stop Server A while requests are active and show Server B promoting and
serving retries. For the partition demo, cut only the A-B link while both
servers can still reach the witness; show that the backup refuses writes,
restore the link, and show the primary synchronizing the backup before writes
resume. Then stop the primary, demonstrate witness-backed promotion, restart
the former primary, and show it syncing from the active node without taking
leadership. Record the required screen video from those VM runs.
