import os
import datetime
from dotenv import load_dotenv
import time
import uuid
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Request, status
from sqlmodel import Session, create_engine, select
from models import Account, Transaction, TransactionType
from requestForms import DepositWithdrawForm, TransferForm

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/banking_db")
engine = create_engine(DATABASE_URL, echo=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("banking_server")

@asynccontextmanager
async def lifespan(app: FastAPI):
    from sqlmodel import SQLModel
    SQLModel.metadata.create_all(engine)
    yield

app = FastAPI(lifespan=lifespan)

def get_session():
    with Session(engine) as session:
        yield session

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    client_ip = request.client.host if request.client else "unknown"
    start_time = time.time()
    
    action = request.url.path
    account_id = "N/A"
    
    response = None
    result = "success"
    try:
        response = await call_next(request)
        if response.status_code >= 400:
            result = "failure"
    except Exception as e:
        result = "error"
        logger.exception(f"Unhandled exception: {e}")
        raise
    finally:
        latency = f"{int((time.time() - start_time) * 1000)}ms"
        logger.info(f"$req={request_id}$ $ip={client_ip}$ action={action} account={account_id} result={result} latency={latency}")
    return response

@app.get("/balance/inquiry")
def read_balance(wallet_id: str, session: Session = Depends(get_session)):
    account = session.exec(select(Account).where(Account.wallet == wallet_id)).first()
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"Case": "Unknown account", "Example": f"a request for account {wallet_id} which does not exist"}
        )
    return {"status": "success", "wallet_id": account.wallet, "balance": account.balance}

@app.post("/deposit")
def deposit(request: Request, body: DepositWithdrawForm, session: Session = Depends(get_session)):
    client_ip = request.client.host if request.client else "127.0.0.1"
    
    try:
        statement = select(Account).where(Account.wallet == body.wallet_id).with_for_update()
        account = session.exec(statement).first()
        
        if not account:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"Case": "Unknown account", "Example": f"a request for account {body.wallet_id} which does not exist"})
        
        last_balance = account.balance
        account.balance += body.amount
        new_balance = account.balance
        account.updated_at = datetime.utcnow()
        
        tx = Transaction(
            wallet_id=account.wallet,
            amount=body.amount,
            last_balance=last_balance,
            new_balance=new_balance,
            type=TransactionType.deposit,
            ip=client_ip
        )
        session.add(tx)
        session.add(account)
        session.commit()
        session.refresh(account)
        
        return {"status": "success", "account": account.wallet, "balance": account.balance}
        
    except HTTPException as he:
        session.rollback()
        raise he
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case": "Malformed request", "Example": str(e)})

@app.post("/withdraw")
def withdraw(request: Request, body: DepositWithdrawForm, session: Session = Depends(get_session)):
    client_ip = request.client.host if request.client else "127.0.0.1"
    
    try:
        statement = select(Account).where(Account.wallet == body.wallet_id).with_for_update()
        account = session.exec(statement).first()
        
        if not account:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"Case": "Unknown account", "Example": f"a request for account {body.wallet_id} which does not exist"})
        
        if account.balance < body.amount:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case": "Insufficient funds", "Example": f"withdrawing {body.amount} from an account holding {account.balance}"})
        
        last_balance = account.balance
        account.balance -= body.amount
        new_balance = account.balance
        account.updated_at = datetime.utcnow()
        
        tx = Transaction(
            wallet_id=account.wallet,
            amount=body.amount,
            last_balance=last_balance,
            new_balance=new_balance,
            type=TransactionType.withdraw,
            ip=client_ip
        )
        session.add(tx)
        session.add(account)
        session.commit()
        session.refresh(account)
        
        return {"status": "success", "account": account.wallet, "balance": account.balance}
        
    except HTTPException as he:
        session.rollback()
        raise he
    except Exception as e:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"Case": "Malformed request", "Example": str(e)}
        )

@app.post("/transfer")
def transfer(request: Request, body: TransferForm, session: Session = Depends(get_session)):
    client_ip = request.client.host if request.client else "127.0.0.1"
    
    if body.amount <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case": "Negative or invalid amount", "Example": "-50, 0, 'abc'"})

    try:
        wallets = sorted([body.source_wallet_id, body.wallet_id])
        
        statement = select(Account).where(Account.wallet.in_(wallets)).with_for_update()
        accounts = {acc.wallet: acc for acc in session.exec(statement).all()}
        
        source_account = accounts.get(body.source_wallet_id)
        target_account = accounts.get(body.wallet_id)
        
        if not source_account or not target_account:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"Case": "Unknown account", "Example": "a request for an account which does not exist"})
            
        if source_account.balance < body.amount:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,detail={"Case": "Insufficient funds", "Example": f"transferring {body.amount} with balance {source_account.balance}"})
            
        src_last = source_account.balance
        source_account.balance -= body.amount
        src_new = source_account.balance
        
        tgt_last = target_account.balance
        target_account.balance += body.amount
        tgt_new = target_account.balance
        
        tx_out = Transaction(wallet_id=source_account.wallet, amount=body.amount, last_balance=src_last, new_balance=src_new, type=TransactionType.transfer, ip=client_ip)
        tx_in = Transaction(wallet_id=target_account.wallet, amount=body.amount, last_balance=tgt_last, new_balance=tgt_new, type=TransactionType.transfer, ip=client_ip)
        
        session.add(tx_out)
        session.add(tx_in)
        session.add(source_account)
        session.add(target_account)
        session.commit()
        
        return {"status": "success", "source_account": source_account.wallet, "source_balance": source_account.balance}

    except HTTPException as he:
        session.rollback()
        raise he
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case": "Malformed request", "Example": str(e)})